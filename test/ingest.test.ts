import { beforeEach, describe, expect, it, vi } from "vitest";
import { handleIngest } from "../src/handlers/ingest";
import type { Env } from "../src/types";

const token = "test-token-that-is-at-least-thirty-two-characters-long";

function makeEnv() {
  const rows = new Map<string, { id: number; file_key: string; status: string }>();
  const outbox: Array<{ id: number; metadata_id: number; file_key: string; request_id: string; status: string }> = [];
  let nextId = 7;
  const put = vi.fn(async (_key: string, body: ReadableStream | ArrayBuffer | string) => {
    let size = 5;
    if (typeof body === "string") size = new TextEncoder().encode(body).byteLength;
    else if (body instanceof ArrayBuffer) size = body.byteLength;
    return { size };
  });
  const del = vi.fn().mockResolvedValue(undefined);
  const send = vi.fn().mockResolvedValue(undefined);

  const prepare = vi.fn((sql: string) => ({
    bind: (...args: unknown[]) => ({
      sql, args,
      first: vi.fn(async () => {
        if (sql.includes("FROM processing_outbox WHERE metadata_id")) {
          return outbox.find(row => row.metadata_id === args[0] && row.status === "READY") ?? null;
        }
        if (sql.includes("SELECT id, file_key, status")) return rows.get(String(args[0])) ?? null;
        if (sql.includes("SELECT id, status FROM metadata")) {
          const row = rows.get(String(args[0]));
          return row ? { id: row.id, status: row.status } : null;
        }
        if (sql.includes("SELECT id FROM metadata WHERE file_key")) {
          return [...rows.values()].find(row => row.file_key === args[0]) ?? null;
        }
        return null;
      }),
      all: vi.fn(async () => ({
        results: sql.includes("FROM processing_outbox")
          ? outbox.filter(row => row.status === "READY").map(row => ({ ...row }))
          : [],
      })),
      run: vi.fn(async () => {
        if (sql.includes("UPDATE processing_outbox SET status = 'ENQUEUED'")) {
          const row = outbox.find(item => item.id === args[0]);
          if (row) row.status = "ENQUEUED";
        }
        return { meta: { changes: 1 } };
      }),
    }),
  }));

  const batch = vi.fn(async (statements: Array<{ sql: string; args: unknown[] }>) => {
    for (const statement of statements) {
      if (statement.sql.includes("INSERT INTO metadata")) {
        const [fileKey, _size, _type, idem, requestId] = statement.args as [string, number, string, string, string];
        if (rows.has(idem)) throw new Error("UNIQUE constraint failed: metadata.idempotency_key");
        rows.set(idem, { id: nextId, file_key: fileKey, status: "PENDING" });
      } else if (statement.sql.includes("INSERT INTO processing_outbox")) {
        const [fileKey, requestId, idem] = statement.args as [string, string, string];
        const row = rows.get(idem);
        if (row) outbox.push({ id: outbox.length + 1, metadata_id: row.id, file_key: fileKey, request_id: requestId, status: "READY" });
      }
    }
    nextId++;
    return [];
  });

  const env: Env = {
    INGEST_API_TOKEN: token,
    MAX_UPLOAD_BYTES: "100",
    AUDIO_BUCKET: { put, delete: del } as unknown as R2Bucket,
    DB: { prepare, batch } as unknown as D1Database,
    PROCESSING_QUEUE: { send } as unknown as Queue<{ fileKey: string; metadataId: number; requestId: string }>,
  };
  return { env, put, del, send, rows, outbox, prepare, batch };
}

function makeRequest(overrides: { method?: string; authorization?: string; contentType?: string; length?: string; idempotency?: string; body?: string } = {}): Request {
  const headerValues: Record<string, string> = {
    ...(overrides.authorization ? { authorization: overrides.authorization } : {}),
    "content-type": overrides.contentType ?? "audio/wav",
    ...(overrides.length ? { "content-length": overrides.length } : {}),
    ...(overrides.idempotency ? { "idempotency-key": overrides.idempotency } : {}),
  };
  const request = new Request("https://example.test/ingest", {
    method: overrides.method ?? "POST",
    headers: Object.fromEntries(Object.entries(headerValues).filter(([name]) => name !== "content-length")),
    body: overrides.body ?? "audio",
  });
  // Fetch implementations may strip Content-Length because it is transport-managed.
  Object.defineProperty(request, "headers", {
    value: { get: (name: string) => headerValues[name.toLowerCase()] ?? null },
  });
  return request;
}

describe("handleIngest", () => {
  let ctx: ReturnType<typeof makeEnv>;
  beforeEach(() => { ctx = makeEnv(); });

  it("rejects missing credentials", async () => {
    const response = await handleIngest(makeRequest({ length: "5", idempotency: "request-123" }), ctx.env);
    expect(response.status).toBe(401);
    expect(ctx.put).not.toHaveBeenCalled();
  });

  it("accepts a case-insensitive Bearer authorization scheme", async () => {
    const response = await handleIngest(makeRequest({ authorization: `bEaReR ${token}`, length: "5", idempotency: "request-123" }), ctx.env);
    expect(response.status).toBe(202);
  });

  it("rejects non-audio content", async () => {
    const response = await handleIngest(makeRequest({ authorization: `Bearer ${token}`, contentType: "text/plain", length: "5", idempotency: "request-123" }), ctx.env);
    expect(response.status).toBe(415);
  });

  it("rejects oversized uploads before storage", async () => {
    const response = await handleIngest(makeRequest({ authorization: `Bearer ${token}`, length: "101", idempotency: "request-123" }), ctx.env);
    expect(response.status).toBe(413);
    expect(ctx.put).not.toHaveBeenCalled();
  });

  it("requires idempotency keys", async () => {
    const response = await handleIngest(makeRequest({ authorization: `Bearer ${token}`, length: "5" }), ctx.env);
    expect(response.status).toBe(400);
  });

  it("rejects a declared length that differs from stored bytes and removes the object", async () => {
    const response = await handleIngest(makeRequest({ authorization: `Bearer ${token}`, length: "6", idempotency: "request-123", body: "audio" }), ctx.env);
    expect(response.status).toBe(400);
    expect(ctx.del).toHaveBeenCalledOnce();
    expect(ctx.send).not.toHaveBeenCalled();
  });

  it("accepts a valid request and atomically persists metadata and outbox before dispatch", async () => {
    const response = await handleIngest(makeRequest({ authorization: `Bearer ${token}`, length: "5", idempotency: "request-123" }), ctx.env);
    expect(response.status).toBe(202);
    expect(ctx.put).toHaveBeenCalledOnce();
    expect(ctx.batch).toHaveBeenCalledOnce();
    expect(ctx.outbox).toHaveLength(1);
    expect(ctx.send).toHaveBeenCalledOnce();
    expect(ctx.outbox[0].status).toBe("ENQUEUED");
  });

  it("returns the existing record for a repeated idempotency key", async () => {
    ctx.rows.set("request-123", { id: 3, file_key: "ingest/existing.wav", status: "PENDING" });
    const response = await handleIngest(makeRequest({ authorization: `Bearer ${token}`, length: "5", idempotency: "request-123" }), ctx.env);
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ duplicate: true, metadataId: 3, fileKey: "ingest/existing.wav" });
    expect(ctx.put).not.toHaveBeenCalled();
  });

  it("retries dispatch for an existing pending idempotency record", async () => {
    ctx.rows.set("request-123", { id: 3, file_key: "ingest/existing.wav", status: "PENDING" });
    ctx.outbox.push({ id: 1, metadata_id: 3, file_key: "ingest/existing.wav", request_id: "prior-request", status: "READY" });
    const response = await handleIngest(makeRequest({ authorization: `Bearer ${token}`, length: "5", idempotency: "request-123" }), ctx.env);
    expect(response.status).toBe(200);
    expect(ctx.send).toHaveBeenCalledOnce();
    expect(ctx.outbox[0].status).toBe("ENQUEUED");
    expect(ctx.put).not.toHaveBeenCalled();
  });

  it("cleans up the new object when a concurrent idempotency insert loses", async () => {
    let selectCount = 0;
    ctx.prepare.mockImplementation((sql: string) => ({
      bind: (...args: unknown[]) => ({
        first: vi.fn(async () => {
          if (sql.includes("SELECT id, file_key, status")) {
            selectCount++;
            return selectCount === 1 ? null : { id: 8, file_key: "ingest/winner", status: "PENDING" };
          }
          if (sql.includes("SELECT id FROM metadata WHERE file_key")) return null;
          return null;
        }),
        all: vi.fn(async () => ({ results: [] })),
        run: vi.fn(async () => ({ meta: { changes: 1 } })),
      }),
    }) as never);
    (ctx.env.DB as unknown as { batch: ReturnType<typeof vi.fn> }).batch = vi.fn(async () => {
      throw new Error("UNIQUE constraint failed");
    });
    const response = await handleIngest(makeRequest({ authorization: `Bearer ${token}`, length: "5", idempotency: "request-123" }), ctx.env);
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ duplicate: true, metadataId: 8 });
    expect(ctx.del).toHaveBeenCalledOnce();
    expect(ctx.send).not.toHaveBeenCalled();
  });
});
