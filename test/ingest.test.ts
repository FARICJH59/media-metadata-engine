import { beforeEach, describe, expect, it, vi } from "vitest";
import { handleIngest } from "../src/handlers/ingest";
import type { Env } from "../src/types";

const token = "test-token-that-is-at-least-thirty-two-characters-long";

function makeEnv() {
  const rows = new Map<string, { id: number; file_key: string; status: string }>();
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
      first: vi.fn(async () => {
        if (sql.includes("SELECT id, file_key, status")) {
          return rows.get(String(args[0])) ?? null;
        }
        if (sql.includes("INSERT INTO metadata")) {
          const [fileKey, size, _type, idem] = args as [string, number, string, string];
          if (rows.has(idem)) throw new Error("UNIQUE constraint failed: metadata.idempotency_key");
          const row = { id: 7, file_key: fileKey, status: "PENDING" };
          rows.set(idem, row);
          return { id: 7 };
        }
        if (sql.includes("SELECT id FROM metadata WHERE file_key")) {
          return [...rows.values()].find(row => row.file_key === args[0]) ?? null;
        }
        return { id: 7 };
      }),
      run: vi.fn(async () => ({ meta: { changes: 1 } })),
    }),
  }));
  const env: Env = {
    INGEST_API_TOKEN: token,
    MAX_UPLOAD_BYTES: "100",
    AUDIO_BUCKET: { put, delete: del } as unknown as R2Bucket,
    DB: { prepare } as unknown as D1Database,
    PROCESSING_QUEUE: { send } as unknown as Queue<{ fileKey: string; metadataId: number; requestId: string }>,
  };
  return { env, put, del, send, rows, prepare };
}

function makeRequest(overrides: { method?: string; authorization?: string; contentType?: string; length?: string; idempotency?: string; body?: string } = {}): Request {
  return new Request("https://example.test/ingest", {
    method: overrides.method ?? "POST",
    headers: {
      ...(overrides.authorization ? { authorization: overrides.authorization } : {}),
      "content-type": overrides.contentType ?? "audio/wav",
      ...(overrides.length ? { "content-length": overrides.length } : {}),
      ...(overrides.idempotency ? { "idempotency-key": overrides.idempotency } : {}),
    },
    body: overrides.body ?? "audio",
  });
}

describe("handleIngest", () => {
  let ctx: ReturnType<typeof makeEnv>;
  beforeEach(() => { ctx = makeEnv(); });

  it("rejects missing credentials", async () => {
    const response = await handleIngest(makeRequest({ length: "5", idempotency: "request-123" }), ctx.env);
    expect(response.status).toBe(401);
    expect(ctx.put).not.toHaveBeenCalled();
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

  it("accepts a valid request and enqueues work", async () => {
    const response = await handleIngest(makeRequest({ authorization: `Bearer ${token}`, length: "5", idempotency: "request-123" }), ctx.env);
    expect(response.status).toBe(202);
    expect(ctx.put).toHaveBeenCalledOnce();
    expect(ctx.send).toHaveBeenCalledOnce();
  });

  it("returns the existing record for a repeated idempotency key", async () => {
    ctx.rows.set("request-123", { id: 3, file_key: "ingest/existing.wav", status: "PENDING" });
    const response = await handleIngest(makeRequest({ authorization: `Bearer ${token}`, length: "5", idempotency: "request-123" }), ctx.env);
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ duplicate: true, metadataId: 3, fileKey: "ingest/existing.wav" });
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
          if (sql.includes("INSERT INTO metadata")) throw new Error("UNIQUE constraint failed");
          return null;
        }),
        run: vi.fn(async () => ({ meta: { changes: 1 } })),
      }),
    }) as never);
    const response = await handleIngest(makeRequest({ authorization: `Bearer ${token}`, length: "5", idempotency: "request-123" }), ctx.env);
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ duplicate: true, metadataId: 8 });
    expect(ctx.del).toHaveBeenCalledOnce();
    expect(ctx.send).not.toHaveBeenCalled();
  });
});
