import { beforeEach, describe, expect, it, vi } from "vitest";
import { handleIngest } from "../src/handlers/ingest";
import type { Env } from "../src/types";

const token = "test-token-that-is-at-least-thirty-two-characters-long";

function makeEnv(): Env {
  return {
    INGEST_API_TOKEN: token,
    MAX_UPLOAD_BYTES: "100",
    AUDIO_BUCKET: {
      put: vi.fn().mockResolvedValue({}),
      delete: vi.fn().mockResolvedValue(undefined),
    } as unknown as R2Bucket,
    DB: {
      prepare: vi.fn((sql: string) => ({
        bind: vi.fn(() => ({
          first: vi.fn(async () => sql.includes("SELECT id, file_key") ? null : { id: 7 }),
          run: vi.fn(async () => ({ meta: { changes: 1 } })),
        })),
      })),
    } as unknown as D1Database,
    PROCESSING_QUEUE: { send: vi.fn().mockResolvedValue(undefined) } as unknown as Queue<{ fileKey: string; metadataId: number; requestId: string }>,
  };
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
  let env: Env;
  beforeEach(() => { env = makeEnv(); });

  it("rejects missing credentials", async () => {
    const response = await handleIngest(makeRequest({ length: "5", idempotency: "request-123" }), env);
    expect(response.status).toBe(401);
    expect(env.AUDIO_BUCKET.put).not.toHaveBeenCalled();
  });

  it("rejects non-audio content", async () => {
    const response = await handleIngest(makeRequest({ authorization: `Bearer ${token}`, contentType: "text/plain", length: "5", idempotency: "request-123" }), env);
    expect(response.status).toBe(415);
  });

  it("rejects oversized uploads before storage", async () => {
    const response = await handleIngest(makeRequest({ authorization: `Bearer ${token}`, length: "101", idempotency: "request-123" }), env);
    expect(response.status).toBe(413);
    expect(env.AUDIO_BUCKET.put).not.toHaveBeenCalled();
  });

  it("requires idempotency keys", async () => {
    const response = await handleIngest(makeRequest({ authorization: `Bearer ${token}`, length: "5" }), env);
    expect(response.status).toBe(400);
  });

  it("accepts a valid request and enqueues work", async () => {
    const response = await handleIngest(makeRequest({ authorization: `Bearer ${token}`, length: "5", idempotency: "request-123" }), env);
    expect(response.status).toBe(202);
    expect(env.AUDIO_BUCKET.put).toHaveBeenCalledOnce();
    expect(env.PROCESSING_QUEUE.send).toHaveBeenCalledOnce();
  });
});
