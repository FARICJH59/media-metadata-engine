import type { Env, ProcessingMessage } from "../types";
import { hasValidBearer, jsonError, validIdempotencyKey } from "../types";

const AUDIO_TYPE = /^audio\/[a-z0-9.+-]+$/i;

export async function handleIngest(request: Request, env: Env): Promise<Response> {
  const requestId = crypto.randomUUID();
  if (request.method !== "POST") return jsonError("Method not allowed", 405, requestId);
  if (!hasValidBearer(request, env)) return jsonError("Unauthorized", 401, requestId);

  const contentType = (request.headers.get("content-type") ?? "").split(";")[0].trim();
  if (!AUDIO_TYPE.test(contentType)) {
    return jsonError("Unsupported content type; an audio/* type is required", 415, requestId);
  }
  const rawLength = request.headers.get("content-length");
  if (!rawLength || !/^\d+$/.test(rawLength)) {
    return jsonError("A valid Content-Length header is required", 411, requestId);
  }
  const fileSize = Number(rawLength);
  const maxBytes = Number(env.MAX_UPLOAD_BYTES ?? "26214400");
  if (!Number.isSafeInteger(fileSize) || fileSize <= 0) {
    return jsonError("Invalid audio payload size", 400, requestId);
  }
  if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0 || fileSize > maxBytes) {
    return jsonError("Audio payload exceeds the configured upload limit", 413, requestId);
  }
  if (!request.body) return jsonError("Missing audio payload", 400, requestId);

  const idempotencyKey = request.headers.get("idempotency-key");
  if (!validIdempotencyKey(idempotencyKey)) {
    return jsonError("A valid Idempotency-Key (8-128 safe characters) is required", 400, requestId);
  }

  const prior = await env.DB.prepare(
    "SELECT id, file_key, status FROM metadata WHERE idempotency_key = ?",
  ).bind(idempotencyKey).first<{ id: number; file_key: string; status: string }>();

  if (prior) {
    return Response.json(
      { success: true, duplicate: true, metadataId: prior.id, fileKey: prior.file_key, status: prior.status },
      { status: 200, headers: { "cache-control": "no-store" } },
    );
  }

  const fileKey = `ingest/${new Date().toISOString().slice(0, 10)}/${crypto.randomUUID()}`;
  let objectStored = false;
  try {
    await env.AUDIO_BUCKET.put(fileKey, request.body, {
      httpMetadata: { contentType },
      customMetadata: { requestId, idempotencyKey },
    });
    objectStored = true;

    const inserted = await env.DB.prepare(
      "INSERT INTO metadata (file_key, file_size, content_type, status, idempotency_key, request_id) VALUES (?, ?, ?, 'PENDING', ?, ?) RETURNING id",
    ).bind(fileKey, fileSize, contentType, idempotencyKey, requestId).first<{ id: number }>();
    if (!inserted?.id) throw new Error("D1 insert did not return an ID");

    const message: ProcessingMessage = { fileKey, metadataId: inserted.id, requestId };
    try {
      await env.PROCESSING_QUEUE.send(message, { contentType: "json" });
    } catch {
      await env.DB.prepare(
        "UPDATE metadata SET error_code = 'QUEUE_ENQUEUE_FAILED', updated_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'PENDING'",
      ).bind(inserted.id).run();
      return jsonError("Audio stored but queue dispatch failed; reconciliation is required", 503, requestId);
    }

    return Response.json(
      { success: true, message: "Audio accepted for processing", fileKey, metadataId: inserted.id, status: "PENDING", requestId },
      { status: 202, headers: { "cache-control": "no-store" } },
    );
  } catch (error) {
    if (objectStored) {
      try {
        const row = await env.DB.prepare("SELECT id FROM metadata WHERE file_key = ?")
          .bind(fileKey).first<{ id: number }>();
        if (!row) await env.AUDIO_BUCKET.delete(fileKey);
      } catch {
        // Reconciliation must detect an orphan if compensation cannot be verified.
      }
    }
    console.error(JSON.stringify({ event: "ingest_failed", requestId, error: error instanceof Error ? error.message : "unknown" }));
    return jsonError("Ingestion failed", 500, requestId);
  }
}
