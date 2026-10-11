import type { Env, ProcessingMessage } from "../types";
import { hasValidBearer, jsonError, validIdempotencyKey } from "../types";

const AUDIO_TYPE = /^audio\/[a-z0-9.+-]+$/i;

function duplicateResponse(prior: { id: number; file_key: string; status: string }): Response {
  return Response.json(
    { success: true, duplicate: true, metadataId: prior.id, fileKey: prior.file_key, status: prior.status },
    { status: 200, headers: { "cache-control": "no-store" } },
  );
}

export async function handleIngest(request: Request, env: Env): Promise<Response> {
  const requestId = crypto.randomUUID();
  if (request.method !== "POST") return jsonError("Method not allowed", 405, requestId);
  if (!hasValidBearer(request, env)) return jsonError("Unauthorized", 401, requestId);

  const contentType = (request.headers.get("content-type") ?? "").split(";")[0].trim();
  if (!AUDIO_TYPE.test(contentType)) {
    return jsonError("Unsupported content type; an audio/* type is required", 415, requestId);
  }
  const rawLength = request.headers.get("content-length");
  if (!rawLength || !/^\\d+$/.test(rawLength)) {
    return jsonError("A valid Content-Length header is required", 411, requestId);
  }
  const declaredSize = Number(rawLength);
  const maxBytes = Number(env.MAX_UPLOAD_BYTES ?? "26214400");
  if (!Number.isSafeInteger(declaredSize) || declaredSize <= 0) {
    return jsonError("Invalid audio payload size", 400, requestId);
  }
  if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0 || declaredSize > maxBytes) {
    return jsonError("Audio payload exceeds the configured upload limit", 413, requestId);
  }
  if (!request.body) return jsonError("Missing audio payload", 400, requestId);

  const idempotencyKey = request.headers.get("idempotency-key");
  if (!validIdempotencyKey(idempotencyKey)) {
    return jsonError("A valid Idempotency-Key (8-128 safe characters) is required", 400, requestId);
  }

  const findPrior = () => env.DB.prepare(
    "SELECT id, file_key, status FROM metadata WHERE idempotency_key = ?",
  ).bind(idempotencyKey).first<{ id: number; file_key: string; status: string }>();

  const prior = await findPrior();
  if (prior) return duplicateResponse(prior);

  const fileKey = `ingest/${new Date().toISOString().slice(0, 10)}/${crypto.randomUUID()}`;
  let objectStored = false;
  try {
    const stored = await env.AUDIO_BUCKET.put(fileKey, request.body, {
      httpMetadata: { contentType },
      customMetadata: { requestId, idempotencyKey },
    });
    objectStored = true;

    // Use the storage provider's measured size, not the client-declared header, as authority.
    if (!stored || !Number.isSafeInteger(stored.size) || stored.size !== declaredSize) {
      await env.AUDIO_BUCKET.delete(fileKey);
      objectStored = false;
      return jsonError("Uploaded byte count does not match Content-Length", 400, requestId);
    }
    if (stored.size > maxBytes) {
      await env.AUDIO_BUCKET.delete(fileKey);
      objectStored = false;
      return jsonError("Audio payload exceeds the configured upload limit", 413, requestId);
    }

    const inserted = await env.DB.prepare(
      "INSERT INTO metadata (file_key, file_size, content_type, status, idempotency_key, request_id) VALUES (?, ?, ?, 'PENDING', ?, ?) RETURNING id",
    ).bind(fileKey, stored.size, contentType, idempotencyKey, requestId).first<{ id: number }>();
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
    // A concurrent request can win the unique idempotency constraint after our initial SELECT.
    // Only treat this as a duplicate if D1 confirms the matching key now exists.
    try {
      const concurrent = await findPrior();
      if (concurrent) {
        if (objectStored && concurrent.file_key !== fileKey) {
          try {
            await env.AUDIO_BUCKET.delete(fileKey);
            objectStored = false;
          } catch {
            console.error(JSON.stringify({ event: "ingest_orphan_cleanup_failed", requestId, fileKey }));
          }
        }
        return duplicateResponse(concurrent);
      }
    } catch {
      // Preserve the original failure; reconciliation must detect any orphaned object.
    }
    if (objectStored) {
      try {
        const row = await env.DB.prepare("SELECT id FROM metadata WHERE file_key = ?")
          .bind(fileKey).first<{ id: number }>();
        if (!row) await env.AUDIO_BUCKET.delete(fileKey);
      } catch {
        console.error(JSON.stringify({ event: "ingest_compensation_unverified", requestId, fileKey }));
      }
    }
    console.error(JSON.stringify({ event: "ingest_failed", requestId, error: error instanceof Error ? error.message : "unknown" }));
    return jsonError("Ingestion failed", 500, requestId);
  }
}
