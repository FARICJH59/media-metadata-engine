import type { Env, ProcessingMessage } from "../types";

interface OutboxRow {
  id: number;
  metadata_id: number;
  file_key: string;
  request_id: string;
}

async function dispatchRow(env: Env, row: OutboxRow): Promise<boolean> {
  const message: ProcessingMessage = {
    fileKey: row.file_key,
    metadataId: row.metadata_id,
    requestId: row.request_id,
  };
  try {
    await env.PROCESSING_QUEUE.send(message, { contentType: "json" });
    await env.DB.prepare(
      "UPDATE processing_outbox SET status = 'ENQUEUED', attempts = attempts + 1, last_error = NULL, dispatched_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'READY'",
    ).bind(row.id).run();
    return true;
  } catch (error) {
    // Leave the row READY so a later scheduled pass can retry it.
    try {
      await env.DB.prepare(
        "UPDATE processing_outbox SET attempts = attempts + 1, last_error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'READY'",
      ).bind(error instanceof Error ? error.message.slice(0, 240) : "QUEUE_DISPATCH_FAILED", row.id).run();
    } catch (recordError) {
      console.error(JSON.stringify({
        event: "outbox_failure_record_failed",
        outboxId: row.id,
        error: recordError instanceof Error ? recordError.message : "unknown",
      }));
    }
    console.error(JSON.stringify({
      event: "outbox_dispatch_failed",
      outboxId: row.id,
      metadataId: row.metadata_id,
      error: error instanceof Error ? error.message : "unknown",
    }));
    return false;
  }
}

export async function dispatchOutboxForMetadata(env: Env, metadataId: number): Promise<boolean> {
  const row = await env.DB.prepare(
    "SELECT id, metadata_id, file_key, request_id FROM processing_outbox WHERE metadata_id = ? AND status = 'READY' LIMIT 1",
  ).bind(metadataId).first<OutboxRow>();
  if (!row) return false;
  return dispatchRow(env, row);
}

export async function dispatchReadyOutbox(env: Env, limit = 25): Promise<number> {
  const safeLimit = Math.max(1, Math.min(Math.floor(limit), 100));
  const ready = await env.DB.prepare(
    "SELECT id, metadata_id, file_key, request_id FROM processing_outbox WHERE status = 'READY' ORDER BY id LIMIT ?",
  ).bind(safeLimit).all<OutboxRow>();
  let dispatched = 0;

  for (const row of ready.results ?? []) {
    const success = await dispatchRow(env, row);
    if (!success) break; // Avoid repeatedly hitting a failing queue in the same pass.
    dispatched++;
  }
  return dispatched;
}
