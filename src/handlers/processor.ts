import type { Env, ProcessingMessage } from "../types";

export async function handleQueueBatch(
  batch: MessageBatch<ProcessingMessage>,
  env: Env,
): Promise<void> {
  for (const message of batch.messages) {
    const { fileKey, metadataId, requestId } = message.body;
    try {
      const object = await env.AUDIO_BUCKET.head(fileKey);
      if (!object) throw new Error("OBJECT_NOT_FOUND");

      const claimed = await env.DB.prepare(
        "UPDATE metadata SET status = 'PROCESSING', error_code = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND file_key = ? AND status IN ('PENDING', 'PROCESSING')",
      ).bind(metadataId, fileKey).run();

      if ((claimed.meta?.changes ?? 0) === 0) {
        const row = await env.DB.prepare("SELECT status FROM metadata WHERE id = ? AND file_key = ?")
          .bind(metadataId, fileKey).first<{ status: string }>();
        if (!row || row.status === "COMPLETED" || row.status === "FAILED") {
          message.ack();
          continue;
        }
      }

      // Do not report acoustic extraction as successful until a real processor and its evidence exist.
      await env.DB.prepare(
        "UPDATE metadata SET status = 'FAILED', error_code = 'PROCESSOR_NOT_CONFIGURED', updated_at = CURRENT_TIMESTAMP WHERE id = ? AND file_key = ? AND status = 'PROCESSING'",
      ).bind(metadataId, fileKey).run();
      console.warn(JSON.stringify({ event: "processor_not_configured", requestId, metadataId, fileKey }));
      message.ack();
    } catch (error) {
      const attempts = message.attempts ?? 1;
      const terminal = attempts >= 5;
      await env.DB.prepare(
        "UPDATE metadata SET status = ?, error_code = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND file_key = ? AND status != 'COMPLETED'",
      ).bind(terminal ? "FAILED" : "PENDING", terminal ? "PROCESSING_RETRIES_EXHAUSTED" : "PROCESSING_RETRY", metadataId, fileKey).run();
      console.error(JSON.stringify({
        event: "processing_failed", requestId, metadataId, attempts, terminal,
        error: error instanceof Error ? error.message : "unknown",
      }));
      if (terminal) message.ack();
      else message.retry({ delaySeconds: Math.min(30 * attempts, 300) });
    }
  }
}
