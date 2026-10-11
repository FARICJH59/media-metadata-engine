import type { Env, ProcessingMessage } from "./types";
import { handleIngest } from "./handlers/ingest";
import { handleStatus } from "./handlers/status";
import { handleQueueBatch } from "./handlers/processor";

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/health" && request.method === "GET") {
      return Response.json({ ok: true, service: "media-metadata-engine" }, {
        headers: { "cache-control": "no-store" },
      });
    }
    if (url.pathname === "/ingest") return handleIngest(request, env);
    if (url.pathname.startsWith("/status/")) return handleStatus(request, env);

    return Response.json({ error: "Not found" }, { status: 404 });
  },

  async queue(batch: MessageBatch<ProcessingMessage>, env: Env): Promise<void> {
    await handleQueueBatch(batch, env);
  },
};
