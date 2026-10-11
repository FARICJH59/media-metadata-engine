import type { Env } from "../types";
import { hasValidBearer, jsonError } from "../types";

export async function handleStatus(request: Request, env: Env): Promise<Response> {
  const requestId = crypto.randomUUID();
  if (request.method !== "GET") return jsonError("Method not allowed", 405, requestId);
  if (!hasValidBearer(request, env)) return jsonError("Unauthorized", 401, requestId);

  const idText = new URL(request.url).pathname.slice("/status/".length);
  if (!/^\d+$/.test(idText)) return jsonError("Invalid metadata ID", 400, requestId);
  const id = Number(idText);
  if (!Number.isSafeInteger(id) || id <= 0) return jsonError("Invalid metadata ID", 400, requestId);

  const row = await env.DB.prepare(
    "SELECT id, file_size, content_type, status, error_code, created_at, updated_at FROM metadata WHERE id = ?",
  ).bind(id).first<Record<string, unknown>>();
  if (!row) return jsonError("Metadata record not found", 404, requestId);

  return Response.json({ metadata: row, requestId }, { headers: { "cache-control": "no-store" } });
}
