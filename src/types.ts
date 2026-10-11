export type MetadataStatus = "PENDING" | "PROCESSING" | "COMPLETED" | "FAILED";

export interface ProcessingMessage {
  fileKey: string;
  metadataId: number;
  requestId: string;
}

export interface AudioMetadata {
  id: number;
  file_key: string;
  file_size: number;
  content_type: string;
  status: MetadataStatus;
  idempotency_key: string;
  request_id: string;
  error_code: string | null;
  created_at: string;
  updated_at: string;
}

export interface Env {
  AUDIO_BUCKET: R2Bucket;
  PROCESSING_QUEUE: Queue<ProcessingMessage>;
  DB: D1Database;
  INGEST_API_TOKEN: string;
  MAX_UPLOAD_BYTES?: string;
}

export function jsonError(message: string, status: number, requestId?: string): Response {
  return Response.json(
    { error: message, ...(requestId ? { requestId } : {}) },
    { status, headers: { "cache-control": "no-store" } },
  );
}

export function hasValidBearer(request: Request, env: Env): boolean {
  const configured = env.INGEST_API_TOKEN;
  if (!configured || configured.length < 32) return false;
  const header = request.headers.get("authorization") ?? "";
  const match = /^Bearer (.+)$/i.exec(header);
  if (!match) return false;
  const supplied = match[1];
  if (supplied.length !== configured.length) return false;
  let difference = 0;
  for (let i = 0; i < configured.length; i++) {
    difference |= configured.charCodeAt(i) ^ supplied.charCodeAt(i);
  }
  return difference === 0;
}

export function validIdempotencyKey(value: string | null): value is string {
  return value !== null && /^[A-Za-z0-9._:-]{8,128}$/.test(value);
}
