# Media Metadata Engine

Cloudflare Worker for authenticated audio ingestion, durable object storage, metadata tracking, and asynchronous processing.

## Routes

- `GET /health`: non-sensitive liveness response.
- `POST /ingest`: bearer-token protected audio ingestion; requires `Content-Length` and `Idempotency-Key`.
- `GET /status/:id`: bearer-token protected metadata status.
- R2 keys are generated server-side; client filenames are never used as storage keys.
- D1 schema is managed by `migrations/`.
- Queue processing verifies object presence but deliberately records `PROCESSOR_NOT_CONFIGURED`. It does **not** claim acoustic extraction is complete.

## Required configuration

1. Create the R2 bucket `spatial-audio-ingest`.
2. Create the Queue `audio-processing-queue`.
3. Create D1 database `media_metadata_db` and replace `YOUR_D1_DATABASE_ID` in `wrangler.toml`.
4. Apply schema: `npx wrangler d1 migrations apply media_metadata_db --remote`.
5. Set a strong secret: `npx wrangler secret put INGEST_API_TOKEN` (at least 32 characters).
6. Configure GitHub Actions secrets `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID`.

Never commit API tokens or database credentials.

## Development

```sh
npm install
npm run check
npm test
npx wrangler dev
```

The v1 authorization model is a single service bearer token, not tenant isolation. HOARE integration should provide verified identity/tenant context and enforce current admission/revocation policy at the processing boundary.

## Reliability boundary

R2, D1, and Queue do not form one atomic transaction. The handler uses best-effort compensation and records queue dispatch failures; stale `PENDING` rows and orphaned R2 objects require reconciliation. A durable outbox/reconciler is required before high-assurance production use.
