# musproj — Portable Music Swiss-Army Knife (MVP backend)

Backend foundation:
- Fastify API (`apps/api`)
- BullMQ Worker (`apps/worker`)
- Upstash Redis (queue + metadata)
- Cloudflare R2 (S3-compatible storage)
- Asset versioning + signed upload/download
- Loop preview + loop apply (currently copy-based)

## Requirements
- Node.js (recommended LTS)
- pnpm
- Upstash Redis database
- Cloudflare R2 bucket + S3 API keys

## Setup
1) Install deps from repo root:
```bash
pnpm install
