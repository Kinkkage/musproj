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

## Architecture

This project uses a modular audio-processing architecture.

- API creates jobs
- Worker executes jobs
- Each processing step is a module

### Worker responsibilities
- job orchestration
- versioning
- storage (R2)
- metadata

### Module responsibilities
- actual audio processing
- producing output audio
- returning objectKey

Modules must implement ProcessingModule interface.
