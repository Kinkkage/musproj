# Architecture (musproj backend foundation)

This repo is the backend foundation for an iOS audio app:
- upload audio to storage (Cloudflare R2)
- keep version history of assets (v1, v2, v3...)
- submit background processing jobs (queue)
- worker executes jobs and writes new versions back to R2

## Components

### apps/api
Fastify HTTP server.
Responsibilities:
- creates signed upload/download URLs for R2 (client uploads directly to R2)
- writes/reads metadata in Redis (Upstash)
- submits jobs to the queue (BullMQ)
- provides job status endpoints for polling

### apps/worker
BullMQ Worker.
Responsibilities:
- listens to queue "audio-jobs"
- downloads input audio from R2 (by objectKey)
- calls external services (Auphonic / LALAL etc.) OR local processing
- uploads processed output back to R2 as a new version
- updates job progress and writes version metadata to Redis

### Storage: Cloudflare R2 (S3-compatible)
We store audio files under keys like:
- assets/<assetId>/v1/source.wav
- assets/<assetId>/v2/enhanced.wav
- assets/<assetId>/v3/vocals.wav
- assets/<assetId>/v3/instrumental.wav

### Metadata + Queue: Upstash Redis
We store asset and version metadata in Redis keys:

Asset meta:
- asset:<assetId> (hash)
  - assetId
  - createdAt
  - status ("ready" etc.)

Latest version:
- asset:<assetId>:latestVersion (string number)

Version list:
- asset:<assetId>:versions (list of version numbers as strings)

Per version metadata:
- asset:<assetId>:v:<version> (hash)
  - version
  - objectKey
  - kind (source/enhanced/loop/vocals/instrumental/...)
  - createdAt
  - fromVersion (optional)

Jobs meta (optional):
- job:<jobId> (hash)

## Typical Flow

1) Client requests upload:
POST /uploads/request
-> API returns:
- assetId
- objectKey
- signed PUT url (client uploads file directly to R2)

2) Client uploads file to R2 using signed PUT url.

3) Client confirms upload:
POST /uploads/confirm/:assetId
-> API checks the object exists in R2 and sets asset status to "ready".

4) Client requests processing:
POST /jobs { type, assetId, inputVersion? }
-> API enqueues a job and returns jobId.

5) Worker processes job:
- reads inputVersion metadata => objectKey
- downloads input from R2
- processes (service/local)
- uploads output to R2 with a new version key
- writes new version metadata + updates latestVersion
- completes job with result (createdVersion etc.)

6) Client polls status:
GET /jobs/:id

7) Client downloads a version:
GET /assets/:assetId/download?version=N
-> API returns signed GET url for R2 object.

## Local development
Run worker and API in two terminals:

Terminal A:
pnpm dev:worker

Terminal B:
pnpm dev:api
