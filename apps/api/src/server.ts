import dotenv from "dotenv";
import { fileURLToPath } from "url";
import { dirname, resolve } from "path";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
dotenv.config({ path: resolve(__dirname, "../.env") });

import Fastify from "fastify";
import cors from "@fastify/cors";
import IORedis from "ioredis";
import { Queue } from "bullmq";

import crypto from "crypto";
import { S3Client, PutObjectCommand, GetObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

function base64UrlEncode(obj: any) {
  const json = JSON.stringify(obj);
  return Buffer.from(json, "utf8").toString("base64").replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
}
function base64UrlDecode(s: string) {
  const pad = s.length % 4 === 0 ? "" : "=".repeat(4 - (s.length % 4));
  const b64 = s.replaceAll("-", "+").replaceAll("_", "/") + pad;
  const json = Buffer.from(b64, "base64").toString("utf8");
  return JSON.parse(json);
}

function isPlainObject(x: any): x is Record<string, any> {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

type CreateJobRequest = {
  type: string;
  provider?: string;
  assetId: string;
  inputVersion?: number;
  params?: Record<string, any>;
};

function parseCreateJobBody(
  raw: any
): { ok: true; data: CreateJobRequest } | { ok: false; status: number; body: any } {
  if (!isPlainObject(raw)) {
    return {
      ok: false,
      status: 400,
      body: { error: "BAD_INPUT", message: "POST /jobs body must be a JSON object" },
    };
  }

  const type = typeof raw.type === "string" ? raw.type.trim() : "";
  const provider = typeof raw.provider === "string" ? raw.provider.trim() : undefined;
  const assetId = typeof raw.assetId === "string" ? raw.assetId.trim() : "";

  const inputVersion =
    raw.inputVersion == null ? undefined : Number.isFinite(Number(raw.inputVersion)) ? Number(raw.inputVersion) : NaN;

  const params = raw.params == null ? undefined : raw.params;

  if (!type) return { ok: false, status: 400, body: { error: "BAD_INPUT", message: "type is required (string)" } };
  if (!assetId) return { ok: false, status: 400, body: { error: "BAD_INPUT", message: "assetId is required (string)" } };
  if (provider != null && !provider)
    return { ok: false, status: 400, body: { error: "BAD_INPUT", message: "provider must be non-empty string" } };

  if (inputVersion !== undefined) {
    if (!Number.isFinite(inputVersion) || inputVersion <= 0 || !Number.isInteger(inputVersion)) {
      return { ok: false, status: 400, body: { error: "BAD_INPUT", message: "inputVersion must be a positive integer" } };
    }
  }
  if (params !== undefined && !isPlainObject(params)) {
    return { ok: false, status: 400, body: { error: "BAD_INPUT", message: "params must be an object if provided" } };
  }

  return { ok: true, data: { type, provider, assetId, inputVersion, params: params as any } };
}

function peaksObjectKey(assetId: string, version: number) {
  return `assets/${assetId}/analysis/v${version}/peaks.json`;
}

async function safeReadR2Json(s3: S3Client, bucket: string, key: string): Promise<any | null> {
  try {
    const resp = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    if (!resp.Body) return null;
    const chunks: Buffer[] = [];
    for await (const c of resp.Body as any) chunks.push(Buffer.from(c));
    const txt = Buffer.concat(chunks).toString("utf8");
    return JSON.parse(txt);
  } catch {
    return null;
  }
}

async function main() {
  const redisUrl = process.env.REDIS_URL?.trim();
  if (!redisUrl) throw new Error("REDIS_URL missing in apps/api/.env");

  const connection = new IORedis(redisUrl, { maxRetriesPerRequest: null, tls: {} });
  const audioQueue = new Queue("audio-jobs", { connection });

  const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID?.trim();
  const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID?.trim();
  const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY?.trim();
  const R2_BUCKET = process.env.R2_BUCKET?.trim();

  if (!R2_ACCOUNT_ID || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY || !R2_BUCKET) {
    throw new Error("R2 env missing. Add R2_* vars in apps/api/.env");
  }

  const s3 = new S3Client({
    region: "auto",
    endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: { accessKeyId: R2_ACCESS_KEY_ID, secretAccessKey: R2_SECRET_ACCESS_KEY },
    forcePathStyle: true,
    requestChecksumCalculation: "WHEN_REQUIRED",
    responseChecksumValidation: "WHEN_REQUIRED",
  });

  const app = Fastify({ logger: true });
  await app.register(cors, { origin: true });

  app.addHook("onRequest", async (req, reply) => {
    const requestId = crypto.randomUUID();
    (req as any).requestId = requestId;
    reply.header("X-Request-Id", requestId);
  });

  const JOB_CONFIG: Record<string, { attempts: number; timeout: number; backoff: { type: "exponential"; delay: number } }> =
    {
      LOOP_APPLY: {
        attempts: Number(process.env.LOOP_APPLY_ATTEMPTS ?? 3),
        timeout: Number(process.env.LOOP_APPLY_TIMEOUT_MS ?? 120000),
        backoff: { type: "exponential", delay: Number(process.env.LOOP_APPLY_BACKOFF_MS ?? 2000) },
      },
      ANALYZE: {
        attempts: Number(process.env.AUDIO_ANALYZE_ATTEMPTS ?? 3),
        timeout: Number(process.env.AUDIO_ANALYZE_TIMEOUT_MS ?? 120000),
        backoff: { type: "exponential", delay: Number(process.env.AUDIO_ANALYZE_BACKOFF_MS ?? 2000) },
      },
      CONVERT: {
        attempts: 3,
        timeout: 120000,
        backoff: { type: "exponential", delay: 2000 },
      },
      DEFAULT: {
        attempts: Number(process.env.JOB_ATTEMPTS_DEFAULT ?? 3),
        timeout: Number(process.env.JOB_TIMEOUT_DEFAULT_MS ?? 120000),
        backoff: { type: "exponential", delay: Number(process.env.JOB_BACKOFF_DEFAULT_MS ?? 2000) },
      },
    };

  function cfgFor(type: string) {
    return JOB_CONFIG[type] ?? JOB_CONFIG.DEFAULT;
  }

  app.get("/health", async () => ({ status: "ok" }));

  // ops registry
  app.get("/ops", async (_req, reply) => {
    return reply.send({
      ops: [
        {
          type: "DENOISE",
          providers: [{ provider: "auphonic", schemaHint: { params: { auphonic: { preset: "string (optional)" } } } }],
        },
        {
          type: "LOOP_APPLY",
          providers: [
            { provider: "ffmpeg", schemaHint: { params: { loop: { startMs: "number", endMs: "number", crossfadeMs: "number (optional)" } } } },
          ],
        },
        {
          type: "CONVERT",
          providers: [{ provider: "ffmpeg", schemaHint: { params: {} } }],
        },
        {
          type: "ANALYZE",
          providers: [{ provider: "ffmpeg", schemaHint: { params: {} } }],
        },
      ],
    });
  });

  // uploads/request: allow any audio (do NOT force ContentType)
  app.post("/uploads/request", async (_req, reply) => {
    const assetId = crypto.randomUUID();
    const objectKey = `assets/${assetId}/v1/source.bin`; // raw import blob
    const createdAt = new Date().toISOString();
    const createdAtMs = Date.parse(createdAt);

    const uploadUrl = await getSignedUrl(
      s3,
      new PutObjectCommand({
        Bucket: R2_BUCKET,
        Key: objectKey,
        // ContentType intentionally omitted to allow m4a/mp3/wav
      }),
      { expiresIn: 60 * 10 }
    );

    await connection.hset(`asset:${assetId}`, { assetId, createdAt, status: "uploaded_pending" });
    await connection.zadd("assets:index", String(createdAtMs), assetId);

    await connection.set(`asset:${assetId}:latestVersion`, "1");
    await connection.hset(`asset:${assetId}:v:1`, {
      version: "1",
      objectKey,
      kind: "source",
      status: "READY",
      createdAt,
    });
    await connection.rpush(`asset:${assetId}:versions`, "1");

    return reply.send({
      assetId,
      version: 1,
      objectKey,
      upload: { method: "PUT", url: uploadUrl, headers: {}, expiresInSec: 600 },
      note: "Upload any audio format; server will normalize to WAV on confirm (CONVERT).",
    });
  });

  // confirm: enqueue CONVERT (idempotent) -> will create v2/source.wav
  app.post("/uploads/confirm/:assetId", async (req, reply) => {
    const assetId = (req.params as any).assetId as string;
    const requestId = (req as any).requestId as string;

    const metaV1 = await connection.hgetall(`asset:${assetId}:v:1`);
    if (!metaV1?.objectKey) return reply.code(404).send({ error: "Asset v1 not found" });

    // ensure file exists
    await s3.send(new HeadObjectCommand({ Bucket: R2_BUCKET, Key: metaV1.objectKey }));

    await connection.hset(`asset:${assetId}`, { status: "ready" });

    // idempotent key for convert
    const idemKey = `idem:CONVERT:${assetId}:v1`;
    const existingJobId = await connection.get(idemKey);
    if (existingJobId) {
      return reply.send({ assetId, status: "ready", convertJobId: existingJobId, idem: true });
    }

    const cfg = cfgFor("CONVERT");

    const jobData = {
      type: "CONVERT",
      provider: "ffmpeg",
      assetId,
      inputVersion: 1,
      requestId,
      idemKey,
    };

    const job = await audioQueue.add("audio-job", jobData, {
      removeOnComplete: false,
      removeOnFail: false,
      attempts: cfg.attempts,
      backoff: cfg.backoff,
      timeout: cfg.timeout,
    });

    await connection.set(idemKey, String(job.id));
    await connection.hset(`job:${job.id}`, {
      jobId: String(job.id),
      assetId,
      type: "CONVERT",
      provider: "ffmpeg",
      inputVersion: "1",
      requestId,
      createdAt: new Date().toISOString(),
      attemptsMax: String(cfg.attempts),
      timeoutMs: String(cfg.timeout),
      idemKey,
    });

    return reply.send({ assetId, status: "ready", convertJobId: String(job.id), idem: false });
  });

  // download
  app.get("/assets/:assetId/download", async (req, reply) => {
    const assetId = (req.params as any).assetId as string;
    const versionQ = (req.query as any)?.version as string | undefined;

    const latestStr = await connection.get(`asset:${assetId}:latestVersion`);
    const latest = latestStr ? Number(latestStr) : 1;
    const version = versionQ ? Number(versionQ) : latest;

    const vMeta = await connection.hgetall(`asset:${assetId}:v:${version}`);
    if (!vMeta?.objectKey) return reply.code(404).send({ error: "Version not found" });

    try {
      await s3.send(new HeadObjectCommand({ Bucket: R2_BUCKET, Key: vMeta.objectKey }));
    } catch {
      return reply.code(404).send({ error: "File not found in storage", assetId, version, objectKey: vMeta.objectKey });
    }

    const downloadUrl = await getSignedUrl(s3, new GetObjectCommand({ Bucket: R2_BUCKET, Key: vMeta.objectKey }), {
      expiresIn: 60 * 10,
    });

    return reply.send({ assetId, version, url: downloadUrl, expiresInSec: 600 });
  });

  // assets list
  app.get("/assets", async (req, reply) => {
    const q = req.query as any;
    const limitRaw = Number(q?.limit ?? 20);
    const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(50, limitRaw)) : 20;

    let maxScore: string = "+inf";
    if (q?.cursor) {
      try {
        const decoded = base64UrlDecode(String(q.cursor));
        const score = Number(decoded?.score);
        if (Number.isFinite(score)) maxScore = `(${score}`;
      } catch {}
    }

    const ids = await connection.zrevrangebyscore("assets:index", maxScore, "-inf", "LIMIT", 0, limit);

    const items = [];
    for (const assetId of ids) {
      const asset = await connection.hgetall(`asset:${assetId}`);
      const latestStr = await connection.get(`asset:${assetId}:latestVersion`);
      const latestVersion = latestStr ? Number(latestStr) : 1;
      items.push({ assetId, createdAt: asset?.createdAt ?? null, status: asset?.status ?? null, latestVersion });
    }

    let nextCursor: string | null = null;
    if (ids.length === limit) {
      const lastId = ids[ids.length - 1];
      const scoreStr = await connection.zscore("assets:index", lastId);
      const score = scoreStr ? Number(scoreStr) : null;
      if (score != null && Number.isFinite(score)) nextCursor = base64UrlEncode({ score });
    }

    return reply.send({ items, nextCursor });
  });

  // asset details
  app.get("/assets/:assetId", async (req, reply) => {
    const assetId = (req.params as any).assetId as string;
    const asset = await connection.hgetall(`asset:${assetId}`);
    if (!asset?.assetId) return reply.code(404).send({ error: "Asset not found" });

    const latestStr = await connection.get(`asset:${assetId}:latestVersion`);
    const latestVersion = latestStr ? Number(latestStr) : 1;

    const includeVersions = String((req.query as any)?.includeVersions ?? "1") !== "0";
    if (!includeVersions) return reply.send({ asset, latestVersion });

    const versionStrs = await connection.lrange(`asset:${assetId}:versions`, 0, -1);

    const versions = [];
    for (const vStr of versionStrs) {
      const v = Number(vStr);
      if (!Number.isFinite(v)) continue;
      versions.push(await connection.hgetall(`asset:${assetId}:v:${v}`));
    }

    return reply.send({ asset, latestVersion, versions });
  });

  // peaks v0 real
  app.get("/assets/:assetId/peaks", async (req, reply) => {
    const assetId = (req.params as any).assetId as string;
    const versionQ = (req.query as any)?.version as string | undefined;

    const latestStr = await connection.get(`asset:${assetId}:latestVersion`);
    const latest = latestStr ? Number(latestStr) : 1;
    const version = versionQ ? Number(versionQ) : latest;

    const vMeta = await connection.hgetall(`asset:${assetId}:v:${version}`);
    if (!vMeta?.objectKey) return reply.code(404).send({ error: "Version not found", assetId, version });

    // 1) redis cache
    const cacheKey = `asset:${assetId}:peaks:v${version}`;
    const cached = await connection.get(cacheKey);
    if (cached) {
      try {
        const obj = JSON.parse(cached);
        return reply.send(obj);
      } catch {}
    }

    // 2) R2 json
    const key = peaksObjectKey(assetId, version);
    const fromR2 = await safeReadR2Json(s3, R2_BUCKET, key);
    if (fromR2) {
      await connection.set(cacheKey, JSON.stringify(fromR2));
      return reply.send(fromR2);
    }

    // 3) enqueue ANALYZE (idempotent)
    const requestId = (req as any).requestId as string;
    const idemKey = `idem:ANALYZE:${assetId}:v${version}`;
    const existingJobId = await connection.get(idemKey);
    if (existingJobId) {
      return reply.code(202).send({
        ok: false,
        status: "PROCESSING",
        jobId: existingJobId,
        assetId,
        version,
        message: "Peaks not ready yet; analysis job already exists",
      });
    }

    const cfg = cfgFor("ANALYZE");
    const jobData = {
      type: "ANALYZE",
      provider: "ffmpeg",
      assetId,
      inputVersion: version,
      requestId,
      idemKey,
    };

    const job = await audioQueue.add("audio-job", jobData, {
      removeOnComplete: false,
      removeOnFail: false,
      attempts: cfg.attempts,
      backoff: cfg.backoff,
      timeout: cfg.timeout,
    });

    await connection.set(idemKey, String(job.id));
    await connection.hset(`job:${job.id}`, {
      jobId: String(job.id),
      assetId,
      type: "ANALYZE",
      provider: "ffmpeg",
      inputVersion: String(version),
      requestId,
      createdAt: new Date().toISOString(),
      attemptsMax: String(cfg.attempts),
      timeoutMs: String(cfg.timeout),
      idemKey,
    });

    return reply.code(202).send({
      ok: false,
      status: "PROCESSING",
      jobId: String(job.id),
      assetId,
      version,
      message: "Peaks not found; analysis job created",
    });
  });

  // POST /jobs (unified)
  app.post("/jobs", async (req, reply) => {
    const parsed = parseCreateJobBody(req.body);
    if (!parsed.ok) return reply.code(parsed.status).send(parsed.body);

    const body = parsed.data;
    const requestId = (req as any).requestId as string;

    const latestStr = await connection.get(`asset:${body.assetId}:latestVersion`);
    const latest = latestStr ? Number(latestStr) : 1;
    const inputVersion = body.inputVersion ?? latest;

    const vMeta = await connection.hgetall(`asset:${body.assetId}:v:${inputVersion}`);
    if (!vMeta?.objectKey) {
      return reply.code(404).send({ error: "Input version not found", assetId: body.assetId, inputVersion });
    }

    const cfg = cfgFor(body.type);

    const jobData: any = {
      type: body.type,
      provider: body.provider,
      assetId: body.assetId,
      inputVersion,
      requestId,
    };

    if (body.params) Object.assign(jobData, body.params);

    // minimal LOOP_APPLY validation
    if (body.type === "LOOP_APPLY") {
      const loop = jobData.loop;
      const startMs = Number(loop?.startMs);
      const endMs = Number(loop?.endMs);
      if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
        return reply.code(400).send({
          error: "BAD_INPUT",
          message: "LOOP_APPLY requires params.loop with startMs/endMs and endMs > startMs",
        });
      }
    }

    const job = await audioQueue.add("audio-job", jobData, {
      removeOnComplete: false,
      removeOnFail: false,
      attempts: cfg.attempts,
      backoff: cfg.backoff,
      timeout: cfg.timeout,
    });

    await connection.hset(`job:${job.id}`, {
      jobId: String(job.id),
      assetId: body.assetId,
      type: body.type,
      provider: body.provider ?? "",
      inputVersion: String(inputVersion),
      requestId,
      createdAt: new Date().toISOString(),
      attemptsMax: String(cfg.attempts),
      timeoutMs: String(cfg.timeout),
    });

    return reply.send({ jobId: String(job.id), requestId });
  });

  // jobs/:id
  app.get("/jobs/:id", async (req, reply) => {
    const id = (req.params as any).id as string;
    const job = await audioQueue.getJob(id);
    if (!job) return reply.code(404).send({ error: "Job not found" });

    const state = await job.getState();
    const meta = await connection.hgetall(`job:${id}`);

    const error =
      meta?.errorCode || meta?.errorMessage
        ? { code: meta.errorCode ?? null, message: meta.errorMessage ?? null, retryable: meta.errorRetryable ? meta.errorRetryable === "true" : null }
        : null;

    return reply.send({
      jobId: String(job.id),
      state,
      status: meta?.status ?? state,
      progress: job.progress,
      attemptsMade: job.attemptsMade ?? 0,
      attemptsMax: job.opts.attempts ?? Number(meta?.attemptsMax ?? 1),
      startedAt: meta?.startedAt ?? null,
      finishedAt: meta?.finishedAt ?? null,
      requestId: meta?.requestId ?? null,
      result: job.returnvalue ?? null,
      failedReason: job.failedReason ?? null,
      error,
      provider: meta?.provider ?? null,
      type: meta?.type ?? null,
    });
  });

  await app.listen({ port: 3000, host: "127.0.0.1" });
  console.log("API on http://127.0.0.1:3000");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
