import "dotenv/config";
import Fastify from "fastify";
import cors from "@fastify/cors";
import { z } from "zod";
import IORedis from "ioredis";
import { Queue } from "bullmq";

import crypto from "crypto";
import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

async function main() {
  // -----------------------------
  // 1) Redis (Upstash) connection
  // -----------------------------
  const redisUrl = process.env.REDIS_URL?.trim();
  if (!redisUrl) throw new Error("REDIS_URL missing in apps/api/.env");

  const connection = new IORedis(redisUrl, {
    maxRetriesPerRequest: null,
    tls: {},
  });

  // -----------------------------
  // 2) Queue
  // -----------------------------
  const audioQueue = new Queue("audio-jobs", { connection });

  // -----------------------------
  // 3) R2 (S3-compatible) client
  // -----------------------------
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
    credentials: {
      accessKeyId: R2_ACCESS_KEY_ID,
      secretAccessKey: R2_SECRET_ACCESS_KEY,
    },
    forcePathStyle: true,
    requestChecksumCalculation: "WHEN_REQUIRED",
    responseChecksumValidation: "WHEN_REQUIRED",
  });

  // -----------------------------
  // 4) Fastify app
  // -----------------------------
  const app = Fastify({ logger: true });
  await app.register(cors, { origin: true });

  app.get("/health", async () => ({ status: "ok" }));

  // =========================================================
  // Upload flow (signed upload -> confirm -> signed download)
  // =========================================================

  app.post("/uploads/request", async (_req, reply) => {
    const assetId = crypto.randomUUID();
    const objectKey = `assets/${assetId}/v1/source.wav`;

    const uploadUrl = await getSignedUrl(
      s3,
      new PutObjectCommand({
        Bucket: R2_BUCKET,
        Key: objectKey,
        ContentType: "audio/wav",
      }),
      { expiresIn: 60 * 10 }
    );

    await connection.hset(`asset:${assetId}`, {
      assetId,
      createdAt: new Date().toISOString(),
      status: "uploaded_pending",
    });

    // versioning: v1
    await connection.set(`asset:${assetId}:latestVersion`, "1");
    await connection.hset(`asset:${assetId}:v:1`, {
      version: "1",
      objectKey,
      kind: "source",
      createdAt: new Date().toISOString(),
    });
    await connection.rpush(`asset:${assetId}:versions`, "1");

    return reply.send({
      assetId,
      version: 1,
      objectKey,
      upload: {
        method: "PUT",
        url: uploadUrl,
        headers: { "Content-Type": "audio/wav" },
        expiresInSec: 600,
      },
    });
  });

  app.post("/uploads/confirm/:assetId", async (req, reply) => {
    const assetId = (req.params as any).assetId as string;
    const metaV1 = await connection.hgetall(`asset:${assetId}:v:1`);
    if (!metaV1?.objectKey) return reply.code(404).send({ error: "Asset v1 not found" });

    await s3.send(new HeadObjectCommand({ Bucket: R2_BUCKET, Key: metaV1.objectKey }));
    await connection.hset(`asset:${assetId}`, { status: "ready" });

    return reply.send({ assetId, status: "ready" });
  });

  // download любой версии; перед выдачей URL проверяем, что файл реально существует
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
      return reply.code(404).send({
        error: "File not found in storage",
        assetId,
        version,
        objectKey: vMeta.objectKey,
      });
    }

    const downloadUrl = await getSignedUrl(
      s3,
      new GetObjectCommand({ Bucket: R2_BUCKET, Key: vMeta.objectKey }),
      { expiresIn: 60 * 10 }
    );

    return reply.send({ assetId, version, url: downloadUrl, expiresInSec: 600 });
  });

  // =========================================
  // Asset meta endpoints
  // =========================================

  app.get("/assets/:assetId", async (req, reply) => {
    const assetId = (req.params as any).assetId as string;
    const asset = await connection.hgetall(`asset:${assetId}`);
    if (!asset?.assetId) return reply.code(404).send({ error: "Asset not found" });

    const latestStr = await connection.get(`asset:${assetId}:latestVersion`);
    const latest = latestStr ? Number(latestStr) : 1;

    return reply.send({ asset, latestVersion: latest });
  });

  app.get("/assets/:assetId/versions", async (req, reply) => {
    const assetId = (req.params as any).assetId as string;
    const versions = await connection.lrange(`asset:${assetId}:versions`, 0, -1);
    return reply.send({ assetId, versions });
  });

  // =========================================
  // Loop preview (сохраняем мета региона)
  // =========================================

  const LoopPreviewBody = z.object({
    assetId: z.string().uuid(),
    version: z.number().int().positive(),
    startMs: z.number().int().min(0),
    endMs: z.number().int().min(1),
    crossfadeMs: z.number().int().min(0).max(100).default(10),
  });

  app.post("/loop/preview", async (req, reply) => {
    const body = LoopPreviewBody.parse(req.body);
    if (body.endMs <= body.startMs) {
      return reply.code(400).send({ error: "endMs must be > startMs" });
    }

    const vMeta = await connection.hgetall(`asset:${body.assetId}:v:${body.version}`);
    if (!vMeta?.objectKey) return reply.code(404).send({ error: "Version not found" });

    const key = `asset:${body.assetId}:loopPreview:v${body.version}`;
    await connection.hset(key, {
      assetId: body.assetId,
      version: String(body.version),
      startMs: String(body.startMs),
      endMs: String(body.endMs),
      crossfadeMs: String(body.crossfadeMs),
      updatedAt: new Date().toISOString(),
    });

    return reply.send({ ok: true, savedTo: key });
  });

  // =========================================
  // Loop apply (A): создаём job, worker скопирует файл + сохранит мета
  // =========================================

  const LoopApplyBody = z.object({
    assetId: z.string().uuid(),
    version: z.number().int().positive(), // inputVersion
  });

  app.post("/loop/apply", async (req, reply) => {
  const body = LoopApplyBody.parse(req.body);

  // 1) убеждаемся, что input версия существует
  const inMeta = await connection.hgetall(`asset:${body.assetId}:v:${body.version}`);
  if (!inMeta?.objectKey) return reply.code(404).send({ error: "Input version not found" });

  // 2) берём preview мету
  const previewKey = `asset:${body.assetId}:loopPreview:v${body.version}`;
  const preview = await connection.hgetall(previewKey);
  if (!preview?.startMs || !preview?.endMs) {
    return reply.code(400).send({
      error: "Loop preview not set for this version",
      need: "POST /loop/preview first",
      previewKey,
    });
  }

  const loop = {
    startMs: Number(preview.startMs),
    endMs: Number(preview.endMs),
    crossfadeMs: Number(preview.crossfadeMs ?? "10"),
  };

  // 3) idempotency key (одинаковые входы -> один jobId)
  const paramsString = JSON.stringify({
    assetId: body.assetId,
    inputVersion: body.version,
    loop,
  });

  const paramsHash = crypto.createHash("sha1").update(paramsString).digest("hex");
  const idemKey = `idem:LOOP_APPLY:${body.assetId}:v${body.version}:${paramsHash}`;

  const existingJobId = await connection.get(idemKey);
  if (existingJobId) {
    return reply.send({
      ok: true,
      jobId: existingJobId,
      idem: true,
      note: "Same params -> returning existing jobId",
    });
  }

  // 4) создаём job "LOOP_APPLY"
  const job = await audioQueue.add(
    "audio-job",
    {
      type: "LOOP_APPLY",
      assetId: body.assetId,
      inputVersion: body.version,
      loop,
      idemKey,        // полезно хранить и в job.data
      paramsHash,     // полезно для отладки
    },
    {
    removeOnComplete: false,
    removeOnFail: false,
    attempts: 3,
    backoff: { type: "exponential", delay: 2000 },
    timeout: 2 * 60 * 1000,
}

  );

  // 5) сохраняем idemKey -> jobId (чтобы повтор не создавал дубль)
  await connection.set(idemKey, String(job.id));

  await connection.hset(`job:${job.id}`, {
    jobId: String(job.id),
    assetId: body.assetId,
    type: "LOOP_APPLY",
    inputVersion: String(body.version),
    paramsHash,
    idemKey,
    createdAt: new Date().toISOString(),
  });

  return reply.send({
    ok: true,
    jobId: String(job.id),
    idem: false,
    note: "Worker will create a new version in R2 and attach loop metadata.",
  });
});



  // =========================================
  // Jobs (queue) endpoints
  // =========================================

  const CreateJobBody = z.object({
    type: z.enum(["LOOP", "DENOISE", "SEPARATE"]),
    assetId: z.string().min(1),
    inputVersion: z.number().int().positive().optional(),
  });

  app.post("/jobs", async (req, reply) => {
    const body = CreateJobBody.parse(req.body);

    const latestStr = await connection.get(`asset:${body.assetId}:latestVersion`);
    const latest = latestStr ? Number(latestStr) : 1;
    const inputVersion = body.inputVersion ?? latest;
    // Проверка: существует ли версия inputVersion
const vMeta = await connection.hgetall(`asset:${body.assetId}:v:${inputVersion}`);
if (!vMeta?.objectKey) {
  return reply.code(404).send({
    error: "Input version not found",
    assetId: body.assetId,
    inputVersion,
  });
}

    const job = await audioQueue.add(
  "audio-job",
  { type: body.type, assetId: body.assetId, inputVersion },
  {
    removeOnComplete: false,
    removeOnFail: false,
    attempts: 3,
    backoff: { type: "exponential", delay: 2000 },
    timeout: 2 * 60 * 1000,
  }
);


    await connection.hset(`job:${job.id}`, {
      jobId: String(job.id),
      assetId: body.assetId,
      type: body.type,
      inputVersion: String(inputVersion),
      createdAt: new Date().toISOString(),
    });

    return reply.send({ jobId: String(job.id) });
  });

  app.get("/jobs/:id", async (req, reply) => {
    const id = (req.params as any).id as string;
    const job = await audioQueue.getJob(id);
    if (!job) return reply.code(404).send({ error: "Job not found" });

    const state = await job.getState();
    return reply.send({
      jobId: String(job.id),
      state,
      progress: job.progress,
      result: job.returnvalue ?? null,
      failedReason: job.failedReason ?? null,
    });
  });

  // -----------------------------
  // Listen
  // -----------------------------
  await app.listen({ port: 3000, host: "127.0.0.1" });
  console.log("API on http://127.0.0.1:3000");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
