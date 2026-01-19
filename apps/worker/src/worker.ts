import "dotenv/config";
import IORedis from "ioredis";
import { Worker } from "bullmq";
import { HeadObjectCommand, GetObjectCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";

import { resolveOperation } from "./operations";
import { AppError, toAppError } from "./errors/appError";

type JobData = {
  type: string;
  provider?: string;

  assetId: string;
  inputVersion: number;
  requestId?: string;

  // params are merged on API into job.data
  [key: string]: any;
};

type InputMeta = {
  objectKey: string;
  [key: string]: any;
};

function mustEnv(name: string): string {
  const v = process.env[name]?.trim();
  if (!v) throw new Error(`${name} missing in apps/worker/.env`);
  return v;
}

function fileNameForType(type: string): string {
  if (type === "LOOP_APPLY") return "loop.wav";
  if (type === "DENOISE") return "denoise.wav";
  if (type === "CONVERT") return "source.wav"; // normalized
  return `${type.toLowerCase()}.wav`;
}

function analysisObjectKey(assetId: string, inputVersion: number) {
  return `assets/${assetId}/analysis/v${inputVersion}/peaks.json`;
}

async function getInputMeta(redis: IORedis, assetId: string, inputVersion: number): Promise<InputMeta> {
  const assetKey = `asset:${assetId}`;
  const inMeta = await redis.hgetall(`${assetKey}:v:${inputVersion}`);
  if (!inMeta?.objectKey) {
    throw new AppError({
      code: "NOT_FOUND",
      message: `Input version not found: asset=${assetId} v${inputVersion}`,
      retryable: false,
      details: { assetId, inputVersion },
    });
  }
  return inMeta as unknown as InputMeta;
}

async function pickNextVersion(redis: IORedis, assetId: string, fallback: number): Promise<number> {
  const assetKey = `asset:${assetId}`;
  const latestStr = await redis.get(`${assetKey}:latestVersion`);
  const latest = latestStr ? Number(latestStr) : fallback;
  return latest + 1;
}

async function createPendingVersionMeta(opts: {
  redis: IORedis;
  assetId: string;
  outVersion: number;
  type: string;
  provider: string;
  inputVersion: number;
  jobId: string;
  objectKey: string;
  requestId?: string;
}) {
  const { redis, assetId, outVersion, type, provider, inputVersion, jobId, objectKey, requestId } = opts;
  const assetKey = `asset:${assetId}`;

  await redis.hset(`${assetKey}:v:${outVersion}`, {
    version: String(outVersion),
    objectKey,
    kind: type.toLowerCase(),
    operationType: type,
    provider,
    status: "PENDING",
    createdAt: new Date().toISOString(),
    fromVersion: String(inputVersion),
    jobId,
    ...(requestId ? { requestId } : {}),
    storage: "r2",
  });

  // no duplicates
  const vStr = String(outVersion);
  const exists = await redis.lpos(`${assetKey}:versions`, vStr);
  if (exists === null) {
    await redis.rpush(`${assetKey}:versions`, vStr);
  }
}

async function markVersionReady(opts: {
  redis: IORedis;
  assetId: string;
  outVersion: number;
  objectKey: string;
  extraMeta?: Record<string, string>;
}) {
  const { redis, assetId, outVersion, objectKey, extraMeta } = opts;
  const assetKey = `asset:${assetId}`;

  await redis.hset(`${assetKey}:v:${outVersion}`, {
    status: "READY",
    objectKey,
    readyAt: new Date().toISOString(),
    ...(extraMeta ?? {}),
  });

  await redis.set(`${assetKey}:latestVersion`, String(outVersion));
}

async function markVersionFailed(opts: { redis: IORedis; assetId: string; outVersion: number; error: AppError }) {
  const { redis, assetId, outVersion, error } = opts;
  const assetKey = `asset:${assetId}`;

  await redis.hset(`${assetKey}:v:${outVersion}`, {
    status: "FAILED",
    failedAt: new Date().toISOString(),
    errorCode: error.code,
    errorMessage: error.message,
    errorRetryable: String(error.retryable),
  });
}

async function saveJobStatus(redis: IORedis, jobId: string, patch: Record<string, string>) {
  await redis.hset(`job:${jobId}`, patch);
}

function logJson(obj: Record<string, any>) {
  console.log(JSON.stringify(obj));
}

function withTimeout<T>(promise: Promise<T>, ms: number, onTimeout: () => void): Promise<T> {
  if (!ms || ms <= 0) return promise;

  return new Promise<T>((resolve, reject) => {
    const t = setTimeout(() => {
      try {
        onTimeout();
      } catch {}
      reject(
        new AppError({
          code: "TIMED_OUT",
          message: `Job timed out after ${ms}ms`,
          retryable: true,
        })
      );
    }, ms);

    promise.then(
      (v) => {
        clearTimeout(t);
        resolve(v);
      },
      (e) => {
        clearTimeout(t);
        reject(e);
      }
    );
  });
}

async function main() {
  const redisUrl = mustEnv("REDIS_URL");
  const connection = new IORedis(redisUrl, { maxRetriesPerRequest: null, tls: {} });

  const R2_ACCOUNT_ID = mustEnv("R2_ACCOUNT_ID");
  const R2_ACCESS_KEY_ID = mustEnv("R2_ACCESS_KEY_ID");
  const R2_SECRET_ACCESS_KEY = mustEnv("R2_SECRET_ACCESS_KEY");
  const R2_BUCKET = mustEnv("R2_BUCKET");

  const s3 = new S3Client({
    region: "auto",
    endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: { accessKeyId: R2_ACCESS_KEY_ID, secretAccessKey: R2_SECRET_ACCESS_KEY },
    forcePathStyle: true,
    requestChecksumCalculation: "WHEN_REQUIRED",
    responseChecksumValidation: "WHEN_REQUIRED",
  });

  const worker = new Worker<JobData>(
    "audio-jobs",
    async (job) => {
      const jobId = String(job.id);
      const { type, assetId, inputVersion, requestId } = job.data;

      let outVersion: number | null = null;
      let outObjectKey: string | null = null;

      // resolve module
      const resolved = resolveOperation(type, job.data.provider);
      if (!resolved) {
        const e = new AppError({
          code: "BAD_INPUT",
          message: `Unknown operation: type=${type} provider=${job.data.provider ?? ""}`,
          retryable: false,
          details: { type, provider: job.data.provider ?? null },
        });
        await saveJobStatus(connection, jobId, {
          status: "FAILED",
          finishedAt: new Date().toISOString(),
          errorCode: e.code,
          errorMessage: e.message,
          errorRetryable: String(e.retryable),
        });
        throw e;
      }

      const module = resolved.module;
      const providerUsed = resolved.provider;

      try {
        logJson({
          level: "info",
          msg: "Job start",
          requestId: requestId ?? null,
          jobId,
          assetId,
          jobType: type,
          provider: providerUsed,
          fromVersion: inputVersion,
        });

        await saveJobStatus(connection, jobId, {
          status: "RUNNING",
          startedAt: new Date().toISOString(),
          assetId,
          type,
          provider: providerUsed,
          inputVersion: String(inputVersion),
          ...(requestId ? { requestId } : {}),
          attemptsMade: String(job.attemptsMade ?? 0),
          attemptsMax: String(job.opts.attempts ?? 1),
        });

        // input meta + sanity
        const inMeta = await getInputMeta(connection, assetId, inputVersion);
        await s3.send(new HeadObjectCommand({ Bucket: R2_BUCKET, Key: inMeta.objectKey }));

        const timeoutMs = typeof job.opts.timeout === "number" ? job.opts.timeout : 0;
        const ac = new AbortController();

        // -------------------------------
        // SPECIAL: ANALYZE (no versioning)
        // -------------------------------
        if (type === "ANALYZE") {
          const peaksKey = analysisObjectKey(assetId, inputVersion);

          const result = await withTimeout(
            module.run({
              assetId,
              inputVersion,
              jobId,
              outputObjectKey: peaksKey,
              payload: job.data,
              signal: ac.signal,
              inputObjectKey: inMeta.objectKey as any,
            } as any),
            timeoutMs,
            () => ac.abort()
          );

          if (!result.ok) {
            throw new AppError({
              code: "PROCESSING_FAILED",
              message: `Module ANALYZE/${providerUsed} returned ok=false`,
              retryable: false,
            });
          }

          // ensure json exists
          await s3.send(new HeadObjectCommand({ Bucket: R2_BUCKET, Key: result.objectKey }));

          // cache in Redis (store the JSON string)
          try {
            const got = await s3.send(new GetObjectCommand({ Bucket: R2_BUCKET, Key: result.objectKey }));
            const body = got.Body as any;
            if (body) {
              const chunks: Buffer[] = [];
              for await (const c of body) chunks.push(Buffer.from(c));
              const jsonStr = Buffer.concat(chunks).toString("utf8");
              await connection.set(`asset:${assetId}:peaks:v${inputVersion}`, jsonStr);
            }
          } catch {}

          await saveJobStatus(connection, jobId, {
            status: "SUCCEEDED",
            finishedAt: new Date().toISOString(),
            objectKey: result.objectKey,
            provider: providerUsed,
            attemptsMade: String(job.attemptsMade ?? 0),
            attemptsMax: String(job.opts.attempts ?? 1),
          });

          await job.updateProgress(100);

          return {
            ok: true,
            processedAt: new Date().toISOString(),
            objectKey: result.objectKey,
            assetId,
            inputVersion,
            type,
            provider: providerUsed,
          };
        }

        // -------------------------------
        // NORMAL: versioning operations
        // -------------------------------
        outVersion = await pickNextVersion(connection, assetId, inputVersion);
        outObjectKey = `assets/${assetId}/v${outVersion}/${fileNameForType(type)}`;

        await createPendingVersionMeta({
          redis: connection,
          assetId,
          outVersion,
          type,
          provider: providerUsed,
          inputVersion,
          jobId,
          objectKey: outObjectKey,
          requestId,
        });

        await job.updateProgress(10);

        const result = await withTimeout(
          module.run({
            assetId,
            inputVersion,
            jobId,
            outputObjectKey: outObjectKey,
            payload: job.data,
            signal: ac.signal,
            inputObjectKey: inMeta.objectKey as any,
            outputVersion: outVersion as any,
          } as any),
          timeoutMs,
          () => ac.abort()
        );

        if (!result.ok) {
          throw new AppError({
            code: "PROCESSING_FAILED",
            message: `Module ${type}/${providerUsed} returned ok=false`,
            retryable: false,
            details: { type, provider: providerUsed },
          });
        }

        await s3.send(new HeadObjectCommand({ Bucket: R2_BUCKET, Key: result.objectKey }));

        await markVersionReady({
          redis: connection,
          assetId,
          outVersion,
          objectKey: result.objectKey,
          extraMeta: result.meta ?? undefined,
        });

        await saveJobStatus(connection, jobId, {
          status: "SUCCEEDED",
          finishedAt: new Date().toISOString(),
          createdVersion: String(outVersion),
          objectKey: result.objectKey,
          provider: providerUsed,
          attemptsMade: String(job.attemptsMade ?? 0),
          attemptsMax: String(job.opts.attempts ?? 1),
        });

        await job.updateProgress(100);

        logJson({
          level: "info",
          msg: "Job succeeded",
          requestId: requestId ?? null,
          jobId,
          assetId,
          jobType: type,
          provider: providerUsed,
          fromVersion: inputVersion,
          toVersion: outVersion,
          objectKey: result.objectKey,
        });

        return {
          ok: true,
          processedAt: new Date().toISOString(),
          createdVersion: outVersion,
          objectKey: result.objectKey,
          from: inMeta.objectKey,
          type,
          provider: providerUsed,
        };
      } catch (err) {
        const e = toAppError(err);

        if (!e.retryable) {
          try {
            await job.discard();
          } catch {}
        }

        logJson({
          level: "error",
          msg: "Job failed",
          requestId: (job.data as any)?.requestId ?? null,
          jobId,
          assetId,
          jobType: type,
          provider: providerUsed,
          fromVersion: inputVersion,
          toVersion: outVersion ?? null,
          error: e.toJSON(),
        });

        if (outVersion != null) {
          try {
            const maybe = await connection.hgetall(`asset:${assetId}:v:${outVersion}`);
            if (maybe?.status === "PENDING") {
              await markVersionFailed({ redis: connection, assetId, outVersion, error: e });
            }
          } catch {}
        }

        await saveJobStatus(connection, jobId, {
          status: e.code === "TIMED_OUT" ? "TIMED_OUT" : "FAILED",
          finishedAt: new Date().toISOString(),
          errorCode: e.code,
          errorMessage: e.message,
          errorRetryable: String(e.retryable),
          provider: providerUsed,
          attemptsMade: String(job.attemptsMade ?? 0),
          attemptsMax: String(job.opts.attempts ?? 1),
        });

        throw err;
      }
    },
    { connection }
  );

  worker.on("completed", (job) => logJson({ level: "info", msg: "Worker completed", jobId: String(job.id) }));
  worker.on("failed", (job, err) =>
    logJson({ level: "error", msg: "Worker failed", jobId: String(job?.id), error: toAppError(err).toJSON() })
  );

  logJson({ level: "info", msg: "Worker running", queue: "audio-jobs" });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
