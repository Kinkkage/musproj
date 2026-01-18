import "dotenv/config";
import IORedis from "ioredis";
import { Worker } from "bullmq";
import { HeadObjectCommand, S3Client } from "@aws-sdk/client-s3";

import { modules } from "./modules";
import { ProcessingModule } from "./types/processing";
import { AppError, toAppError } from "./errors/appError";

type JobData = {
  type: string;
  assetId: string;
  inputVersion: number;
  requestId?: string;
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
  return `${type.toLowerCase()}.wav`;
}

async function getInputMeta(
  redis: IORedis,
  assetId: string,
  inputVersion: number
): Promise<InputMeta> {
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
  inputVersion: number;
  jobId: string;
  objectKey: string;
  requestId?: string;
}) {
  const { redis, assetId, outVersion, type, inputVersion, jobId, objectKey, requestId } = opts;
  const assetKey = `asset:${assetId}`;

  await redis.hset(`${assetKey}:v:${outVersion}`, {
    version: String(outVersion),
    objectKey,
    kind: type.toLowerCase(),
    status: "PENDING",
    createdAt: new Date().toISOString(),
    fromVersion: String(inputVersion),
    jobId,
    ...(requestId ? { requestId } : {}),
    storage: "r2",
  });

  // ✅ не добавляем дубли в список versions
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

async function markVersionFailed(opts: {
  redis: IORedis;
  assetId: string;
  outVersion: number;
  error: AppError;
}) {
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

// Реальный таймаут на уровне приложения (не зависим от BullMQ)
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
  // ---------- Redis ----------
  const redisUrl = mustEnv("REDIS_URL");
  const connection = new IORedis(redisUrl, {
    maxRetriesPerRequest: null,
    tls: {},
  });

  // ---------- R2 ----------
  const R2_ACCOUNT_ID = mustEnv("R2_ACCOUNT_ID");
  const R2_ACCESS_KEY_ID = mustEnv("R2_ACCESS_KEY_ID");
  const R2_SECRET_ACCESS_KEY = mustEnv("R2_SECRET_ACCESS_KEY");
  const R2_BUCKET = mustEnv("R2_BUCKET");

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

  const worker = new Worker<JobData>(
    "audio-jobs",
    async (job) => {
      const jobId = String(job.id);
      const { type, assetId, inputVersion, requestId } = job.data;

      let outVersion: number | null = null;
      let outObjectKey: string | null = null;

      try {
        logJson({
          level: "info",
          msg: "Job start",
          requestId: requestId ?? null,
          jobId,
          assetId,
          jobType: type,
          fromVersion: inputVersion,
        });

        await saveJobStatus(connection, jobId, {
          status: "RUNNING",
          startedAt: new Date().toISOString(),
          assetId,
          type,
          inputVersion: String(inputVersion),
          ...(requestId ? { requestId } : {}),
          attemptsMade: String(job.attemptsMade ?? 0),
          attemptsMax: String(job.opts.attempts ?? 1),
        });

        // ---------- input meta ----------
        const inMeta = await getInputMeta(connection, assetId, inputVersion);

        // sanity: input exists in R2
        await s3.send(
          new HeadObjectCommand({
            Bucket: R2_BUCKET,
            Key: inMeta.objectKey,
          })
        );

        // ---------- select module ----------
        const module: ProcessingModule | undefined = modules[type];
        if (!module) {
          throw new AppError({
            code: "BAD_INPUT",
            message: `Unknown processing module: ${type}`,
            retryable: false,
            details: { type },
          });
        }

        // ---------- prepare output version + key ----------
        outVersion = await pickNextVersion(connection, assetId, inputVersion);
        outObjectKey = `assets/${assetId}/v${outVersion}/${fileNameForType(type)}`;

        await createPendingVersionMeta({
          redis: connection,
          assetId,
          outVersion,
          type,
          inputVersion,
          jobId,
          objectKey: outObjectKey,
          requestId,
        });

        await job.updateProgress(10);

        // ---------- run module with REAL timeout + abort ----------
        const timeoutMs = typeof job.opts.timeout === "number" ? job.opts.timeout : 0;
        const ac = new AbortController();

        const result = await withTimeout(
          module.run({
            assetId,
            inputVersion,
            jobId,
            outputObjectKey: outObjectKey,
            payload: job.data,
            signal: ac.signal, // <- модуль может убить ffmpeg
            // подсказка модулю (откуда читать) — loopApply ожидает inputObjectKey
            inputObjectKey: inMeta.objectKey as any,
            outputVersion: outVersion as any,
          } as any),
          timeoutMs,
          () => ac.abort()
        );

        if (!result.ok) {
          throw new AppError({
            code: "PROCESSING_FAILED",
            message: `Module ${type} returned ok=false`,
            retryable: false,
            details: { type },
          });
        }

        // ВАЖНО: файл должен реально существовать
        await s3.send(
          new HeadObjectCommand({
            Bucket: R2_BUCKET,
            Key: result.objectKey,
          })
        );

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
        };
      } catch (err) {
        const e = toAppError(err);

        // запретить ретраи для неретраимых ошибок
        if (!e.retryable) {
          try {
            await job.discard();
          } catch {}
        }

        logJson({
          level: "error",
          msg: "Job failed",
          requestId: requestId ?? null,
          jobId,
          assetId,
          jobType: type,
          fromVersion: inputVersion,
          toVersion: outVersion ?? null,
          error: e.toJSON(),
        });

        // если успели создать версию — пометим FAILED
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
          attemptsMade: String(job.attemptsMade ?? 0),
          attemptsMax: String(job.opts.attempts ?? 1),
        });

        throw err;
      }
    },
    { connection }
  );

  worker.on("completed", (job) =>
    logJson({ level: "info", msg: "Worker completed", jobId: String(job.id) })
  );
  worker.on("failed", (job, err) =>
    logJson({
      level: "error",
      msg: "Worker failed",
      jobId: String(job?.id),
      error: toAppError(err).toJSON(),
    })
  );

  logJson({ level: "info", msg: "Worker running", queue: "audio-jobs" });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
