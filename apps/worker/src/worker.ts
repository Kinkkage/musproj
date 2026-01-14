import "dotenv/config";
import IORedis from "ioredis";
import { Worker } from "bullmq";
import { HeadObjectCommand, S3Client } from "@aws-sdk/client-s3";

import { modules } from "./modules";
import { ProcessingModule } from "./types/processing";

type JobData = {
  type: string;
  assetId: string;
  inputVersion: number;
  [key: string]: any;
};

type InputMeta = {
  objectKey: string;
  [key: string]: any;
};

type JobError = {
  code: string;
  message: string;
  retryable: boolean;
  details?: any;
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

function classifyError(err: any): JobError {
  const msg = String(err?.message ?? err);

  // простая классификация (потом улучшим)
  const retryable =
    err?.code === "ECONNRESET" ||
    err?.code === "ETIMEDOUT" ||
    err?.code === "ENOTFOUND" ||
    msg.includes("timeout") ||
    msg.includes("Too Many Requests") ||
    msg.includes("429");

  // “плохой ввод” / “нет данных” обычно не ретраим
  if (msg.includes("Input version not found") || msg.includes("Unknown processing module")) {
    return { code: "BAD_INPUT", message: msg, retryable: false };
  }

  return {
    code: retryable ? "RETRYABLE" : "PROCESSING_FAILED",
    message: msg,
    retryable,
    details: err?.stack ? { stack: err.stack } : undefined,
  };
}

async function getInputMeta(
  redis: IORedis,
  assetId: string,
  inputVersion: number
): Promise<InputMeta> {
  const assetKey = `asset:${assetId}`;
  const inMeta = await redis.hgetall(`${assetKey}:v:${inputVersion}`);
  if (!inMeta?.objectKey) {
    throw new Error(`Input version not found: asset=${assetId} v${inputVersion}`);
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
  objectKey: string; // planned object key
}) {
  const { redis, assetId, outVersion, type, inputVersion, jobId, objectKey } = opts;
  const assetKey = `asset:${assetId}`;

  await redis.hset(`${assetKey}:v:${outVersion}`, {
    version: String(outVersion),
    objectKey,
    kind: type.toLowerCase(),
    status: "PENDING",
    createdAt: new Date().toISOString(),
    fromVersion: String(inputVersion),
    jobId,
    storage: "r2",
  });

  // добавляем в список версий сразу (так проще дебажить)
  await redis.rpush(`${assetKey}:versions`, String(outVersion));
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

  // ВАЖНО: latestVersion только при READY
  await redis.set(`${assetKey}:latestVersion`, String(outVersion));
}

async function markVersionFailed(opts: {
  redis: IORedis;
  assetId: string;
  outVersion: number;
  error: JobError;
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

      try {
        console.log("Job start:", jobId, job.data);

        const { type, assetId, inputVersion } = job.data;
        const assetKey = `asset:${assetId}`;

        await saveJobStatus(connection, jobId, {
          status: "RUNNING",
          startedAt: new Date().toISOString(),
          assetId,
          type,
          inputVersion: String(inputVersion),
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
          throw new Error(`Unknown processing module: ${type}`);
        }

        // ---------- prepare output version + key ----------
        const outVersion = await pickNextVersion(connection, assetId, inputVersion);
        const outObjectKey = `assets/${assetId}/v${outVersion}/${fileNameForType(type)}`;

        // создаём версию сразу (PENDING)
        await createPendingVersionMeta({
          redis: connection,
          assetId,
          outVersion,
          type,
          inputVersion,
          jobId,
          objectKey: outObjectKey,
        });

        await job.updateProgress(10);

        // ---------- run module ----------
        const result = await module.run({
          assetId,
          inputVersion,
          jobId,

          // подсказки модулю (куда писать)
          outputVersion: outVersion,
          outputObjectKey: outObjectKey,

          // подсказка (откуда читать)
          inputObjectKey: inMeta.objectKey,

          payload: job.data,
        } as any);

        if (!result.ok) {
          throw new Error(`Module ${type} failed`);
        }

        // ВАЖНО: файл должен реально существовать
        await s3.send(
          new HeadObjectCommand({
            Bucket: R2_BUCKET,
            Key: result.objectKey,
          })
        );

        // ---------- READY + latest ----------
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
        });

        await job.updateProgress(100);

        return {
          ok: true,
          processedAt: new Date().toISOString(),
          createdVersion: outVersion,
          objectKey: result.objectKey,
          from: inMeta.objectKey,
          type,
        };
      } catch (err) {
        const e = classifyError(err);
        console.error("PROCESSING ERROR", {
          jobId,
          type: (job.data as any)?.type,
          assetId: (job.data as any)?.assetId,
          inputVersion: (job.data as any)?.inputVersion,
          error: e,
        });

        // если успели создать outVersion — попробуем пометить FAILED
        const assetId = (job.data as any)?.assetId as string | undefined;
        const inputVersion = (job.data as any)?.inputVersion as number | undefined;

        // мы не всегда знаем outVersion, но чаще знаем (по тому же алгоритму)
        if (assetId && typeof inputVersion === "number") {
          try {
            const outVersion = await pickNextVersion(connection, assetId, inputVersion);
            // ВНИМАНИЕ: pickNextVersion вернёт next+1, а нам нужен тот, который создавали.
            // Поэтому помечаем FAILED только если эта версия уже существует и PENDING.
            const maybe = await connection.hgetall(`asset:${assetId}:v:${outVersion}`);
            if (maybe?.status === "PENDING") {
              await markVersionFailed({ redis: connection, assetId, outVersion, error: e });
            }
          } catch {}
        }

        await saveJobStatus(connection, jobId, {
          status: "FAILED",
          finishedAt: new Date().toISOString(),
          errorCode: e.code,
          errorMessage: e.message,
          errorRetryable: String(e.retryable),
        });

        throw err;
      }
    },
    { connection }
  );

  worker.on("completed", (job) => console.log("Job completed:", job.id));
  worker.on("failed", (job, err) => console.error("Job failed:", job?.id, err));

  console.log("Worker is running and listening queue: audio-jobs");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
