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

function mustEnv(name: string): string {
  const v = process.env[name]?.trim();
  if (!v) throw new Error(`${name} missing in apps/worker/.env`);
  return v;
}

async function getInputMeta(redis: IORedis, assetId: string, inputVersion: number): Promise<InputMeta> {
  const assetKey = `asset:${assetId}`;
  const inMeta = await redis.hgetall(`${assetKey}:v:${inputVersion}`);
  if (!inMeta?.objectKey) throw new Error(`Input version not found: asset=${assetId} v${inputVersion}`);
  return inMeta as unknown as InputMeta;
}

async function main() {
  // ---------- Redis ----------
  const redisUrl = mustEnv("REDIS_URL");
  const connection = new IORedis(redisUrl, { maxRetriesPerRequest: null, tls: {} });

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
      try {
        console.log("Job start:", { jobId: job.id, type: job.data.type, assetId: job.data.assetId });

        const { type, assetId, inputVersion } = job.data;
        const assetKey = `asset:${assetId}`;

        // ---------- module ----------
        const module: ProcessingModule | undefined = modules[type as keyof typeof modules];
        if (!module) throw new Error(`Unknown processing module: ${type}`);

        // ---------- input meta + sanity ----------
        const inMeta = await getInputMeta(connection, assetId, inputVersion);
        await s3.send(new HeadObjectCommand({ Bucket: R2_BUCKET, Key: inMeta.objectKey }));

        // ---------- deterministic output path (no duplicates) ----------
        const outObjectKey = `assets/${assetId}/jobs/${job.id}/output.wav`;

        // ---------- allocate next version number via counter ----------
        // IMPORTANT: versionCounter должен существовать (API поставит "1" при upload/request)
        const outVersion = Number(await connection.incr(`${assetKey}:versionCounter`));

        // ---------- create version meta as PENDING (do NOT set latest yet) ----------
        await connection.hset(`${assetKey}:v:${outVersion}`, {
          version: String(outVersion),
          objectKey: outObjectKey,
          kind: type.toLowerCase(),
          status: "PENDING",
          createdAt: new Date().toISOString(),
          fromVersion: String(inputVersion),
          jobId: String(job.id),
          storage: "r2",
        });
        await connection.rpush(`${assetKey}:versions`, String(outVersion));

        await job.updateProgress(10);

        // ---------- run module ----------
        const result = await module.run({
          assetId,
          inputVersion,
          jobId: String(job.id),
          outputObjectKey: outObjectKey,
          payload: {
            ...job.data,
            inObjectKey: inMeta.objectKey, // модулю нужен входной ключ
          },
        });

        if (!result.ok) throw new Error(`Module ${type} failed`);

        await job.updateProgress(80);

        // ---------- mark READY + set latest only now ----------
        await connection.hset(`${assetKey}:v:${outVersion}`, {
          status: "READY",
          objectKey: result.objectKey,
          ...(result.meta ?? {}),
          readyAt: new Date().toISOString(),
        });

        await connection.set(`${assetKey}:latestVersion`, String(outVersion));
        await connection.set(`asset:${assetId}:versionCounter`, "1");
        
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
        console.error("PROCESSING ERROR", {
          jobId: job.id,
          type: (job.data as any)?.type,
          assetId: (job.data as any)?.assetId,
          error: err,
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
