import "dotenv/config";
import IORedis from "ioredis";
import { Worker } from "bullmq";
import { S3Client, CopyObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3";

type JobData =
  | {
      type: "LOOP" | "DENOISE" | "SEPARATE";
      assetId: string;
      inputVersion: number;
    }
  | {
      type: "LOOP_APPLY";
      assetId: string;
      inputVersion: number;
      loop: { startMs: number; endMs: number; crossfadeMs: number };
    };

async function main() {
  // ---------- Redis ----------
  const redisUrl = process.env.REDIS_URL?.trim();
  if (!redisUrl) throw new Error("REDIS_URL missing in apps/worker/.env");

  const connection = new IORedis(redisUrl, {
    maxRetriesPerRequest: null,
    tls: {},
  });

  // ---------- R2 ----------
  const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID?.trim();
  const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID?.trim();
  const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY?.trim();
  const R2_BUCKET = process.env.R2_BUCKET?.trim();

  if (!R2_ACCOUNT_ID || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY || !R2_BUCKET) {
    throw new Error("R2 env missing in apps/worker/.env (R2_*)");
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

  const worker = new Worker<JobData>(
    "audio-jobs",
    async (job) => {
      console.log("Job start:", job.id, job.data);

      const assetKey = `asset:${job.data.assetId}`;

      // input version meta
      const inMeta = await connection.hgetall(`${assetKey}:v:${job.data.inputVersion}`);
      if (!inMeta?.objectKey) throw new Error(`Input version not found: v${job.data.inputVersion}`);

      // output version
      const latestStr = await connection.get(`${assetKey}:latestVersion`);
      const latest = latestStr ? Number(latestStr) : job.data.inputVersion;
      const outVersion = Math.max(latest, job.data.inputVersion) + 1;

      const kind = job.data.type.toLowerCase();
      const outObjectKey = `assets/${job.data.assetId}/v${outVersion}/${kind}.wav`;

      await job.updateProgress(10);

      // sanity: input exists in R2
      await s3.send(new HeadObjectCommand({ Bucket: R2_BUCKET, Key: inMeta.objectKey }));

      // A) Реальный файл: пока просто COPY input -> output
      await s3.send(
        new CopyObjectCommand({
          Bucket: R2_BUCKET,
          Key: outObjectKey,
          CopySource: `/${R2_BUCKET}/${inMeta.objectKey}`,
          ContentType: "audio/wav",
          MetadataDirective: "REPLACE",
        })
      );

      await job.updateProgress(80);

      // meta for new version
      const meta: Record<string, string> = {
        version: String(outVersion),
        objectKey: outObjectKey,
        kind,
        createdAt: new Date().toISOString(),
        fromVersion: String(job.data.inputVersion),
        jobId: String(job.id),
        storage: "r2",
      };

      // Если это LOOP_APPLY — прикрепим метаданные региона
      if (job.data.type === "LOOP_APPLY") {
        meta.loopStartMs = String(job.data.loop.startMs);
        meta.loopEndMs = String(job.data.loop.endMs);
        meta.loopCrossfadeMs = String(job.data.loop.crossfadeMs);
      }

      await connection.hset(`${assetKey}:v:${outVersion}`, meta);
      await connection.rpush(`${assetKey}:versions`, String(outVersion));
      await connection.set(`${assetKey}:latestVersion`, String(outVersion));

      await job.updateProgress(100);

      return {
        ok: true,
        processedAt: new Date().toISOString(),
        createdVersion: outVersion,
        objectKey: outObjectKey,
        from: inMeta.objectKey,
        type: job.data.type,
      };
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
