import { CopyObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { ProcessingModule } from "../../types/processing";

function mustEnv(name: string): string {
  const v = process.env[name]?.trim();
  if (!v) throw new Error(`${name} missing in apps/worker/.env`);
  return v;
}

const s3 = new S3Client({
  region: "auto",
  endpoint: `https://${mustEnv("R2_ACCOUNT_ID")}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: mustEnv("R2_ACCESS_KEY_ID"),
    secretAccessKey: mustEnv("R2_SECRET_ACCESS_KEY"),
  },
  forcePathStyle: true,
  requestChecksumCalculation: "WHEN_REQUIRED",
  responseChecksumValidation: "WHEN_REQUIRED",
});

const BUCKET = mustEnv("R2_BUCKET");

export const loopApplyModule: ProcessingModule = {
  type: "LOOP_APPLY",

  async run(input) {
    const inKey = String(input.payload?.inObjectKey ?? "");
    if (!inKey) {
      // worker всегда может прокинуть inObjectKey; если нет — модуль не знает откуда копировать
      return { ok: false, objectKey: input.outputObjectKey, meta: { error: "inObjectKey missing" } };
    }

    // stub: просто копируем input -> output
    await s3.send(
      new CopyObjectCommand({
        Bucket: BUCKET,
        Key: input.outputObjectKey,
        CopySource: `/${BUCKET}/${inKey}`,
        ContentType: "audio/wav",
        MetadataDirective: "REPLACE",
      })
    );

    return {
      ok: true,
      objectKey: input.outputObjectKey,
      meta: {
        loopStartMs: String(input.payload?.loop?.startMs ?? ""),
        loopEndMs: String(input.payload?.loop?.endMs ?? ""),
        loopCrossfadeMs: String(input.payload?.loop?.crossfadeMs ?? ""),
      },
    };
  },
};
