import { CopyObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { ProcessingModule } from "../../types/processing";

function mustEnv(name: string): string {
  const v = process.env[name]?.trim();
  if (!v) throw new Error(`${name} missing in apps/worker/.env`);
  return v;
}

export const loopApplyModule: ProcessingModule = {
  type: "LOOP_APPLY",

  async run(input: any) {
    // input приходит из worker.ts, мы ждём поля:
    // input.inputObjectKey, input.outputObjectKey, input.payload.loop

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

    const inputObjectKey = input.inputObjectKey as string | undefined;
    const outputObjectKey = input.outputObjectKey as string | undefined;

    if (!inputObjectKey) {
      return {
        ok: false,
        objectKey: "",
        meta: { error: "inputObjectKey missing" },
      };
    }

    if (!outputObjectKey) {
      return {
        ok: false,
        objectKey: "",
        meta: { error: "outputObjectKey missing" },
      };
    }

    // Сейчас делаем заглушку обработки:
    // COPY input -> output (как будто "применили луп")
    await s3.send(
      new CopyObjectCommand({
        Bucket: R2_BUCKET,
        Key: outputObjectKey,
        CopySource: `/${R2_BUCKET}/${inputObjectKey}`,
        ContentType: "audio/wav",
        MetadataDirective: "REPLACE",
      })
    );

    // В meta фиксируем параметры лупа
    const loop = input.payload?.loop ?? {};
    return {
      ok: true,
      objectKey: outputObjectKey,
      meta: {
        loopStartMs: String(loop.startMs ?? ""),
        loopEndMs: String(loop.endMs ?? ""),
        loopCrossfadeMs: String(loop.crossfadeMs ?? ""),
        note: "LOOP_APPLY: copy stub (DSP later)",
      },
    };
  },
};
