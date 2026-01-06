import { CopyObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { ProcessingModule } from "../../types/processing";

const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID?.trim();
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID?.trim();
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY?.trim();
const R2_BUCKET = process.env.R2_BUCKET?.trim();

if (!R2_ACCOUNT_ID || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY || !R2_BUCKET) {
  throw new Error("R2 env missing (auphonic.module.ts)");
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

export const auphonicModule: ProcessingModule = {
  type: "AUPHONIC",

  async run(input: any) {
    // Пока заглушка: COPY input -> output
    const inputKey = input.inputObjectKey as string;
    const outKey = input.outputObjectKey as string;

    await s3.send(
      new CopyObjectCommand({
        Bucket: R2_BUCKET,
        Key: outKey,
        CopySource: `/${R2_BUCKET}/${inputKey}`,
        ContentType: "audio/wav",
        MetadataDirective: "REPLACE",
      })
    );

    return {
      ok: true,
      objectKey: outKey,
      meta: { note: "AUPHONIC stub copy (real API later)" },
    };
  },
};
