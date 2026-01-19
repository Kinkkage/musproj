import "dotenv/config";
import path from "node:path";
import os from "node:os";
import fs from "node:fs";
import fsp from "node:fs/promises";
import { pipeline } from "node:stream/promises";
import { spawn } from "node:child_process";

import { GetObjectCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { ProcessingModule, ProcessingInput } from "../../types/processing";
import { AppError } from "../../errors/appError";

function mustEnv(name: string): string {
  const v = process.env[name]?.trim();
  if (!v) throw new Error(`${name} missing in apps/worker/.env`);
  return v;
}

function envOrDefault(name: string, fallback: string) {
  return process.env[name]?.trim() || fallback;
}

function envNum(name: string, fallback: number) {
  const v = process.env[name]?.trim();
  if (!v) return fallback;
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function runCmd(cmd: string, args: string[], opts?: { cwd?: string; signal?: AbortSignal }) {
  return new Promise<void>((resolve, reject) => {
    const child = spawn(cmd, args, {
      cwd: opts?.cwd,
      stdio: ["ignore", "pipe", "pipe"],
      signal: opts?.signal,
      windowsHide: true,
    });

    let stderr = "";
    let stdout = "";
    child.stdout.on("data", (d) => (stdout += d.toString()));
    child.stderr.on("data", (d) => (stderr += d.toString()));
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) return resolve();
      reject(new Error(`${cmd} failed code=${code}\n${stdout}\n${stderr}`));
    });
  });
}

export const convertModule: ProcessingModule = {
  type: "CONVERT",

  async run(input: ProcessingInput) {
    const R2_ACCOUNT_ID = mustEnv("R2_ACCOUNT_ID");
    const R2_ACCESS_KEY_ID = mustEnv("R2_ACCESS_KEY_ID");
    const R2_SECRET_ACCESS_KEY = mustEnv("R2_SECRET_ACCESS_KEY");
    const R2_BUCKET = mustEnv("R2_BUCKET");

    const ffmpegPath = envOrDefault("FFMPEG_PATH", "ffmpeg");

    // fixed output format
    const targetSr = envNum("IMPORT_TARGET_SR", 48000); // 44100 or 48000
    const targetCh = envNum("IMPORT_TARGET_CH", 2); // 1 mono or 2 stereo

    const s3 = new S3Client({
      region: "auto",
      endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
      credentials: { accessKeyId: R2_ACCESS_KEY_ID, secretAccessKey: R2_SECRET_ACCESS_KEY },
      forcePathStyle: true,
      requestChecksumCalculation: "WHEN_REQUIRED",
      responseChecksumValidation: "WHEN_REQUIRED",
    });

    const inputObjectKey = (input as any).inputObjectKey as string | undefined;
    const outputObjectKey = input.outputObjectKey;

    if (!inputObjectKey) {
      throw new AppError({
        code: "BAD_INPUT",
        message: "CONVERT bad input: inputObjectKey missing",
        retryable: false,
      });
    }
    if (!outputObjectKey) {
      throw new AppError({
        code: "BAD_INPUT",
        message: "CONVERT bad input: outputObjectKey missing",
        retryable: false,
      });
    }

    const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "musproj-convert-"));
    const inPath = path.join(tmpDir, "input.bin");
    const outPath = path.join(tmpDir, "output.wav");

    try {
      // 1) download
      const getResp = await s3.send(new GetObjectCommand({ Bucket: R2_BUCKET, Key: inputObjectKey }));
      if (!getResp.Body) {
        throw new AppError({
          code: "BAD_INPUT",
          message: "CONVERT: R2 GetObject returned empty body",
          retryable: false,
          details: { inputObjectKey },
        });
      }
      await pipeline(getResp.Body as any, fs.createWriteStream(inPath));

      // 2) ffmpeg normalize/convert to PCM WAV
      // -ar fixed SR, -ac fixed channels, PCM 16-bit
      const args = [
        "-hide_banner",
        "-y",
        "-i",
        inPath,
        "-vn",
        "-ar",
        String(targetSr),
        "-ac",
        String(targetCh),
        "-c:a",
        "pcm_s16le",
        outPath,
      ];

      await runCmd(ffmpegPath, args, { signal: input.signal });

      // 3) upload WAV to R2
      await s3.send(
        new PutObjectCommand({
          Bucket: R2_BUCKET,
          Key: outputObjectKey,
          Body: fs.createReadStream(outPath),
          ContentType: "audio/wav",
        })
      );

      return {
        ok: true,
        objectKey: outputObjectKey,
        meta: {
          operationType: "CONVERT",
          provider: "ffmpeg",
          targetSr: String(targetSr),
          targetCh: String(targetCh),
          fromVersion: String(input.inputVersion),
        },
      };
    } finally {
      try {
        await fsp.rm(tmpDir, { recursive: true, force: true });
      } catch {}
    }
  },
};
