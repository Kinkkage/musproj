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

function ffprobeDuration(ffprobePath: string, filePath: string): Promise<number | null> {
  const args = [
    "-v",
    "error",
    "-show_entries",
    "format=duration",
    "-of",
    "default=noprint_wrappers=1:nokey=1",
    filePath,
  ];

  return new Promise((resolve, reject) => {
    const child = spawn(ffprobePath, args, { stdio: ["ignore", "pipe", "pipe"], windowsHide: true });
    let out = "";
    let err = "";
    child.stdout.on("data", (d) => (out += d.toString()));
    child.stderr.on("data", (d) => (err += d.toString()));
    child.on("error", reject);
    child.on("close", (code) => {
      if (code !== 0) return reject(new Error(`ffprobe failed code=${code}\n${err}`));
      const v = Number(String(out).trim());
      resolve(Number.isFinite(v) ? v : null);
    });
  });
}

type PeakPoint = { min: number; max: number }; // normalized -1..1

export const analyzeModule: ProcessingModule = {
  type: "ANALYZE",

  async run(input: ProcessingInput) {
    const R2_ACCOUNT_ID = mustEnv("R2_ACCOUNT_ID");
    const R2_ACCESS_KEY_ID = mustEnv("R2_ACCESS_KEY_ID");
    const R2_SECRET_ACCESS_KEY = mustEnv("R2_SECRET_ACCESS_KEY");
    const R2_BUCKET = mustEnv("R2_BUCKET");

    const ffmpegPath = envOrDefault("FFMPEG_PATH", "ffmpeg");
    const ffprobePath = envOrDefault("FFPROBE_PATH", "ffprobe");

    const points = Math.max(200, Math.min(5000, envNum("PEAKS_POINTS", 1000)));
    const decodeSr = envNum("PEAKS_DECODE_SR", 44100);

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
      throw new AppError({ code: "BAD_INPUT", message: "ANALYZE bad input: inputObjectKey missing", retryable: false });
    }
    if (!outputObjectKey) {
      throw new AppError({ code: "BAD_INPUT", message: "ANALYZE bad input: outputObjectKey missing", retryable: false });
    }

    const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "musproj-analyze-"));
    const inPath = path.join(tmpDir, "input.audio");
    const outJsonPath = path.join(tmpDir, "peaks.json");

    try {
      // 1) download
      const getResp = await s3.send(new GetObjectCommand({ Bucket: R2_BUCKET, Key: inputObjectKey }));
      if (!getResp.Body) {
        throw new AppError({
          code: "BAD_INPUT",
          message: "ANALYZE: R2 GetObject returned empty body",
          retryable: false,
          details: { inputObjectKey },
        });
      }
      await pipeline(getResp.Body as any, fs.createWriteStream(inPath));

      // 2) duration
      const lengthSec = (await ffprobeDuration(ffprobePath, inPath)) ?? 0;

      // 3) decode to s16le mono @ decodeSr and compute peaks in-stream
      const args = [
        "-hide_banner",
        "-i",
        inPath,
        "-vn",
        "-ac",
        "1",
        "-ar",
        String(decodeSr),
        "-f",
        "s16le",
        "pipe:1",
      ];

      const child = spawn(ffmpegPath, args, {
        stdio: ["ignore", "pipe", "pipe"],
        windowsHide: true,
        signal: input.signal,
      });

      let stderr = "";
      child.stderr.on("data", (d) => (stderr += d.toString()));

      // If duration unknown, still generate peaks based on observed samples.
      const expectedSamples = lengthSec > 0 ? Math.max(1, Math.floor(lengthSec * decodeSr)) : 0;
      const windowSize = expectedSamples > 0 ? Math.max(1, Math.floor(expectedSamples / points)) : 0;

      const peaks: PeakPoint[] = [];
      let sampleIndex = 0;

      let curMin = 1.0;
      let curMax = -1.0;
      let curCount = 0;

      function pushWindow() {
        if (curCount <= 0) {
          peaks.push({ min: 0, max: 0 });
        } else {
          peaks.push({ min: curMin, max: curMax });
        }
        curMin = 1.0;
        curMax = -1.0;
        curCount = 0;
      }

      await new Promise<void>((resolve, reject) => {
        const onData = (buf: Buffer) => {
          // iterate int16 samples (little endian)
          for (let i = 0; i + 1 < buf.length; i += 2) {
            const s = buf.readInt16LE(i) / 32768; // -1..1 approx
            if (s < curMin) curMin = s;
            if (s > curMax) curMax = s;
            curCount++;
            sampleIndex++;

            if (windowSize > 0) {
              if (curCount >= windowSize) pushWindow();
            } else {
              // unknown duration: just cap points by chunking observed samples equally later
              // here: approximate by pushing every N samples based on growth
              const dynamicWin = Math.max(1, Math.floor((sampleIndex / points) || 1));
              if (curCount >= dynamicWin) pushWindow();
            }

            if (peaks.length >= points) {
              // stop early once we have enough points
              child.stdout.off("data", onData);
              try { child.kill("SIGKILL"); } catch {}
              break;
            }
          }
        };

        child.stdout.on("data", onData);
        child.on("error", reject);
        child.on("close", (code) => {
          if (code === 0 || code === null) {
            // flush last window
            if (peaks.length < points) pushWindow();
            // trim to points
            while (peaks.length > points) peaks.pop();
            resolve();
          } else {
            reject(new Error(`ffmpeg analyze failed code=${code}\n${stderr}`));
          }
        });
      });

      const payload = {
        assetId: input.assetId,
        version: input.inputVersion,
        lengthSec,
        points: peaks.length,
        peaks,
        generatedAt: new Date().toISOString(),
      };

      await fsp.writeFile(outJsonPath, JSON.stringify(payload));

      // 4) upload json to R2
      await s3.send(
        new PutObjectCommand({
          Bucket: R2_BUCKET,
          Key: outputObjectKey,
          Body: fs.createReadStream(outJsonPath),
          ContentType: "application/json",
        })
      );

      return {
        ok: true,
        objectKey: outputObjectKey,
        meta: {
          operationType: "ANALYZE",
          provider: "ffmpeg",
          lengthSec: String(lengthSec),
          points: String(peaks.length),
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
