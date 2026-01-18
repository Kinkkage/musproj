import "dotenv/config";
import path from "node:path";
import os from "node:os";
import fs from "node:fs";
import fsp from "node:fs/promises";
import { pipeline } from "node:stream/promises";
import { spawn } from "node:child_process";

import { GetObjectCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { ProcessingModule, ProcessingInput } from "../../types/processing";

function mustEnv(name: string): string {
  const v = process.env[name]?.trim();
  if (!v) throw new Error(`${name} missing in apps/worker/.env`);
  return v;
}

function envOrDefault(name: string, fallback: string) {
  return process.env[name]?.trim() || fallback;
}

function runCmd(
  cmd: string,
  args: string[],
  opts?: { cwd?: string; signal?: AbortSignal }
) {
  return new Promise<void>((resolve, reject) => {
    const child = spawn(cmd, args, {
      cwd: opts?.cwd,
      stdio: ["ignore", "pipe", "pipe"],
      signal: opts?.signal, // <- важно: AbortSignal убьёт процесс
      windowsHide: true,
    });

    let stderr = "";
    let stdout = "";

    child.stdout.on("data", (d) => (stdout += d.toString()));
    child.stderr.on("data", (d) => (stderr += d.toString()));

    child.on("error", (err) => reject(err));
    child.on("close", (code) => {
      if (code === 0) return resolve();
      reject(
        new Error(
          `${cmd} failed with code=${code}\n` +
            (stdout ? `stdout:\n${stdout}\n` : "") +
            (stderr ? `stderr:\n${stderr}\n` : "")
        )
      );
    });
  });
}

async function ffprobeDuration(ffprobePath: string, filePath: string): Promise<number | null> {
  const args = [
    "-v",
    "error",
    "-show_entries",
    "format=duration",
    "-of",
    "default=noprint_wrappers=1:nokey=1",
    filePath,
  ];

  let out = "";
  await new Promise<void>((resolve, reject) => {
    const child = spawn(ffprobePath, args, {
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true,
    });
    child.stdout.on("data", (d) => (out += d.toString()));
    let err = "";
    child.stderr.on("data", (d) => (err += d.toString()));
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) return resolve();
      reject(new Error(`ffprobe failed code=${code}\n${err}`));
    });
  });

  const v = Number(String(out).trim());
  return Number.isFinite(v) ? v : null;
}

function clampCrossfadeSec(crossfadeSec: number, durationSec: number) {
  if (crossfadeSec <= 0) return 0;
  if (durationSec <= 0) return 0;
  return Math.min(crossfadeSec, durationSec / 2);
}

export const loopApplyModule: ProcessingModule = {
  type: "LOOP_APPLY",

  async run(input: ProcessingInput) {
    const R2_ACCOUNT_ID = mustEnv("R2_ACCOUNT_ID");
    const R2_ACCESS_KEY_ID = mustEnv("R2_ACCESS_KEY_ID");
    const R2_SECRET_ACCESS_KEY = mustEnv("R2_SECRET_ACCESS_KEY");
    const R2_BUCKET = mustEnv("R2_BUCKET");

    const ffmpegPath = envOrDefault("FFMPEG_PATH", "ffmpeg");
    const ffprobePath = envOrDefault("FFPROBE_PATH", "ffprobe");

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

    const inputObjectKey = (input as any).inputObjectKey as string | undefined;
    const outputObjectKey = input.outputObjectKey;

    if (!inputObjectKey) throw new Error("LOOP_APPLY bad input: inputObjectKey missing");
    if (!outputObjectKey) throw new Error("LOOP_APPLY bad input: outputObjectKey missing");

    const loop = (input.payload as any)?.loop ?? {};
    const startMs = Number(loop.startMs);
    const endMs = Number(loop.endMs);
    const crossfadeMs = Number(loop.crossfadeMs ?? 10);

    if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) {
      throw new Error("LOOP_APPLY bad input: loop.startMs/endMs must be numbers");
    }
    if (endMs <= startMs) {
      throw new Error("LOOP_APPLY bad input: endMs must be > startMs");
    }
    if (!Number.isFinite(crossfadeMs) || crossfadeMs < 0) {
      throw new Error("LOOP_APPLY bad input: crossfadeMs must be >= 0 number");
    }

    const startSec = startMs / 1000;
    const durationSec = Math.max(0, (endMs - startMs) / 1000);

    const crossfadeSecRaw = crossfadeMs / 1000;
    const crossfadeSec = clampCrossfadeSec(crossfadeSecRaw, durationSec);
    const fadeOutStart = Math.max(0, durationSec - crossfadeSec);

    console.log("[LOOP_APPLY] tool:", { ffmpegPath, ffprobePath });
    console.log("[LOOP_APPLY] params:", {
      assetId: input.assetId,
      inputVersion: input.inputVersion,
      inputObjectKey,
      outputObjectKey,
      startMs,
      endMs,
      crossfadeMs,
      startSec,
      durationSec,
    });

    const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "musproj-loop-"));
    const inPath = path.join(tmpDir, "input.wav");
    const outPath = path.join(tmpDir, "output.wav");

    try {
      // 1) download
      const getResp = await s3.send(
        new GetObjectCommand({
          Bucket: R2_BUCKET,
          Key: inputObjectKey,
        })
      );
      if (!getResp.Body) throw new Error("LOOP_APPLY: R2 GetObject returned empty body");
      await pipeline(getResp.Body as any, fs.createWriteStream(inPath));

      // 2) build filters (fade in/out)
      const filters: string[] = [];
      if (crossfadeSec > 0) {
        filters.push(`afade=t=in:st=0:d=${crossfadeSec}`);
        filters.push(`afade=t=out:st=${fadeOutStart}:d=${crossfadeSec}`);
      }
      const af = filters.length ? filters.join(",") : "";

      // 3) ffmpeg (use -t duration)
      const args: string[] = [
        "-hide_banner",
        "-y",
        "-i",
        inPath,
        "-ss",
        String(startSec),
        "-t",
        String(durationSec),
      ];

      if (af) args.push("-af", af);

      args.push("-c:a", "pcm_s16le", outPath);

      console.log("[LOOP_APPLY] ffmpeg args:", args);

      await runCmd(ffmpegPath, args, { signal: input.signal });

      // optional: log produced duration
      try {
        const outDur = await ffprobeDuration(ffprobePath, outPath);
        if (outDur != null) console.log("[LOOP_APPLY] output duration sec:", outDur);
      } catch (e) {
        console.log(
          "[LOOP_APPLY] ffprobe duration failed (non-fatal):",
          String((e as any)?.message ?? e)
        );
      }

      // 4) upload
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
          dspTool: "ffmpeg",
          loopStartMs: String(startMs),
          loopEndMs: String(endMs),
          loopCrossfadeMs: String(crossfadeMs),
        },
      };
    } finally {
      // always cleanup
      try {
        await fsp.rm(tmpDir, { recursive: true, force: true });
      } catch {}
    }
  },
};
