import "dotenv/config";
import path from "node:path";
import os from "node:os";
import fs from "node:fs";
import fsp from "node:fs/promises";
import { pipeline } from "node:stream/promises";

import { GetObjectCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";

import { ProcessingModule, ProcessingInput } from "../../types/processing";
import { AppError } from "../../errors/appError";

function mustEnv(name: string): string {
  const v = process.env[name]?.trim();
  if (!v) throw new Error(`${name} missing in apps/worker/.env`);
  return v;
}

function envNum(name: string, fallback: number) {
  const v = process.env[name]?.trim();
  if (!v) return fallback;
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (!ms || ms <= 0) return Promise.resolve();

  return new Promise<void>((resolve, reject) => {
    if (signal?.aborted) {
      return reject(
        new AppError({
          code: "TIMED_OUT",
          message: "Aborted",
          retryable: true,
        })
      );
    }

    const t = setTimeout(() => {
      cleanup();
      resolve();
    }, ms);

    const onAbort = () => {
      cleanup();
      reject(
        new AppError({
          code: "TIMED_OUT",
          message: "Aborted",
          retryable: true,
        })
      );
    };

    const cleanup = () => {
      clearTimeout(t);
      if (signal) signal.removeEventListener("abort", onAbort);
    };

    if (signal) signal.addEventListener("abort", onAbort, { once: true });
  });
}

function asText(x: any) {
  return typeof x === "string" ? x : JSON.stringify(x);
}

async function safeReadText(resp: Response): Promise<string> {
  try {
    return await resp.text();
  } catch {
    return "";
  }
}

function isRetryableHttp(status: number) {
  return status === 429 || (status >= 500 && status <= 599);
}

function contentTypeByExt(ext: string) {
  const e = ext.toLowerCase();
  if (e === ".wav") return "audio/wav";
  if (e === ".mp3") return "audio/mpeg";
  if (e === ".m4a") return "audio/mp4";
  if (e === ".aac") return "audio/aac";
  if (e === ".ogg") return "audio/ogg";
  if (e === ".flac") return "audio/flac";
  return "application/octet-stream";
}

type AuphonicEnvelope<T> = {
  status_code: number;
  error_code: string | null;
  error_message: string;
  form_errors?: any;
  data: T;
};

async function auphonicFetchJson<T>(opts: {
  url: string;
  method?: string;
  headers?: Record<string, string>;
  body?: any;
  signal?: AbortSignal;
  badInputOn401403?: boolean;
}): Promise<AuphonicEnvelope<T>> {
  const { url, method = "GET", headers, body, signal, badInputOn401403 = true } = opts;

  try {
    const resp = await fetch(url, {
      method,
      headers,
      body,
      signal,
      redirect: "follow",
    });

    if (badInputOn401403 && (resp.status === 401 || resp.status === 403)) {
      const txt = await safeReadText(resp);
      throw new AppError({
        code: "BAD_INPUT",
        message: `Auphonic auth/config error: HTTP ${resp.status}${txt ? ` | ${txt}` : ""}`,
        retryable: false,
        details: { url, status: resp.status },
      });
    }

    if (!resp.ok) {
      const txt = await safeReadText(resp);
      const retry = isRetryableHttp(resp.status);
      throw new AppError({
        code: retry ? "NETWORK" : "PROVIDER_ERROR",
        message: `Auphonic HTTP error ${resp.status}: ${txt || resp.statusText}`,
        retryable: retry,
        details: { url, status: resp.status },
      });
    }

    const json = (await resp.json()) as AuphonicEnvelope<T>;

    if ((json as any)?.status_code && (json as any).status_code !== 200) {
      const msg = (json as any).error_message || "Auphonic returned non-200 status_code";
      const hasFormErrors =
        !!(json as any).form_errors && Object.keys((json as any).form_errors).length;

      throw new AppError({
        code: hasFormErrors ? "BAD_INPUT" : "PROVIDER_ERROR",
        message: `Auphonic API error: ${msg}`,
        retryable: !hasFormErrors,
        details: { url, auphonic: json },
      });
    }

    return json;
  } catch (e: any) {
    if (e?.name === "AbortError" || String(e?.message || "").toLowerCase().includes("aborted")) {
      throw new AppError({
        code: "TIMED_OUT",
        message: "Auphonic request aborted",
        retryable: true,
        details: { url },
        cause: e,
      });
    }

    if (e instanceof AppError) throw e;

    throw new AppError({
      code: "NETWORK",
      message: `Network/provider failure: ${asText(e?.message ?? e)}`,
      retryable: true,
      details: { url },
      cause: e,
    });
  }
}

function pickBestOutputFile(outputFiles: any[]): { downloadUrl: string; filename?: string } | null {
  if (!Array.isArray(outputFiles) || outputFiles.length === 0) return null;

  const wav = outputFiles.find((f) => {
    const fn = String(f?.filename ?? "").toLowerCase();
    const fmt = String(f?.format ?? "").toLowerCase();
    return !!f?.download_url && (fn.endsWith(".wav") || fmt === "wav");
  });
  if (wav?.download_url) return { downloadUrl: wav.download_url, filename: wav.filename };

  const audio = outputFiles.find((f) => {
    if (!f?.download_url) return false;
    const fmt = String(f?.format ?? "").toLowerCase();
    const fn = String(f?.filename ?? "").toLowerCase();
    if (fmt === "chaps" || fmt === "chapters" || fmt === "json" || fmt === "txt") return false;
    if (fn.endsWith(".txt") || fn.endsWith(".json") || fn.endsWith(".png") || fn.endsWith(".jpg"))
      return false;
    return true;
  });
  if (audio?.download_url) return { downloadUrl: audio.download_url, filename: audio.filename };

  const any = outputFiles.find((f) => f?.download_url);
  if (any?.download_url) return { downloadUrl: any.download_url, filename: any.filename };

  return null;
}

export const auphonicModule: ProcessingModule = {
  // не меняем API: /jobs принимает DENOISE
  type: "DENOISE",

  async run(input: ProcessingInput) {
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

    // ---------- Auphonic ----------
    const AUPHONIC_API_KEY = mustEnv("AUPHONIC_API_KEY");
    const presetFromEnv = process.env["AUPHONIC_PRESET"]?.trim() || "";
    const pollMs = envNum("AUPHONIC_POLL_INTERVAL_MS", 3000);

    const inputObjectKey = (input as any).inputObjectKey as string | undefined;
    const outputObjectKey = input.outputObjectKey;

    const preset =
      ((input.payload as any)?.auphonic?.preset as string | undefined)?.trim() || presetFromEnv;

    if (!inputObjectKey) {
      throw new AppError({
        code: "BAD_INPUT",
        message: "DENOISE (Auphonic) bad input: inputObjectKey missing",
        retryable: false,
        details: { assetId: input.assetId, jobId: input.jobId },
      });
    }
    if (!outputObjectKey) {
      throw new AppError({
        code: "BAD_INPUT",
        message: "DENOISE (Auphonic) bad input: outputObjectKey missing",
        retryable: false,
      });
    }
    if (!preset) {
      throw new AppError({
        code: "BAD_INPUT",
        message:
          "DENOISE (Auphonic) bad input: preset missing (payload.auphonic.preset or AUPHONIC_PRESET)",
        retryable: false,
      });
    }

    console.log("[AUPHONIC] start", {
      assetId: input.assetId,
      inputVersion: input.inputVersion,
      inputObjectKey,
      outputObjectKey,
      preset,
      jobId: input.jobId,
    });

    const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "musproj-auphonic-"));
    const inPath = path.join(tmpDir, "input.wav");
    const outPath = path.join(tmpDir, "output.bin");

    let providerJobId = "";

    try {
      // 1) download from R2
      const getResp = await s3.send(
        new GetObjectCommand({
          Bucket: R2_BUCKET,
          Key: inputObjectKey,
        })
      );
      if (!getResp.Body) {
        throw new AppError({
          code: "BAD_INPUT",
          message: "DENOISE (Auphonic): R2 GetObject returned empty body",
          retryable: false,
          details: { inputObjectKey },
        });
      }
      await pipeline(getResp.Body as any, fs.createWriteStream(inPath));

      // 2) build multipart with native FormData/Blob
      const buf = await fsp.readFile(inPath);
      const fileBlob = new Blob([buf], { type: "audio/wav" });

      const form = new FormData();
      form.set("preset", preset);
      form.set("action", "start");
      form.set("title", `asset:${input.assetId} v${input.inputVersion} denoise`);
      // имя файла важно для некоторых серверов
      form.set("input_file", fileBlob, "input.wav");

      const createResp = await auphonicFetchJson<{ uuid: string; status?: number; status_string?: string }>({
        url: "https://auphonic.com/api/simple/productions.json",
        method: "POST",
        headers: {
          Authorization: `bearer ${AUPHONIC_API_KEY}`,
        },
        body: form,
        signal: input.signal,
      });

      providerJobId = String(createResp?.data?.uuid || "");
      if (!providerJobId) {
        throw new AppError({
          code: "PROVIDER_ERROR",
          message: "Auphonic did not return production uuid",
          retryable: true,
          details: { createResp },
        });
      }

      console.log("[AUPHONIC] created", { providerJobId });

      // 3) poll status until done/error
      while (true) {
        await sleep(pollMs, input.signal);

        const st = await auphonicFetchJson<{ status: number; status_string: string }>({
          url: `https://auphonic.com/api/production/${providerJobId}/status.json`,
          method: "GET",
          headers: {
            Authorization: `bearer ${AUPHONIC_API_KEY}`,
          },
          signal: input.signal,
        });

        const status = Number((st as any)?.data?.status);
        const statusString = String((st as any)?.data?.status_string ?? "");

        console.log("[AUPHONIC] poll", { providerJobId, status, statusString });

        if (status === 3) break;
        if (status === 2) {
          let errorMessage = statusString || "Auphonic production failed";
          try {
            const det = await auphonicFetchJson<any>({
              url: `https://auphonic.com/api/production/${providerJobId}.json`,
              method: "GET",
              headers: { Authorization: `bearer ${AUPHONIC_API_KEY}` },
              signal: input.signal,
            });
            errorMessage = String(det?.data?.error_message || errorMessage);
          } catch {}

          throw new AppError({
            code: "PROVIDER_ERROR",
            message: `Auphonic processing error: ${errorMessage}`,
            retryable: true,
            details: { providerJobId, status, statusString },
          });
        }
      }

      // 4) get details to pick output file
      const details = await auphonicFetchJson<any>({
        url: `https://auphonic.com/api/production/${providerJobId}.json`,
        method: "GET",
        headers: {
          Authorization: `bearer ${AUPHONIC_API_KEY}`,
        },
        signal: input.signal,
      });

      const outputFiles = details?.data?.output_files;
      const picked = pickBestOutputFile(outputFiles);

      if (!picked?.downloadUrl) {
        throw new AppError({
          code: "PROVIDER_ERROR",
          message: "Auphonic finished but no downloadable output_files found",
          retryable: true,
          details: { providerJobId, outputFiles },
        });
      }

      // 5) download result
      const dl = new URL(picked.downloadUrl);
      dl.searchParams.set("bearer_token", AUPHONIC_API_KEY);

      const dlResp = await fetch(dl.toString(), {
        method: "GET",
        signal: input.signal,
        redirect: "follow",
      });

      if (!dlResp.ok || !dlResp.body) {
        const txt = await safeReadText(dlResp);
        const retry = isRetryableHttp(dlResp.status);
        throw new AppError({
          code: retry ? "NETWORK" : "PROVIDER_ERROR",
          message: `Failed to download Auphonic result: HTTP ${dlResp.status}${txt ? ` | ${txt}` : ""}`,
          retryable: retry,
          details: { providerJobId, url: dl.toString(), status: dlResp.status },
        });
      }

      await pipeline(dlResp.body as any, fs.createWriteStream(outPath));

      // 6) upload to R2
      const filename = String(picked.filename ?? "");
      const ext = filename ? path.extname(filename) : path.extname(outputObjectKey);
      const ct = contentTypeByExt(ext || ".wav");

      await s3.send(
        new PutObjectCommand({
          Bucket: R2_BUCKET,
          Key: outputObjectKey,
          Body: fs.createReadStream(outPath),
          ContentType: ct,
        })
      );

      console.log("[AUPHONIC] uploaded", { outputObjectKey, contentType: ct });

      return {
        ok: true,
        objectKey: outputObjectKey,
        meta: {
          provider: "auphonic",
          preset,
          providerJobId,
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
