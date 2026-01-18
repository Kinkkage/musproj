export type AppErrorShape = {
  code: string;
  message: string;
  retryable: boolean;
  details?: any;
  cause?: any;
};

export class AppError extends Error {
  code: string;
  retryable: boolean;
  details?: any;
  cause?: any;

  constructor(opts: AppErrorShape) {
    super(opts.message);
    this.name = "AppError";
    this.code = opts.code;
    this.retryable = opts.retryable;
    this.details = opts.details;
    this.cause = opts.cause;
  }

  toJSON() {
    return {
      code: this.code,
      message: this.message,
      retryable: this.retryable,
      details: this.details,
    };
  }
}

export function isAppError(err: any): err is AppError {
  return err && typeof err === "object" && err.name === "AppError" && typeof err.code === "string";
}

export function toAppError(err: any, fallbackCode = "PROCESSING_FAILED"): AppError {
  if (isAppError(err)) return err;

  const msg = String(err?.message ?? err);
  const msgLower = msg.toLowerCase();

  // ---- TIMEOUT (отдельный код) ----
  // BullMQ/Node могут давать разные сообщения, ловим по ключевым словам
  if (msgLower.includes("timed out") || msgLower.includes("timeout")) {
    return new AppError({
      code: "TIMED_OUT",
      message: msg,
      retryable: true, // обычно таймаут можно попробовать ретраить
      details: err?.stack ? { stack: err.stack } : undefined,
      cause: err,
    });
  }

  // ---- network / rate limit (чаще retryable) ----
  const retryable =
    err?.code === "ECONNRESET" ||
    err?.code === "ETIMEDOUT" ||
    err?.code === "ENOTFOUND" ||
    msgLower.includes("too many requests") ||
    msg.includes("429");

  return new AppError({
    code: retryable ? "RETRYABLE" : fallbackCode,
    message: msg,
    retryable,
    details: err?.stack ? { stack: err.stack } : undefined,
    cause: err,
  });
}
