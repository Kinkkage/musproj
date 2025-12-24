export type JobType =
  | "LOOP_PREVIEW"
  | "LOOP_APPLY"
  | "DENOISE"
  | "SEPARATE"
  | "AUPHONIC_ENHANCE";

export type AssetKind =
  | "source"
  | "loopPreview"
  | "loop"
  | "enhanced"
  | "vocals"
  | "instrumental"
  | "other";

export type JobPayload = {
  type: JobType;
  assetId: string;
  inputVersion?: number;
  // optional params for future
  params?: Record<string, unknown>;
};

export type JobResultOk = {
  ok: true;
  processedAt: string;
  createdVersion?: number;
  objectKey?: string;
  from?: string;
  extra?: Record<string, unknown>;
};

export type JobResultFail = {
  ok: false;
  processedAt: string;
  error: string;
  extra?: Record<string, unknown>;
};

export type JobResult = JobResultOk | JobResultFail;

export type AssetMeta = {
  assetId: string;
  createdAt: string;
  status: "uploaded_pending" | "ready" | "processing" | "error";
};

export type AssetVersionMeta = {
  version: number;
  objectKey: string;
  kind: AssetKind;
  createdAt: string;
  fromVersion?: number;
  note?: string;
  extra?: Record<string, unknown>;
};
