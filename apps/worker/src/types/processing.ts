export type ProcessingInput = {
  assetId: string;
  inputVersion: number;
  jobId: string;
  payload: Record<string, any>;
};

export type ProcessingOutput = {
  ok: boolean;

  /** R2 object key of produced audio */
  objectKey: string;

  /** optional metadata to store with version */
  meta?: Record<string, string>;
};

export interface ProcessingModule {
  /** unique job type */
  type: string;

  /** main processing function */
  run(input: ProcessingInput): Promise<ProcessingOutput>;
}
