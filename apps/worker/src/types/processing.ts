export type ProcessingInput = {
  assetId: string;
  inputVersion: number;
  jobId: string;

  /** where module must write output in R2 */
  outputObjectKey: string;

  payload: Record<string, any>;
};

export type ProcessingOutput = {
  ok: boolean;

  /** module must write to outputObjectKey and return it */
  objectKey: string;

  meta?: Record<string, string>;
  warnings?: string[];
};

export interface ProcessingModule {
  type: string;
  run(input: ProcessingInput): Promise<ProcessingOutput>;
}
