export type ProcessingInput = {
  assetId: string;
  inputVersion: number;
  jobId: string;

  /** where module must write output in R2 */
  outputObjectKey: string;

  /** where module should read input from in R2 (passed by worker) */
  inputObjectKey?: string;

  /** optional output version if module wants it (worker passes it) */
  outputVersion?: number;

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
