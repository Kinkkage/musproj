export type BaseJob = {
  assetId: string;
  inputVersion: number;
};

export type LoopApplyJob = BaseJob & {
  type: "LOOP_APPLY";
  loop: {
    startMs: number;
    endMs: number;
    crossfadeMs: number;
  };
};

export type SimpleJob = BaseJob & {
  type: "LOOP" | "DENOISE" | "SEPARATE" | "AUPHONIC";
};

export type AudioJob = LoopApplyJob | SimpleJob;
