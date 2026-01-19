import { ProcessingModule } from "../types/processing";

import { loopApplyModule } from "../modules/loop-apply/loopApply.module";
import { auphonicModule } from "../modules/auphonic/auphonic.module";
import { convertModule } from "../modules/convert/convert.module";
import { analyzeModule } from "../modules/analyze/analyze.module";

const operations: Record<string, Record<string, ProcessingModule>> = {
  DENOISE: {
    auphonic: auphonicModule,
  },
  LOOP_APPLY: {
    ffmpeg: loopApplyModule,
  },
  CONVERT: {
    ffmpeg: convertModule,
  },
  ANALYZE: {
    ffmpeg: analyzeModule,
  },
};

export function resolveOperation(type: string, provider?: string): { module: ProcessingModule; provider: string } | null {
  const t = String(type || "").trim();
  const p = String(provider || "").trim();

  if (!t) return null;

  const providers = operations[t];
  if (!providers) return null;

  if (p) {
    const m = providers[p];
    return m ? { module: m, provider: p } : null;
  }

  const keys = Object.keys(providers);
  if (keys.length === 1) {
    const only = keys[0];
    return { module: providers[only], provider: only };
  }

  return null;
}

export function listOps() {
  return Object.entries(operations).map(([type, provs]) => ({
    type,
    providers: Object.keys(provs),
  }));
}
