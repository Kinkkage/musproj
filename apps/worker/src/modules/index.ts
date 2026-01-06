import { ProcessingModule } from "../types/processing";

import { loopApplyModule } from "./loop-apply/loopApply.module";
import { auphonicModule } from "./auphonic/auphonic.module";

export const modules: Record<string, ProcessingModule> = {
  [loopApplyModule.type]: loopApplyModule,
  [auphonicModule.type]: auphonicModule,
};
