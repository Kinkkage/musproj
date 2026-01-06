import { ProcessingModule } from "../../types/processing";

export const LoopModule: ProcessingModule = {
  type: "LOOP",

  async run(input) {
    // пока просто заглушка
    return {
      ok: true,
      outputVersion: input.inputVersion,
      meta: { note: "loop processed (stub)" },
    };
  },
};
