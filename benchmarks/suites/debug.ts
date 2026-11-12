import DebugStream from "web-utility-streams/DebugStream";
import {
  countIds,
  idStream,
  OURS,
  sourceBaseline,
  type Suite,
} from "../shared.ts";

export default {
  generated: true,
  description:
    "Call a function with each chunk of a generated stream of `{ id }` objects, passing the chunks through unchanged",
  cases: [
    {
      library: OURS,
      name: "DebugStream",
      ours: true,
      run: async ({ count }) => {
        let seen = 0;
        const result = await countIds(
          idStream(count).pipeThrough(new DebugStream(() => seen++)),
        );
        return seen === count ? result : { error: `saw ${seen} chunks` };
      },
    },
    {
      library: "native",
      name: "TransformStream",
      run: async ({ count }) => {
        let seen = 0;
        const result = await countIds(
          idStream(count).pipeThrough(
            new TransformStream({
              transform(chunk, controller) {
                seen++;
                controller.enqueue(chunk);
              },
            }),
          ),
        );
        return seen === count ? result : { error: `saw ${seen} chunks` };
      },
    },
    sourceBaseline,
  ],
} satisfies Suite;
