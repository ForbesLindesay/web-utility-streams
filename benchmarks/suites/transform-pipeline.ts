import MapStream from "web-utility-streams/MapStream";
import TransformPipelineStream from "web-utility-streams/TransformPipelineStream";
import { copy, countIds, idStream, OURS, type Suite } from "../shared.ts";

export default {
  generated: true,
  description:
    "Pipe a generated stream of `{ id }` objects through two `MapStream`s, combined into one transform stream",
  cases: [
    {
      library: OURS,
      name: "TransformPipelineStream",
      ours: true,
      run: ({ count }) =>
        countIds(
          idStream(count).pipeThrough(
            new TransformPipelineStream((input) =>
              input
                .pipeThrough(new MapStream(copy))
                .pipeThrough(new MapStream(copy)),
            ),
          ),
        ),
    },
    {
      library: "baseline",
      name: "pipeThrough each MapStream directly",
      baseline: true,
      run: ({ count }) =>
        countIds(
          idStream(count)
            .pipeThrough(new MapStream(copy))
            .pipeThrough(new MapStream(copy)),
        ),
    },
  ],
} satisfies Suite;
