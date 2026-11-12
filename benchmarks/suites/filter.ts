import FilterStream from "web-utility-streams/FilterStream";
import {
  countIds,
  idReadable,
  idStream,
  isNotNoise,
  noisySourceBaseline,
  OURS,
  type Suite,
} from "../shared.ts";

export default {
  generated: true,
  description:
    "Filter a generated stream of `{ id }` objects with a noise chunk after each one, removing the noise",
  cases: [
    {
      library: OURS,
      name: "FilterStream",
      ours: true,
      run: ({ count }) =>
        countIds(
          idStream(count, { noise: true }).pipeThrough(
            new FilterStream(isNotNoise),
          ),
        ),
    },
    {
      library: "native",
      name: "TransformStream",
      run: ({ count }) =>
        countIds(
          idStream(count, { noise: true }).pipeThrough(
            new TransformStream({
              transform(chunk, controller) {
                if (isNotNoise(chunk)) controller.enqueue(chunk);
              },
            }),
          ),
        ),
    },
    {
      library: "node:stream",
      name: "Readable.prototype.filter (Node streams)",
      run: ({ count }) =>
        countIds(idReadable(count, { noise: true }).filter(isNotNoise)),
    },
    noisySourceBaseline,
  ],
} satisfies Suite;
