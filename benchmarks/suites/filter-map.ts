import FilterMapStream from "web-utility-streams/FilterMapStream";
import FilterStream from "web-utility-streams/FilterStream";
import MapStream from "web-utility-streams/MapStream";
import {
  copy,
  countIds,
  type IdChunk,
  idReadable,
  idStream,
  isNotNoise,
  type Noise,
  noisySourceBaseline,
  OURS,
  type Suite,
} from "../shared.ts";

const copyIfNotNoise = (value: IdChunk | Noise): IdChunk | undefined =>
  isNotNoise(value) ? { id: value.id } : undefined;

export default {
  generated: true,
  description:
    "Filter and map a generated stream of `{ id }` objects with a noise chunk after each one, removing the noise and copying the other chunks",
  cases: [
    {
      library: OURS,
      name: "FilterMapStream (sync function)",
      ours: true,
      run: ({ count }) =>
        countIds(
          idStream(count, { noise: true }).pipeThrough(
            new FilterMapStream(copyIfNotNoise),
          ),
        ),
    },
    {
      library: OURS,
      name: "FilterMapStream (async function)",
      ours: true,
      run: ({ count }) =>
        countIds(
          idStream(count, { noise: true }).pipeThrough(
            new FilterMapStream(async (value) => copyIfNotNoise(value)),
          ),
        ),
    },
    {
      library: OURS,
      name: "FilterStream + MapStream",
      ours: true,
      run: ({ count }) =>
        countIds(
          idStream(count, { noise: true })
            .pipeThrough(new FilterStream(isNotNoise))
            .pipeThrough(new MapStream(copy)),
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
                const result = copyIfNotNoise(chunk);
                if (result !== undefined) controller.enqueue(result);
              },
            }),
          ),
        ),
    },
    {
      library: "node:stream",
      name: "Readable.prototype.filter + map (Node streams)",
      run: ({ count }) =>
        countIds(
          idReadable(count, { noise: true }).filter(isNotNoise).map(copy),
        ),
    },
    noisySourceBaseline,
  ],
} satisfies Suite;
