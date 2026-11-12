import MapStream from "web-utility-streams/MapStream";
import {
  copy,
  countIds,
  idReadable,
  idStream,
  OURS,
  sourceBaseline,
  type Suite,
} from "../shared.ts";

export default {
  generated: true,
  description:
    "Map each chunk of a generated stream of `{ id }` objects to a copy of it",
  cases: [
    {
      library: OURS,
      name: "MapStream (sync function)",
      ours: true,
      run: ({ count }) =>
        countIds(idStream(count).pipeThrough(new MapStream(copy))),
    },
    {
      library: OURS,
      name: "MapStream (async function)",
      ours: true,
      run: ({ count }) =>
        countIds(
          idStream(count).pipeThrough(
            new MapStream(async (value) => copy(value)),
          ),
        ),
    },
    {
      library: "native",
      name: "TransformStream",
      run: ({ count }) =>
        countIds(
          idStream(count).pipeThrough(
            new TransformStream({
              transform(chunk, controller) {
                controller.enqueue(copy(chunk));
              },
            }),
          ),
        ),
    },
    {
      library: "node:stream",
      name: "Readable.prototype.map (Node streams)",
      run: ({ count }) => countIds(idReadable(count).map(copy)),
    },
    sourceBaseline,
    {
      library: "baseline",
      name: "Read the Node.js Readable source only",
      baseline: true,
      run: ({ count }) => countIds(idReadable(count)),
    },
  ],
} satisfies Suite;
