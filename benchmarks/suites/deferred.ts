import DeferredStream from "web-utility-streams/DeferredStream";
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
    "Read a generated stream of `{ id }` objects that is only available from a promise",
  cases: [
    {
      library: OURS,
      name: "DeferredStream",
      ours: true,
      run: ({ count }) =>
        countIds(new DeferredStream(Promise.resolve(idStream(count)))),
    },
    {
      library: "native",
      name: "TransformStream, piping the stream into it once it resolves",
      run: ({ count }) => {
        const { readable, writable } = new TransformStream();
        Promise.resolve(idStream(count))
          .then((stream) => stream.pipeTo(writable))
          .catch(() => {});
        return countIds(readable);
      },
    },
    {
      library: "native",
      name: "ReadableStream.from(async generator)",
      run: ({ count }) =>
        countIds(
          ReadableStream.from(
            (async function* () {
              yield* await Promise.resolve(idStream(count));
            })(),
          ),
        ),
    },
    sourceBaseline,
  ],
} satisfies Suite;
