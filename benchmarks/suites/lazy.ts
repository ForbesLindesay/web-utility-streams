import LazyStream from "web-utility-streams/LazyStream";
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
    "Read a generated stream of `{ id }` objects that is only created when it is first read",
  cases: [
    {
      library: OURS,
      name: "LazyStream",
      ours: true,
      run: ({ count }) => countIds(new LazyStream(() => idStream(count))),
    },
    {
      library: OURS,
      name: "LazyStream (async function)",
      ours: true,
      run: ({ count }) => countIds(new LazyStream(async () => idStream(count))),
    },
    {
      library: "native",
      name: "ReadableStream.from(async generator)",
      run: ({ count }) =>
        countIds(
          ReadableStream.from(
            (async function* () {
              yield* idStream(count);
            })(),
          ),
        ),
    },
    sourceBaseline,
  ],
} satisfies Suite;
