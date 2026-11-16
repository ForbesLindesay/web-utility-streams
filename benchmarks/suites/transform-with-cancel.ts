import TransformStreamWithCancel, {
  TransformStreamWithCancelPolyfill,
  type TransformStreamWithCancelConstructor,
} from "web-utility-streams/TransformStreamWithCancel";
import {
  countIds,
  idStream,
  OURS,
  sourceBaseline,
  type IdChunk,
  type RunResult,
  type Suite,
} from "../shared.ts";

const run =
  (TS: TransformStreamWithCancelConstructor, async: boolean) =>
  async ({ count }: { count: number }): Promise<RunResult> => {
    let flushed = 0;
    const result = await countIds(
      idStream(count).pipeThrough(
        new TS<IdChunk, IdChunk>({
          transform: async
            ? async (chunk, controller) => controller.enqueue(chunk)
            : (chunk, controller) => controller.enqueue(chunk),
          flush: () => void flushed++,
          cancel: () => {},
        }),
      ),
    );
    return flushed === 1 ? result : { error: `flushed ${flushed} times` };
  };

export default {
  generated: true,
  description:
    "Pass a generated stream of `{ id }` objects through a transform that enqueues each chunk unchanged, with `flush` and `cancel` methods. Node.js supports `cancel`, so `TransformStreamWithCancel` is the native `TransformStream`; the polyfill is what's used in runtimes that don't",
  cases: [
    {
      library: OURS,
      name: "TransformStreamWithCancelPolyfill (sync transform)",
      ours: true,
      run: run(TransformStreamWithCancelPolyfill, false),
    },
    {
      library: OURS,
      name: "TransformStreamWithCancelPolyfill (async transform)",
      ours: true,
      run: run(TransformStreamWithCancelPolyfill, true),
    },
    {
      library: OURS,
      name: "TransformStreamWithCancel (sync transform)",
      ours: true,
      run: run(TransformStreamWithCancel, false),
    },
    {
      library: "native",
      name: "TransformStream (sync transform)",
      run: run(TransformStream, false),
    },
    {
      library: "native",
      name: "TransformStream (async transform)",
      run: run(TransformStream, true),
    },
    sourceBaseline,
  ],
} satisfies Suite;
