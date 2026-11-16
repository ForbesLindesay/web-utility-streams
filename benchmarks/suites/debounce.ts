import DebounceStream, {
  type DebounceStreamOptions,
} from "web-utility-streams/DebounceStream";
import {
  idStream,
  OURS,
  sourceBaseline,
  type IdChunk,
  type RunResult,
  type Suite,
} from "../shared.ts";

// Much longer than any run, so the output is always the same: the first
// chunk (unless the mode is "TRAILING") and the last chunk (unless the mode
// is "LEADING"), which is emitted when the input closes.
const DEBOUNCE_MS = 60_000;

/**
 * Read the debounced output, and check it has the expected ids
 */
async function checkDebounced(
  output: AsyncIterable<IdChunk>,
  count: number,
  mode: NonNullable<DebounceStreamOptions["mode"]>,
): Promise<RunResult> {
  const ids: number[] = [];
  for await (const chunk of output) ids.push(chunk.id);
  const expected = [
    ...(mode === "TRAILING" ? [] : [0]),
    ...(mode === "LEADING" ? [] : [count - 1]),
  ];
  if (ids.join() !== expected.join()) {
    return { error: `emitted ids ${ids.join()}, expected ${expected.join()}` };
  }
  return { ignored: ids.length };
}

const ours = (mode: NonNullable<DebounceStreamOptions["mode"]>) => ({
  library: OURS,
  name: `DebounceStream (${mode})`,
  ours: true,
  run: ({ count }: { count: number }) =>
    checkDebounced(
      idStream(count).pipeThrough(
        new DebounceStream<IdChunk>({ debounceMs: DEBOUNCE_MS, mode }),
      ),
      count,
      mode,
    ),
});

export default {
  generated: true,
  description: `Debounce a generated stream of \`{ id }\` objects, with a period of ${DEBOUNCE_MS / 1000}s, so almost every chunk is dropped`,
  cases: [
    ours("BOTH"),
    ours("LEADING"),
    ours("TRAILING"),
    {
      library: "native",
      name: "TransformStream with setTimeout (BOTH)",
      run: ({ count }) => {
        let timer: ReturnType<typeof setTimeout> | undefined;
        let hasTrailing = false;
        let trailing: IdChunk | undefined;
        const endPeriod = (
          controller: TransformStreamDefaultController<IdChunk>,
        ) => {
          timer = undefined;
          if (!hasTrailing) return;
          controller.enqueue(trailing!);
          hasTrailing = false;
          timer = setTimeout(endPeriod, DEBOUNCE_MS, controller);
        };
        return checkDebounced(
          idStream(count).pipeThrough(
            new TransformStream<IdChunk, IdChunk>({
              transform(chunk, controller) {
                if (timer !== undefined) {
                  hasTrailing = true;
                  trailing = chunk;
                  return;
                }
                controller.enqueue(chunk);
                timer = setTimeout(endPeriod, DEBOUNCE_MS, controller);
              },
              flush(controller) {
                clearTimeout(timer);
                if (hasTrailing) controller.enqueue(trailing!);
              },
            }),
          ),
          count,
          "BOTH",
        );
      },
    },
    sourceBaseline,
  ],
} satisfies Suite;
