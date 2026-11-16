import TimeoutStream from "web-utility-streams/TimeoutStream";
import {
  countIds,
  idStream,
  OURS,
  sourceBaseline,
  type IdChunk,
  type Suite,
} from "../shared.ts";

// Much longer than any run, so the timeout never fires
const TIMEOUT_MS = 60_000;

export default {
  generated: true,
  description: `Error a generated stream of \`{ id }\` objects if no chunk is received for ${TIMEOUT_MS / 1000}s, which never happens, so every chunk is passed through unchanged`,
  cases: [
    {
      library: OURS,
      name: "TimeoutStream (fixed timeout)",
      ours: true,
      run: ({ count }) =>
        countIds(
          idStream(count).pipeThrough(
            new TimeoutStream<IdChunk>({ timeoutMs: TIMEOUT_MS }),
          ),
        ),
    },
    {
      library: OURS,
      name: "TimeoutStream (timeout from a function)",
      ours: true,
      run: ({ count }) =>
        countIds(
          idStream(count).pipeThrough(
            new TimeoutStream<IdChunk>({ timeoutMs: () => TIMEOUT_MS }),
          ),
        ),
    },
    {
      library: "native",
      name: "TransformStream, restarting a setTimeout for each chunk",
      run: ({ count }) => {
        let timer: ReturnType<typeof setTimeout> | undefined;
        const onTimeout = (
          controller: TransformStreamDefaultController<IdChunk>,
        ) => controller.error(new Error("Timeout"));
        return countIds(
          idStream(count).pipeThrough(
            new TransformStream<IdChunk, IdChunk>({
              start(controller) {
                timer = setTimeout(onTimeout, TIMEOUT_MS, controller);
              },
              transform(chunk, controller) {
                clearTimeout(timer);
                timer = setTimeout(onTimeout, TIMEOUT_MS, controller);
                controller.enqueue(chunk);
              },
              flush() {
                clearTimeout(timer);
              },
              cancel() {
                clearTimeout(timer);
              },
            }),
          ),
        );
      },
    },
    sourceBaseline,
  ],
} satisfies Suite;
