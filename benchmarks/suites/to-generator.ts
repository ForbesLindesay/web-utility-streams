import readableStreamToGenerator from "web-utility-streams/readableStreamToGenerator";
import {
  type Counted,
  countIds,
  type IdChunk,
  idStream,
  OURS,
  type Suite,
} from "../shared.ts";

/**
 * Count the chunks using a reader directly, which is a little faster than
 * async iteration
 */
async function countWithReader(
  stream: ReadableStream<IdChunk>,
): Promise<Counted> {
  const reader = stream.getReader();
  let count = 0;
  let idSum = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) return { count, idSum };
    count++;
    idSum += value.id;
  }
}

export default {
  generated: true,
  description:
    "Iterate over a generated stream of `{ id }` objects with `for await`",
  cases: [
    {
      library: OURS,
      name: "readableStreamToGenerator",
      ours: true,
      run: ({ count }) => countIds(readableStreamToGenerator(idStream(count))),
    },
    {
      library: "native",
      name: "ReadableStream's built-in async iterator",
      run: ({ count }) => countIds(idStream(count)),
    },
    {
      library: "baseline",
      name: "reader.read() in a loop (no async iterator)",
      baseline: true,
      run: ({ count }) => countWithReader(idStream(count)),
    },
  ],
} satisfies Suite;
