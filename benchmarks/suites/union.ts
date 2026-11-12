import type { ReadableStreamReadResult } from "node:stream/web";
import UnionStream from "web-utility-streams/UnionStream";
import {
  countIds,
  idStreams,
  OURS,
  SOURCE_COUNT,
  type Suite,
} from "../shared.ts";

/**
 * Merge async iterators by racing their reads. This is roughly what you would
 * write without `UnionStream`.
 */
async function* raceMerge<T>(streams: ReadableStream<T>[]): AsyncGenerator<T> {
  type Reader = ReadableStreamDefaultReader<T>;
  const readers = streams.map((stream) => stream.getReader());
  const reads = new Map<
    Reader,
    Promise<{ reader: Reader; result: ReadableStreamReadResult<T> }>
  >();
  const read = (reader: Reader) =>
    reader.read().then((result) => ({ reader, result }));
  for (const reader of readers) reads.set(reader, read(reader));
  while (reads.size) {
    const { reader, result } = await Promise.race(reads.values());
    if (result.done) {
      reads.delete(reader);
    } else {
      reads.set(reader, read(reader));
      yield result.value;
    }
  }
}

export default {
  generated: true,
  description: `Merge ${SOURCE_COUNT} generated streams of \`{ id }\` objects, read in parallel`,
  cases: [
    {
      library: OURS,
      name: "UnionStream",
      ours: true,
      run: ({ count }) =>
        countIds(new UnionStream(idStreams(count, SOURCE_COUNT))),
    },
    {
      library: OURS,
      name: "UnionStream with { highWaterMark: 0 }",
      ours: true,
      run: ({ count }) =>
        countIds(
          new UnionStream(idStreams(count, SOURCE_COUNT), {
            highWaterMark: 0,
          }),
        ),
    },
    {
      library: "native",
      name: "ReadableStream.from(async generator racing reads)",
      run: ({ count }) =>
        countIds(
          ReadableStream.from(raceMerge(idStreams(count, SOURCE_COUNT))),
        ),
    },
    {
      library: "baseline",
      name: "Read each source stream in turn",
      baseline: true,
      run: async ({ count }) => {
        let total = { count: 0, idSum: 0 };
        for (const stream of idStreams(count, SOURCE_COUNT)) {
          const { count, idSum } = await countIds(stream);
          total = { count: total.count + count, idSum: total.idSum + idSum };
        }
        return total;
      },
    },
  ],
} satisfies Suite;
