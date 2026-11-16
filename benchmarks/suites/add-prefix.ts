import AddPrefixStream from "web-utility-streams/AddPrefixStream";
import {
  countIds,
  idStream,
  OURS,
  sourceBaseline,
  type IdChunk,
  type Suite,
} from "../shared.ts";

// The number of chunks in the short prefixes, which is typical of adding a
// header or a few initial events to a stream
const SHORT_PREFIX = 10;

/**
 * The first `length` ids, as an array
 */
const prefixIds = (length: number): IdChunk[] =>
  Array.from({ length }, (_, id) => ({ id }));

/**
 * The first `length` ids, from an async generator
 */
async function* asyncPrefixIds(length: number): AsyncGenerator<IdChunk> {
  for (let id = 0; id < length; id++) yield { id };
}

/**
 * The rest of the ids, after a prefix of `length` chunks
 */
const rest = (count: number, length: number) =>
  idStream(count - length, { start: length });

export default {
  generated: true,
  description: `Add a prefix to a generated stream of \`{ id }\` objects. The short prefix is the first ${SHORT_PREFIX} chunks, and the long prefix is the first half of the chunks`,
  cases: [
    {
      library: OURS,
      name: "AddPrefixStream (short array prefix)",
      ours: true,
      run: ({ count }) =>
        countIds(
          rest(count, SHORT_PREFIX).pipeThrough(
            new AddPrefixStream(prefixIds(SHORT_PREFIX)),
          ),
        ),
    },
    {
      library: OURS,
      name: "AddPrefixStream (short prefix from a promise)",
      ours: true,
      run: ({ count }) =>
        countIds(
          rest(count, SHORT_PREFIX).pipeThrough(
            new AddPrefixStream(Promise.resolve(prefixIds(SHORT_PREFIX))),
          ),
        ),
    },
    {
      library: OURS,
      name: "AddPrefixStream (long array prefix)",
      ours: true,
      run: ({ count }) => {
        const length = Math.floor(count / 2);
        return countIds(
          rest(count, length).pipeThrough(
            new AddPrefixStream(prefixIds(length)),
          ),
        );
      },
    },
    {
      library: OURS,
      name: "AddPrefixStream (long async generator prefix)",
      ours: true,
      run: ({ count }) => {
        const length = Math.floor(count / 2);
        return countIds(
          rest(count, length).pipeThrough(
            new AddPrefixStream(asyncPrefixIds(length)),
          ),
        );
      },
    },
    {
      library: "native",
      name: "TransformStream, enqueuing the short prefix in start",
      run: ({ count }) =>
        countIds(
          rest(count, SHORT_PREFIX).pipeThrough(
            new TransformStream<IdChunk, IdChunk>({
              start(controller) {
                for (const chunk of prefixIds(SHORT_PREFIX)) {
                  controller.enqueue(chunk);
                }
              },
            }),
          ),
        ),
    },
    {
      library: "native",
      name: "ReadableStream.from(async generator), with the short prefix",
      run: ({ count }) =>
        countIds(
          ReadableStream.from(
            (async function* () {
              yield* prefixIds(SHORT_PREFIX);
              yield* rest(count, SHORT_PREFIX);
            })(),
          ),
        ),
    },
    {
      library: "native",
      name: "ReadableStream.from(async generator), with the long async generator prefix",
      run: ({ count }) => {
        const length = Math.floor(count / 2);
        return countIds(
          ReadableStream.from(
            (async function* () {
              yield* asyncPrefixIds(length);
              yield* rest(count, length);
            })(),
          ),
        );
      },
    },
    sourceBaseline,
  ],
} satisfies Suite;
