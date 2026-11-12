import ConcatStream from "web-utility-streams/ConcatStream";
import {
  countIds,
  idStreams,
  OURS,
  SOURCE_COUNT,
  type Suite,
} from "../shared.ts";

export default {
  generated: true,
  description: `Join ${SOURCE_COUNT} generated streams of \`{ id }\` objects, one after another`,
  cases: [
    {
      library: OURS,
      name: "ConcatStream",
      ours: true,
      run: ({ count }) =>
        countIds(new ConcatStream(idStreams(count, SOURCE_COUNT))),
    },
    {
      library: OURS,
      name: "ConcatStream with { highWaterMark: 0 }",
      ours: true,
      run: ({ count }) =>
        countIds(
          new ConcatStream(idStreams(count, SOURCE_COUNT), {
            highWaterMark: 0,
          }),
        ),
    },
    {
      library: "native",
      name: "TransformStream, piping each source into it in turn",
      run: ({ count }) => {
        const { readable, writable } = new TransformStream();
        (async () => {
          for (const stream of idStreams(count, SOURCE_COUNT)) {
            await stream.pipeTo(writable, { preventClose: true });
          }
          await writable.close();
        })();
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
              for (const stream of idStreams(count, SOURCE_COUNT)) {
                yield* stream;
              }
            })(),
          ),
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
