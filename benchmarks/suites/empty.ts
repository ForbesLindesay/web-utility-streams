import EmptyStream from "web-utility-streams/EmptyStream";
import { type CaseResult, OURS, type Suite } from "../shared.ts";

async function readEmptyStreams(
  count: number,
  create: () => ReadableStream<never>,
): Promise<CaseResult> {
  let streams = 0;
  let idSum = 0;
  for (let i = 0; i < count; i++) {
    const { done } = await create().getReader().read();
    if (!done) return { error: "The stream was not empty" };
    streams++;
    idSum += i;
  }
  return { count: streams, idSum };
}

export default {
  generated: true,
  description:
    "Create and read one empty stream per chunk (the empty stream's index is counted as its id)",
  cases: [
    {
      library: OURS,
      name: "EmptyStream",
      ours: true,
      run: ({ count }) => readEmptyStreams(count, () => new EmptyStream()),
    },
    {
      library: "native",
      name: "new ReadableStream({ start: (c) => c.close() })",
      run: ({ count }) =>
        readEmptyStreams(
          count,
          () =>
            new ReadableStream({
              start(controller) {
                controller.close();
              },
            }),
        ),
    },
    {
      library: "native",
      name: "ReadableStream.from([])",
      run: ({ count }) =>
        readEmptyStreams(count, () => ReadableStream.from([])),
    },
  ],
} satisfies Suite;
