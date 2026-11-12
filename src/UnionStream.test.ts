import { test, expect } from "vitest";
import EmptyStream from "./EmptyStream.js";
import MapStream from "./MapStream.js";
import {
  countingSource,
  delay,
  Delay,
  destination,
  readableStream,
  readableStreamResults,
  runPipeline,
  trackedSource,
} from "./internal/StreamTestUtils.js";
import UnionStream from "./UnionStream.js";

test("UnionStream - merges two streams in the order the chunks are available", async () => {
  expect(
    await readableStreamResults(
      new UnionStream([
        readableStream([1, 2, new Delay(400), 3, 4]),
        readableStream([new Delay(200), "a", "b", "c"]),
      ]),
    ),
  ).toEqual([
    { ok: true, value: 1 },
    { ok: true, value: 2 },
    { ok: true, value: "a" },
    { ok: true, value: "b" },
    { ok: true, value: "c" },
    { ok: true, value: 3 },
    { ok: true, value: 4 },
  ]);
});

test("UnionStream - can propagate cancellation", async () => {
  // Cancelling is propagated to both streams
  let cancelledA = false;
  let cancelledB = false;
  const stream = new UnionStream([
    readableStream([1, 2, new Delay(200), 3, 4], {
      onCancel: () => (cancelledA = true),
    }),
    readableStream([new Delay(100), "a", "b", "c"], {
      onCancel: () => (cancelledB = true),
    }),
  ]);
  await stream.cancel();
  expect(cancelledA).toBe(true);
  expect(cancelledB).toBe(true);
});

test("UnionStream - can propagate errors", async () => {
  let cancelledA = false;
  expect(
    await readableStreamResults(
      new UnionStream([
        readableStream([1, 2, new Delay(200), 3, 4], {
          onCancel: () => (cancelledA = true),
        }),
        readableStream([new Delay(100), "a"], { err: true }),
      ]),
    ),
  ).toEqual([
    { ok: true, value: 1 },
    { ok: true, value: 2 },
    { ok: true, value: "a" },
    { ok: false },
  ]);

  // When we got an error on one stream, we should cancel the other ones
  expect(cancelledA).toBe(true);

  // Wait to check no uncaught exceptions are thrown after the stream is closed.
  await delay(200);
});

test("UnionStream - can handle downstream errors", async () => {
  let cancelledA = false;
  let cancelledB = false;
  expect(
    await readableStreamResults(
      new UnionStream([
        readableStream([1, 2, new Delay(200), 3, 4], {
          onCancel: () => (cancelledA = true),
        }),
        readableStream([new Delay(100), "a"], {
          onCancel: () => (cancelledB = true),
        }),
      ]).pipeThrough(
        new TransformStream({
          transform(value, controller) {
            if (typeof value === "number") controller.enqueue(value);
            else controller.error(new Error("Expected a number"));
          },
        }),
      ),
    ),
  ).toEqual([{ ok: true, value: 1 }, { ok: true, value: 2 }, { ok: false }]);

  // The source streams are cancelled asynchronously so we add a tiny delay here to wait for that
  await delay(100);

  // When we got an error on one stream, we should cancel the other ones
  expect(cancelledA).toBe(true);
  expect(cancelledB).toBe(true);
});

test("UnionStream - closes immediately when there are no source streams", async () => {
  expect(await readableStreamResults(new UnionStream([]))).toEqual([]);
});

test("UnionStream - waits for all source streams to close", async () => {
  expect(
    await readableStreamResults(
      new UnionStream([
        readableStream<number>([]),
        readableStream([new Delay(50), 1]),
      ]),
    ),
  ).toEqual([{ ok: true, value: 1 }]);
});

test("UnionStream - passes the cancel reason to the source streams", async () => {
  const reasons: unknown[] = [];
  const source = () =>
    new ReadableStream({ cancel: (reason) => void reasons.push(reason) });
  await new UnionStream([source(), source()]).cancel("Some reason");
  expect(reasons).toEqual(["Some reason", "Some reason"]);
});

test("UnionStream - source errors in a pipeline", async () => {
  const a = trackedSource([1, 2, 3, 4]);
  const b = trackedSource([5], { err: true });
  const dest = destination();
  const result = await runPipeline(
    new UnionStream([a.stream, b.stream])
      .pipeThrough(
        new MapStream(async (value) => {
          await delay(5);
          return value;
        }),
      )
      .pipeTo(dest.stream),
  );
  expect(result).toMatchObject({
    ok: false,
    error: { message: "Example error" },
  });
  await delay(50);
  expect(a.state.cancelled).toBe(true);
});

test("UnionStream - destination errors in a pipeline", async () => {
  const a = trackedSource([1, 2, 3, 4]);
  const b = trackedSource([5, 6, 7]);
  const dest = destination({ failOnWrite: 1 });
  const result = await runPipeline(
    new UnionStream([a.stream, b.stream])
      .pipeThrough(
        new MapStream(async (value) => {
          await delay(5);
          return value;
        }),
      )
      .pipeTo(dest.stream),
  );
  expect(result).toMatchObject({
    ok: false,
    error: { message: "Destination failed" },
  });
  await delay(50);
  expect(a.state.cancelled).toBe(true);
  expect(b.state.cancelled).toBe(true);
});

test("UnionStream - pipe is aborted", async () => {
  const a = trackedSource([1, 2, 3, 4]);
  const b = trackedSource([5, 6, 7]);
  const dest = destination();
  const controller = new AbortController();
  setTimeout(() => controller.abort(new Error("Aborted")), 20);
  const result = await runPipeline(
    new UnionStream([a.stream, b.stream, new EmptyStream()]).pipeTo(
      dest.stream,
      {
        signal: controller.signal,
      },
    ),
  );
  expect(result).toMatchObject({ ok: false, error: { message: "Aborted" } });
  await delay(50);
  expect(a.state.cancelled).toBe(true);
  expect(b.state.cancelled).toBe(true);
});

test("UnionStream - by default, reads ahead of the reader from every source", async () => {
  const a = countingSource([1, 2, 3]);
  const b = countingSource(["a", "b", "c"]);
  new UnionStream([a.stream, b.stream]);
  await delay(10);
  // Each source is read once, as they're read in parallel
  expect(a.pulled).toEqual([1]);
  expect(b.pulled).toEqual(["a"]);
});

test("UnionStream - with a highWaterMark of 0, only reads when the output is read", async () => {
  const a = countingSource([1, 2, 3]);
  const b = countingSource(["a", "b", "c"]);
  const reader = new UnionStream([a.stream, b.stream], {
    highWaterMark: 0,
  }).getReader();
  await delay(10);
  expect(a.pulled).toEqual([]);
  expect(b.pulled).toEqual([]);
  expect(await reader.read()).toEqual({ done: false, value: 1 });
  await delay(10);
  // Both sources are read in parallel, so the chunk from `b` is buffered
  expect(a.pulled).toEqual([1]);
  expect(b.pulled).toEqual(["a"]);
  expect(await reader.read()).toEqual({ done: false, value: "a" });
  await delay(10);
  expect(a.pulled).toEqual([1]);
  expect(b.pulled).toEqual(["a"]);
});

test("UnionStream - with a larger highWaterMark, reads further ahead", async () => {
  const a = countingSource([1, 2, 3, 4, 5, 6]);
  const stream = new UnionStream([a.stream], { highWaterMark: 3 });
  await delay(10);
  expect(a.pulled).toEqual([1, 2, 3]);
  const reader = stream.getReader();
  expect(await reader.read()).toEqual({ done: false, value: 1 });
  await delay(10);
  expect(a.pulled).toEqual([1, 2, 3, 4]);
});

test("UnionStream - uses the size function of the queuing strategy", async () => {
  const a = countingSource(["a", "bb", "ccc", "dddd"]);
  new UnionStream([a.stream], {
    highWaterMark: 5,
    size: (chunk) => chunk.length,
  });
  await delay(10);
  // Reads until the total size of the queued chunks reaches 5
  expect(a.pulled).toEqual(["a", "bb", "ccc"]);
});
