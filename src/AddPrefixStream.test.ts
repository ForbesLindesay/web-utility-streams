import { test, expect } from "vitest";
import AddPrefixStream from "./AddPrefixStream.js";
import EmptyStream from "./EmptyStream.js";
import {
  countingSource,
  delay,
  Delay,
  readableStream,
  readableStreamResults,
  testTransformRobustness,
  trackedSource,
} from "./internal/StreamTestUtils.js";

const values = <T>(items: T[]) => items.map((value) => ({ ok: true, value }));

test("AddPrefixStream - emits an array prefix before the input", async () => {
  expect(
    await readableStreamResults(
      readableStream([3, 4]).pipeThrough(
        new AddPrefixStream([1, 2]),
      ) satisfies ReadableStream<number>,
    ),
  ).toEqual(values([1, 2, 3, 4]));
});

test("AddPrefixStream - emits a sync iterable prefix", async () => {
  function* prefix() {
    yield "a";
    yield "b";
  }
  expect(
    await readableStreamResults(
      readableStream(["c"]).pipeThrough(new AddPrefixStream(prefix())),
    ),
  ).toEqual(values(["a", "b", "c"]));
});

test("AddPrefixStream - emits an async iterable prefix", async () => {
  async function* prefix() {
    yield 1;
    await delay(20);
    yield 2;
  }
  expect(
    await readableStreamResults(
      readableStream([3]).pipeThrough(new AddPrefixStream(prefix())),
    ),
  ).toEqual(values([1, 2, 3]));
});

test("AddPrefixStream - accepts a ReadableStream as the prefix", async () => {
  expect(
    await readableStreamResults(
      readableStream([3]).pipeThrough(
        new AddPrefixStream(readableStream([1, 2])),
      ),
    ),
  ).toEqual(values([1, 2, 3]));
});

test("AddPrefixStream - accepts a promise for the prefix", async () => {
  expect(
    await readableStreamResults(
      readableStream([3]).pipeThrough(
        new AddPrefixStream(delay(10).then(() => [1, 2])),
      ),
    ),
  ).toEqual(values([1, 2, 3]));
});

test("AddPrefixStream - accepts a string as the prefix", async () => {
  expect(
    await readableStreamResults(
      readableStream(["c"]).pipeThrough(new AddPrefixStream("ab")),
    ),
  ).toEqual(values(["a", "b", "c"]));
});

test("AddPrefixStream - works with an empty prefix", async () => {
  expect(
    await readableStreamResults(
      readableStream([1, 2]).pipeThrough(new AddPrefixStream([])),
    ),
  ).toEqual(values([1, 2]));
});

test("AddPrefixStream - emits the prefix when the input is empty", async () => {
  expect(
    await readableStreamResults(
      new EmptyStream().pipeThrough(new AddPrefixStream([1, 2])),
    ),
  ).toEqual(values([1, 2]));
});

test("AddPrefixStream - emits the prefix without waiting for the input", async () => {
  const reader = readableStream([new Delay(1000), 3])
    .pipeThrough(new AddPrefixStream([1, 2]))
    .getReader();
  const start = Date.now();
  expect(await reader.read()).toEqual({ done: false, value: 1 });
  expect(await reader.read()).toEqual({ done: false, value: 2 });
  expect(Date.now() - start).toBeLessThan(500);
  await reader.cancel();
});

test("AddPrefixStream - only reads the prefix as the output is read", async () => {
  const pulled: number[] = [];
  function* prefix() {
    for (let i = 1; i <= 3; i++) {
      pulled.push(i);
      yield i;
    }
  }
  const reader = new EmptyStream()
    .pipeThrough(new AddPrefixStream(prefix()))
    .getReader();
  await delay(10);
  expect(pulled).toEqual([]);
  expect(await reader.read()).toEqual({ done: false, value: 1 });
  await delay(10);
  expect(pulled).toEqual([1]);
  expect(await reader.read()).toEqual({ done: false, value: 2 });
  await delay(10);
  expect(pulled).toEqual([1, 2]);
  await reader.cancel();
});

test("AddPrefixStream - does not read ahead from the input while the prefix is being emitted", async () => {
  const source = countingSource([10, 11, 12, 13, 14]);
  const reader = source.stream
    .pipeThrough(new AddPrefixStream([1, 2, 3]))
    .getReader();
  await delay(10);
  // pipeThrough reads one chunk to fill the writable side's buffer
  expect(source.pulled.length).toBeLessThanOrEqual(2);
  expect(await reader.read()).toEqual({ done: false, value: 1 });
  expect(await reader.read()).toEqual({ done: false, value: 2 });
  await delay(10);
  expect(source.pulled.length).toBeLessThanOrEqual(2);
  await reader.cancel();
});

test("AddPrefixStream - once the prefix is done, reads at most 4 chunks ahead of the output", async () => {
  const source = countingSource(Array.from({ length: 20 }, (_, i) => i));
  const reader = source.stream
    .pipeThrough(new AddPrefixStream([100]))
    .getReader();
  expect(await reader.read()).toEqual({ done: false, value: 100 });
  for (let i = 0; i < 10; i++) {
    expect(await reader.read()).toEqual({ done: false, value: i });
    await delay(10);
    expect(source.pulled.length).toBeGreaterThan(i + 1);
    expect(source.pulled.length).toBeLessThanOrEqual(i + 5);
  }
  await reader.cancel();
});

test("AddPrefixStream - aborting the writable side errors the output without waiting for the prefix, and closes the prefix", async () => {
  let closed = false;
  async function* prefix() {
    try {
      yield 1;
      await delay(1000);
      yield 2;
    } finally {
      closed = true;
    }
  }
  const stream = new AddPrefixStream<number>(prefix());
  const reader = stream.readable.getReader();
  const writer = stream.writable.getWriter();
  // This write waits until the prefix has been read
  const pendingWrite = writer.write(10);
  expect(await reader.read()).toEqual({ done: false, value: 1 });
  const start = Date.now();
  const pendingRead = reader.read();
  await writer.abort(new Error("Aborted"));
  await expect(pendingRead).rejects.toThrow("Aborted");
  await expect(pendingWrite).rejects.toThrow("Aborted");
  expect(Date.now() - start).toBeLessThan(500);
  // The generator can only finish once its pending `next` call has
  // finished, so wait for that.
  await delay(1100);
  expect(closed).toBe(true);
});

test("AddPrefixStream - a source error in a pipeline errors the output without waiting for the prefix", async () => {
  async function* prefix() {
    yield 1;
    await delay(1000);
    yield 2;
  }
  const reader = readableStream<number>([new Delay(20)], { err: true })
    .pipeThrough(new AddPrefixStream(prefix()))
    .getReader();
  expect(await reader.read()).toEqual({ done: false, value: 1 });
  const start = Date.now();
  await expect(reader.read()).rejects.toThrow("Example error");
  expect(Date.now() - start).toBeLessThan(500);
  await delay(1000);
});

test("AddPrefixStream - errors the output and cancels the source if the prefix throws", async () => {
  const source = trackedSource([10, 11]);
  function* prefix() {
    yield 1;
    throw new Error("Prefix failed");
  }
  expect(
    await readableStreamResults(
      source.stream.pipeThrough(new AddPrefixStream(prefix())),
    ),
  ).toEqual([{ ok: true, value: 1 }, { ok: false }]);
  await delay(20);
  expect(source.state.cancelled).toBe(true);
});

test("AddPrefixStream - errors the output and cancels the source if the async prefix throws", async () => {
  const source = trackedSource([10, 11]);
  async function* prefix() {
    yield 1;
    throw new Error("Prefix failed");
  }
  expect(
    await readableStreamResults(
      source.stream.pipeThrough(new AddPrefixStream(prefix())),
    ),
  ).toEqual([{ ok: true, value: 1 }, { ok: false }]);
  await delay(20);
  expect(source.state.cancelled).toBe(true);
});

test("AddPrefixStream - errors the output and cancels the source if the promise rejects", async () => {
  const source = trackedSource([10, 11]);
  const reader = source.stream
    .pipeThrough(
      new AddPrefixStream<number>(Promise.reject(new Error("Prefix failed"))),
    )
    .getReader();
  await expect(reader.read()).rejects.toThrow("Prefix failed");
  await delay(20);
  expect(source.state.cancelled).toBe(true);
});

test("AddPrefixStream - errors if the prefix is not iterable", async () => {
  for (const prefix of [42, Promise.resolve(42)]) {
    const reader = new EmptyStream()
      .pipeThrough(new AddPrefixStream<number>(prefix as any))
      .getReader();
    await expect(reader.read()).rejects.toThrow(TypeError);
  }
});

test("AddPrefixStream - cancelling the output closes the prefix and cancels the source with the reason", async () => {
  let closed = false;
  async function* prefix() {
    try {
      yield 1;
      await delay(20);
      yield 2;
      yield 3;
    } finally {
      closed = true;
    }
  }
  const reasons: unknown[] = [];
  const source = new ReadableStream({
    cancel: (reason) => void reasons.push(reason),
  });
  const reader = source.pipeThrough(new AddPrefixStream(prefix())).getReader();
  expect(await reader.read()).toEqual({ done: false, value: 1 });
  await reader.cancel("Some reason");
  await delay(50);
  expect(closed).toBe(true);
  expect(reasons).toEqual(["Some reason"]);
});

test("AddPrefixStream - closes an iterable from the promise if cancelled while waiting for it", async () => {
  let closed = false;
  function* prefix() {
    try {
      yield 1;
    } finally {
      closed = true;
    }
  }
  const generator = prefix();
  const reader = new EmptyStream()
    .pipeThrough(
      new AddPrefixStream<number>(
        delay(20).then(() => {
          // Start the generator, so closing it runs the `finally` block
          generator.next();
          return generator;
        }),
      ),
    )
    .getReader();
  const pendingRead = reader.read();
  await reader.cancel();
  expect(await pendingRead).toEqual({ done: true, value: undefined });
  await delay(50);
  expect(closed).toBe(true);
});

test("AddPrefixStream - has the readable and writable sides of a transform stream", () => {
  const stream = new AddPrefixStream([1]) satisfies TransformStream<
    number,
    number
  >;
  expect(stream.readable).toBeInstanceOf(ReadableStream);
  expect(stream.writable).toBeInstanceOf(WritableStream);
});

testTransformRobustness({
  name: "AddPrefixStream (array)",
  create: () => new AddPrefixStream(["prefix"]),
  validInput: [1, 2, 3],
});

testTransformRobustness({
  name: "AddPrefixStream (async generator)",
  create: () =>
    new AddPrefixStream(
      (async function* () {
        yield "a";
        await delay(5);
        yield "b";
      })(),
    ),
  validInput: [1, 2, 3],
});

testTransformRobustness({
  name: "AddPrefixStream (promise)",
  create: () => new AddPrefixStream(delay(5).then(() => ["a", "b"])),
  validInput: [1, 2, 3],
});
