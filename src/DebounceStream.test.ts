import { test, expect } from "vitest";
import DebounceStream, {
  type DebounceStreamOptions,
} from "./DebounceStream.js";
import {
  delay,
  Delay,
  readableStreamResults,
  testTransformRobustness,
  trackedSource,
} from "./internal/StreamTestUtils.js";

interface Emitted {
  value: number;
  /** Milliseconds since the first write */
  time: number;
}

/**
 * Write `steps` to a DebounceStream (waiting for each `Delay`), then close it,
 * while reading the output and recording when each chunk was emitted.
 */
async function debounce(
  options: DebounceStreamOptions,
  steps: (number | Delay)[],
): Promise<Emitted[]> {
  const stream = new DebounceStream<number>(options);
  const start = Date.now();
  const emitted: Emitted[] = [];
  const reading = (async () => {
    const reader = stream.readable.getReader();
    while (true) {
      const result = await reader.read();
      if (result.done) return;
      emitted.push({ value: result.value, time: Date.now() - start });
    }
  })();
  const writer = stream.writable.getWriter();
  for (const step of steps) {
    if (step instanceof Delay) await step.wait();
    else await writer.write(step);
  }
  await writer.close();
  await reading;
  return emitted;
}

const valuesOf = (emitted: Emitted[]) => emitted.map((e) => e.value);

test("DebounceStream - BOTH emits the first and the most recent chunk", async () => {
  const emitted = await debounce({ debounceMs: 100 }, [
    1,
    2,
    3,
    new Delay(150),
  ]);
  expect(valuesOf(emitted)).toEqual([1, 3]);
  expect(emitted[0]!.time).toBeLessThan(50);
  // The trailing chunk is emitted at the end of the period
  expect(emitted[1]!.time).toBeGreaterThanOrEqual(90);
  expect(emitted[1]!.time).toBeLessThan(140);
});

test("DebounceStream - LEADING only emits the first chunk in each period", async () => {
  const emitted = await debounce({ debounceMs: 100, mode: "LEADING" }, [
    1,
    2,
    new Delay(50),
    3,
    new Delay(100),
    4,
    5,
  ]);
  expect(valuesOf(emitted)).toEqual([1, 4]);
  expect(emitted[1]!.time).toBeGreaterThanOrEqual(140);
});

test("DebounceStream - TRAILING emits the most recent chunk at the end of each period", async () => {
  const emitted = await debounce({ debounceMs: 100, mode: "TRAILING" }, [
    1,
    2,
    3,
    new Delay(150),
  ]);
  expect(valuesOf(emitted)).toEqual([3]);
  expect(emitted[0]!.time).toBeGreaterThanOrEqual(90);
  expect(emitted[0]!.time).toBeLessThan(140);
});

test("DebounceStream - BOTH only emits a single chunk once", async () => {
  expect(
    valuesOf(await debounce({ debounceMs: 50 }, [1, new Delay(100)])),
  ).toEqual([1]);
});

test("DebounceStream - emits chunks that are further apart than the period", async () => {
  for (const mode of ["LEADING", "TRAILING", "BOTH"] as const) {
    expect(
      valuesOf(
        await debounce({ debounceMs: 30, mode }, [
          1,
          new Delay(80),
          2,
          new Delay(80),
          3,
          new Delay(80),
        ]),
      ),
    ).toEqual([1, 2, 3]);
  }
});

test("DebounceStream - emitting a trailing chunk starts a new period", async () => {
  const emitted = await debounce({ debounceMs: 100 }, [
    1,
    2,
    // 2 is emitted at 100ms, which starts a new period that ends at 200ms
    new Delay(150),
    3,
    4,
    new Delay(100),
  ]);
  expect(valuesOf(emitted)).toEqual([1, 2, 4]);
  expect(emitted[2]!.time).toBeGreaterThanOrEqual(190);
  expect(emitted[2]!.time).toBeLessThan(240);
});

test("DebounceStream - closing emits the waiting chunk straight away", async () => {
  for (const mode of ["TRAILING", "BOTH"] as const) {
    const start = Date.now();
    const emitted = await debounce({ debounceMs: 1000, mode }, [1, 2, 3]);
    expect(valuesOf(emitted)).toEqual(mode === "BOTH" ? [1, 3] : [3]);
    expect(Date.now() - start).toBeLessThan(500);
  }
});

test("DebounceStream - closing with LEADING drops the chunks in the current period", async () => {
  expect(
    valuesOf(await debounce({ debounceMs: 1000, mode: "LEADING" }, [1, 2, 3])),
  ).toEqual([1]);
});

test("DebounceStream - closes immediately with no input", async () => {
  expect(await debounce({ debounceMs: 1000 }, [])).toEqual([]);
});

test("DebounceStream - writes don't wait for the output to be read", async () => {
  const stream = new DebounceStream<number>({ debounceMs: 1000 });
  const writer = stream.writable.getWriter();
  for (let i = 0; i < 100; i++) await writer.write(i);
  await writer.close();
  // 0 was emitted, but replaced by 99 as it hadn't been read
  expect(await readableStreamResults(stream.readable)).toEqual([
    { ok: true, value: 99 },
  ]);
});

test("DebounceStream - only keeps the most recent emitted chunk if the output isn't read", async () => {
  const stream = new DebounceStream<number>({ debounceMs: 30 });
  const writer = stream.writable.getWriter();
  await writer.write(1); // emitted straight away, but not read
  await writer.write(2);
  await delay(50); // 2 is emitted, replacing 1
  await writer.write(3);
  await writer.close(); // 3 is emitted, replacing 2
  expect(await readableStreamResults(stream.readable)).toEqual([
    { ok: true, value: 3 },
  ]);
});

test("DebounceStream - aborting the writable side errors the output", async () => {
  const stream = new DebounceStream<number>({ debounceMs: 100 });
  const reader = stream.readable.getReader();
  const writer = stream.writable.getWriter();
  await writer.write(1);
  await writer.write(2);
  expect(await reader.read()).toEqual({ done: false, value: 1 });
  await writer.abort(new Error("Aborted"));
  await expect(reader.read()).rejects.toThrow("Aborted");
  // The trailing chunk is never emitted
  await delay(150);
});

test("DebounceStream - cancelling the output cancels the source with the reason", async () => {
  const reasons: unknown[] = [];
  const source = new ReadableStream({
    start(controller) {
      controller.enqueue(1);
      controller.enqueue(2);
    },
    cancel: (reason) => void reasons.push(reason),
  });
  const reader = source
    .pipeThrough(new DebounceStream({ debounceMs: 100 }))
    .getReader();
  expect(await reader.read()).toEqual({ done: false, value: 1 });
  await reader.cancel("Some reason");
  await delay(150);
  expect(reasons).toEqual(["Some reason"]);
});

test("DebounceStream - a source error errors the output", async () => {
  const source = trackedSource([1, 2, new Delay(50)], { err: true });
  expect(
    await readableStreamResults(
      source.stream.pipeThrough(new DebounceStream({ debounceMs: 1000 })),
    ),
  ).toEqual([{ ok: true, value: 1 }, { ok: false }]);
});

test("DebounceStream - rejects invalid options", () => {
  expect(() => new DebounceStream({ debounceMs: -1 })).toThrow(RangeError);
  expect(() => new DebounceStream({ debounceMs: NaN })).toThrow(RangeError);
  expect(
    () => new DebounceStream({ debounceMs: 1, mode: "MIDDLE" as any }),
  ).toThrow(TypeError);
});

test("DebounceStream - is typed as a TransformStream", async () => {
  const output = new ReadableStream<string>({
    start(controller) {
      controller.enqueue("a");
      controller.close();
    },
  }).pipeThrough(
    new DebounceStream<string>({ debounceMs: 10 }),
  ) satisfies ReadableStream<string>;
  expect(await readableStreamResults(output)).toEqual([
    { ok: true, value: "a" },
  ]);
});

// The test source writes a chunk every 15ms, so with a shorter period every
// chunk is emitted, which the robustness tests need.
for (const mode of ["LEADING", "TRAILING", "BOTH"] as const) {
  testTransformRobustness({
    name: `DebounceStream (${mode})`,
    create: () => new DebounceStream({ debounceMs: 5, mode }),
    validInput: [1, 2, 3],
  });
}
