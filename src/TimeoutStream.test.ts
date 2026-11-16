import { describe, test, expect, vi } from "vitest";
import * as nativeModule from "./TimeoutStream.js";
import {
  delay,
  Delay,
  readableStream,
  readableStreamResults,
  runPipeline,
  testTransformRobustness,
  trackedSource,
} from "./internal/StreamTestUtils.js";

// Load a second copy of TimeoutStream, as it would be in a runtime whose
// TransformStream doesn't support the transformer's `cancel` method, so it
// uses TransformStreamWithCancelPolyfill
class TransformStreamWithoutCancel<I, O> extends TransformStream<I, O> {
  constructor(
    transformer?: Transformer<I, O>,
    writableStrategy?: QueuingStrategy<I>,
    readableStrategy?: QueuingStrategy<O>,
  ) {
    super(
      transformer && {
        start: transformer.start?.bind(transformer),
        transform: transformer.transform?.bind(transformer),
        flush: transformer.flush?.bind(transformer),
      },
      writableStrategy,
      readableStrategy,
    );
  }
}
vi.stubGlobal("TransformStream", TransformStreamWithoutCancel);
vi.resetModules();
const polyfilledModule = await import("./TimeoutStream.js");
vi.unstubAllGlobals();

test("TimeoutStream - uses the polyfill when TransformStream doesn't support cancel", () => {
  expect(new nativeModule.default({ timeoutMs: 0 })).toBeInstanceOf(
    TransformStream,
  );
  const polyfilled = new polyfilledModule.default({ timeoutMs: 0 });
  expect(polyfilled).not.toBeInstanceOf(TransformStream);
  void polyfilled.readable.cancel();
});

for (const [variant, { default: TimeoutStream, TimeoutStreamError }] of [
  ["native", nativeModule],
  ["polyfill", polyfilledModule],
] as const) {
  describe(variant, () => {
    test("TimeoutStream - passes chunks through unchanged when they arrive in time", async () => {
      expect(
        await readableStreamResults(
          readableStream([1, new Delay(20), 2, new Delay(20), 3]).pipeThrough(
            new TimeoutStream<number>({ timeoutMs: 60 }),
          ) satisfies ReadableStream<number>,
        ),
      ).toEqual([
        { ok: true, value: 1 },
        { ok: true, value: 2 },
        { ok: true, value: 3 },
      ]);
    });

    test("TimeoutStream - errors the stream, and cancels the source, if there is a gap between chunks", async () => {
      const source = trackedSource([1, 2, new Delay(200), 3]);
      const start = Date.now();
      const result = await runPipeline(
        source.stream
          .pipeThrough(new TimeoutStream<number>({ timeoutMs: 50 }))
          .pipeTo(new WritableStream()),
      );
      expect(result).toMatchObject({ ok: false });
      const error = (result as { error: unknown }).error;
      expect(error).toBeInstanceOf(TimeoutStreamError);
      expect(error).toMatchObject({
        name: "TimeoutStreamError",
        code: "TIMEOUT_STREAM_TIMEOUT",
        message: "TimeoutStream: no chunk was received for 50ms.",
      });
      // The second chunk is written ~15ms after the first, so the timeout is
      // reached ~65ms after the start
      const elapsed = Date.now() - start;
      expect(elapsed).toBeGreaterThanOrEqual(60);
      expect(elapsed).toBeLessThan(150);
      await delay(10);
      expect(source.state.cancelled).toBe(true);
    });

    test("TimeoutStream - errors the stream if the first chunk doesn't arrive in time", async () => {
      expect(
        await readableStreamResults(
          readableStream([new Delay(100), 1]).pipeThrough(
            new TimeoutStream<number>({ timeoutMs: 30 }),
          ),
        ),
      ).toEqual([{ ok: false }]);
    });

    test("TimeoutStream - stops the timeout when the input closes, even if the output isn't read straight away", async () => {
      const stream = new TimeoutStream<number>({ timeoutMs: 20 });
      const writer = stream.writable.getWriter();
      // One chunk fits in the stream's buffer, so the input can close
      // without the output being read
      void writer.write(1);
      void writer.close();
      await delay(60);
      expect(await readableStreamResults(stream.readable)).toEqual([
        { ok: true, value: 1 },
      ]);
    });

    test("TimeoutStream - a slow reader can cause a timeout", async () => {
      // The input is fast, but the output is read slowly, so backpressure
      // stops chunks being written for longer than the timeout
      const source = trackedSource([1, 2, 3, 4]);
      const output: number[] = [];
      const result = await runPipeline(
        source.stream
          .pipeThrough(new TimeoutStream<number>({ timeoutMs: 30 }))
          .pipeTo(
            new WritableStream(
              {
                async write(chunk) {
                  output.push(chunk);
                  await delay(80);
                },
              },
              { highWaterMark: 1 },
            ),
          ),
      );
      expect(result).toMatchObject({ ok: false });
      expect((result as { error: unknown }).error).toBeInstanceOf(
        TimeoutStreamError,
      );
      expect(output.length).toBeLessThan(4);
      await delay(10);
      expect(source.state.cancelled).toBe(true);
    });

    test("TimeoutStream - stops the timeout when the output is cancelled", async () => {
      let timeouts = 0;
      const stream = new TimeoutStream<number>({
        timeoutMs: 20,
        onTimeout: () => timeouts++,
      });
      await stream.readable.cancel();
      await delay(60);
      expect(timeouts).toBe(0);
    });

    test("TimeoutStream - stops the timeout when the input is aborted", async () => {
      let timeouts = 0;
      const stream = new TimeoutStream<number>({
        timeoutMs: 20,
        onTimeout: () => timeouts++,
      });
      await stream.writable.abort();
      await delay(60);
      expect(timeouts).toBe(0);
    });

    test("TimeoutStream - onTimeout can error the stream with a custom error", async () => {
      const source = trackedSource([1, new Delay(100), 2]);
      const result = await runPipeline(
        source.stream
          .pipeThrough(
            new TimeoutStream<number>({
              timeoutMs: 30,
              onTimeout: (controller) =>
                controller.error(new Error("Too slow")),
            }),
          )
          .pipeTo(new WritableStream()),
      );
      expect(result).toMatchObject({
        ok: false,
        error: { message: "Too slow" },
      });
    });

    test("TimeoutStream - onTimeout can end the stream cleanly", async () => {
      const source = trackedSource([1, 2, new Delay(100), 3]);
      expect(
        await readableStreamResults(
          source.stream.pipeThrough(
            new TimeoutStream<number>({
              timeoutMs: 30,
              onTimeout: (controller) => {
                controller.enqueue(-1);
                controller.terminate();
              },
            }),
          ),
        ),
      ).toEqual([
        { ok: true, value: 1 },
        { ok: true, value: 2 },
        { ok: true, value: -1 },
      ]);
      await delay(10);
      expect(source.state.cancelled).toBe(true);
    });

    test("TimeoutStream - if onTimeout doesn't end the stream, the timeout restarts with the next chunk", async () => {
      const timeouts: number[] = [];
      const start = Date.now();
      expect(
        await readableStreamResults(
          readableStream([1, new Delay(100), 2, new Delay(100), 3]).pipeThrough(
            new TimeoutStream<number>({
              timeoutMs: 40,
              onTimeout: () => timeouts.push(Date.now() - start),
            }),
          ),
        ),
      ).toEqual([
        { ok: true, value: 1 },
        { ok: true, value: 2 },
        { ok: true, value: 3 },
      ]);
      // Once after each gap
      expect(timeouts.length).toBe(2);
    });

    test("TimeoutStream - errors the stream if onTimeout throws", async () => {
      const result = await runPipeline(
        readableStream([1, new Delay(100), 2])
          .pipeThrough(
            new TimeoutStream<number>({
              timeoutMs: 30,
              onTimeout: () => {
                throw new Error("onTimeout failed");
              },
            }),
          )
          .pipeTo(new WritableStream()),
      );
      expect(result).toMatchObject({
        ok: false,
        error: { message: "onTimeout failed" },
      });
    });

    test("TimeoutStream - a function can choose the timeout for each chunk", async () => {
      const calls: [number, number | undefined][] = [];
      const result = await runPipeline(
        readableStream([1, new Delay(60), 2, new Delay(60), 3])
          .pipeThrough(
            new TimeoutStream<number>({
              timeoutMs: (remainingMs, chunk) => {
                calls.push([remainingMs, chunk]);
                // A long timeout after the first chunk, then a short one
                return chunk === undefined ? 30 : chunk === 1 ? 200 : 30;
              },
            }),
          )
          .pipeTo(new WritableStream()),
      );
      expect(result).toMatchObject({
        ok: false,
        error: { message: "TimeoutStream: no chunk was received for 30ms." },
      });
      expect(calls.map(([, chunk]) => chunk)).toEqual([undefined, 1, 2]);
      // The first timeout is chosen with 0 remaining
      expect(calls[0]![0]).toBe(0);
      // The first chunk arrives straight away, so most of the initial timeout
      // remains
      expect(calls[1]![0]).toBeGreaterThan(15);
      expect(calls[1]![0]).toBeLessThanOrEqual(30);
      // The second chunk arrives ~75ms into the 200ms timeout
      expect(calls[2]![0]).toBeGreaterThan(100);
      expect(calls[2]![0]).toBeLessThan(200);
    });

    test("TimeoutStream - a shorter timeout from the function fires before the previous, longer one would have", async () => {
      const start = Date.now();
      let firstChunk = true;
      const result = await runPipeline(
        readableStream([1, new Delay(300), 2])
          .pipeThrough(
            new TimeoutStream<number>({
              timeoutMs: () => {
                // The initial timeout is long, but the timeout after the first
                // chunk is short
                if (firstChunk) {
                  firstChunk = false;
                  return 1000;
                }
                return 30;
              },
            }),
          )
          .pipeTo(new WritableStream()),
      );
      expect(result).toMatchObject({ ok: false });
      expect(Date.now() - start).toBeLessThan(150);
    });

    test("TimeoutStream - errors the stream if the function throws", async () => {
      let timeouts = 0;
      const result = await runPipeline(
        readableStream([1, 2])
          .pipeThrough(
            new TimeoutStream<number>({
              timeoutMs: (_remaining, chunk) => {
                if (chunk === 2) throw new Error("Getter failed");
                return 30;
              },
              onTimeout: () => timeouts++,
            }),
          )
          .pipeTo(new WritableStream()),
      );
      expect(result).toMatchObject({
        ok: false,
        error: { message: "Getter failed" },
      });
      // The timer is stopped
      await delay(60);
      expect(timeouts).toBe(0);
    });

    test("TimeoutStream - throws for an invalid timeout", () => {
      expect(() => new TimeoutStream({ timeoutMs: -1 })).toThrow(RangeError);
      expect(() => new TimeoutStream({ timeoutMs: NaN })).toThrow(RangeError);
      expect(() => new TimeoutStream({ timeoutMs: () => -1 })).toThrow(
        RangeError,
      );
    });

    testTransformRobustness({
      name: "TimeoutStream",
      create: () => new TimeoutStream({ timeoutMs: 1000 }),
      validInput: [1, 2, 3],
    });
    testTransformRobustness({
      name: "TimeoutStream (timeout from a function)",
      create: () => new TimeoutStream({ timeoutMs: () => 1000 }),
      validInput: [1, 2, 3],
    });
  });
}
