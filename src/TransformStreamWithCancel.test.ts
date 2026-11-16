import { describe, test, expect } from "vitest";
import TransformStreamWithCancel, {
  supportsTransformerCancel,
  TransformStreamWithCancelPolyfill,
  type TransformStreamWithCancelConstructor,
} from "./TransformStreamWithCancel.js";
import {
  delay,
  readableStream,
  readableStreamResults,
  testTransformRobustness,
} from "./internal/StreamTestUtils.js";

test("TransformStreamWithCancel - is the native TransformStream in Node.js, which supports cancel", () => {
  expect(supportsTransformerCancel).toBe(true);
  expect(TransformStreamWithCancel).toBe(TransformStream);
});

const implementations: [string, TransformStreamWithCancelConstructor][] = [
  // The native implementation is tested too, to check the tests' expectations
  // match the standard
  ["native TransformStream", TransformStream],
  ["TransformStreamWithCancelPolyfill", TransformStreamWithCancelPolyfill],
];

/**
 * Wait for a promise to settle, returning how it settled
 */
async function settled(
  promise: Promise<unknown>,
): Promise<{ ok: true; value: unknown } | { ok: false; error: unknown }> {
  try {
    return { ok: true, value: await promise };
  } catch (error) {
    return { ok: false, error };
  }
}

for (const [name, TS] of implementations) {
  describe(name, () => {
    test("passes chunks through unchanged without a transformer", async () => {
      expect(
        await readableStreamResults(
          readableStream([1, 2]).pipeThrough(new TS()),
        ),
      ).toEqual([
        { ok: true, value: 1 },
        { ok: true, value: 2 },
      ]);
    });

    test("calls transform with each chunk, then flush", async () => {
      const calls: string[] = [];
      const stream = new TS<number, number>({
        transform(chunk, controller) {
          calls.push(`transform ${chunk}`);
          controller.enqueue(chunk * 2);
        },
        async flush(controller) {
          await delay(5);
          calls.push("flush");
          controller.enqueue(100);
        },
        cancel() {
          calls.push("cancel");
        },
      });
      expect(
        await readableStreamResults(readableStream([1, 2]).pipeThrough(stream)),
      ).toEqual([
        { ok: true, value: 2 },
        { ok: true, value: 4 },
        { ok: true, value: 100 },
      ]);
      expect(calls).toEqual(["transform 1", "transform 2", "flush"]);
    });

    test("waits for async transforms", async () => {
      const stream = new TS<number, number>({
        async transform(chunk, controller) {
          await delay(chunk === 1 ? 20 : 1);
          controller.enqueue(chunk);
        },
      });
      expect(
        await readableStreamResults(readableStream([1, 2]).pipeThrough(stream)),
      ).toEqual([
        { ok: true, value: 1 },
        { ok: true, value: 2 },
      ]);
    });

    test("waits for an async start before transforming", async () => {
      const calls: string[] = [];
      const stream = new TS<number, number>({
        async start() {
          await delay(10);
          calls.push("started");
        },
        transform(chunk, controller) {
          calls.push(`transform ${chunk}`);
          controller.enqueue(chunk);
        },
      });
      const writer = stream.writable.getWriter();
      void writer.write(1);
      void writer.close();
      expect(await readableStreamResults(stream.readable)).toEqual([
        { ok: true, value: 1 },
      ]);
      expect(calls).toEqual(["started", "transform 1"]);
    });

    test("calls the transformer's methods with the transformer as `this`", async () => {
      const thisValues: unknown[] = [];
      const transformer = {
        start() {
          thisValues.push(this);
        },
        transform(chunk: number, controller: TransformStreamDefaultController) {
          thisValues.push(this);
          controller.enqueue(chunk);
        },
        flush() {
          thisValues.push(this);
        },
        cancel() {
          thisValues.push(this);
        },
      };
      await readableStreamResults(
        readableStream([1]).pipeThrough(new TS(transformer)),
      );
      await new TS(transformer).readable.cancel();
      expect(thisValues).toEqual([
        transformer,
        transformer,
        transformer,
        transformer,
        transformer,
      ]);
    });

    test("throws from the constructor if start throws", () => {
      expect(
        () =>
          new TS({
            start() {
              throw new Error("Failed");
            },
          }),
      ).toThrow("Failed");
    });

    test("errors both sides if start rejects", async () => {
      const stream = new TS({
        async start() {
          throw new Error("Failed");
        },
      });
      await expect(stream.readable.getReader().read()).rejects.toThrow(
        "Failed",
      );
      await expect(stream.writable.getWriter().closed).rejects.toThrow(
        "Failed",
      );
    });

    test("throws for a readableType or writableType", () => {
      expect(() => new TS({ readableType: "bytes" } as any)).toThrow(
        RangeError,
      );
      expect(() => new TS({ writableType: "bytes" } as any)).toThrow(
        RangeError,
      );
    });

    test("calls cancel when the readable side is cancelled, then errors the writable side", async () => {
      const calls: unknown[] = [];
      const stream = new TS({
        async cancel(reason) {
          await delay(5);
          calls.push(reason);
        },
        flush() {
          calls.push("flush");
        },
      });
      const writer = stream.writable.getWriter();
      await stream.readable.cancel("reason");
      expect(calls).toEqual(["reason"]);
      expect(await settled(writer.closed)).toEqual({
        ok: false,
        error: "reason",
      });
      expect(await settled(writer.write(1))).toEqual({
        ok: false,
        error: "reason",
      });
      // Aborting as well doesn't call cancel again
      await writer.abort("other").catch(() => {});
      expect(calls).toEqual(["reason"]);
    });

    test("calls cancel when the writable side is aborted, then errors the readable side", async () => {
      const calls: unknown[] = [];
      const stream = new TS({
        cancel(reason) {
          calls.push(reason);
        },
      });
      const reader = stream.readable.getReader();
      await stream.writable.abort("reason");
      expect(calls).toEqual(["reason"]);
      expect(await settled(reader.read())).toEqual({
        ok: false,
        error: "reason",
      });
      await reader.cancel("other").catch(() => {});
      expect(calls).toEqual(["reason"]);
    });

    test("unblocks a write that is waiting for backpressure when the readable side is cancelled", async () => {
      const transformed: number[] = [];
      const stream = new TS<number, number>({
        transform(chunk) {
          transformed.push(chunk);
        },
      });
      const writer = stream.writable.getWriter();
      const write = settled(writer.write(1));
      await delay(5);
      // Nothing has been read, so the write is waiting
      expect(transformed).toEqual([]);
      await stream.readable.cancel("reason");
      expect(await write).toEqual({ ok: false, error: "reason" });
      expect(transformed).toEqual([]);
    });

    test("rejects and errors the other side if cancel throws", async () => {
      const stream = new TS({
        cancel() {
          throw new Error("Cancel failed");
        },
      });
      const writer = stream.writable.getWriter();
      expect(await settled(stream.readable.cancel("reason"))).toMatchObject({
        ok: false,
        error: { message: "Cancel failed" },
      });
      expect(await settled(writer.closed)).toMatchObject({
        ok: false,
        error: { message: "Cancel failed" },
      });
    });

    test("rejects and errors the readable side if cancel rejects when aborting", async () => {
      const stream = new TS({
        async cancel() {
          throw new Error("Cancel failed");
        },
      });
      const reader = stream.readable.getReader();
      expect(await settled(stream.writable.abort("reason"))).toMatchObject({
        ok: false,
        error: { message: "Cancel failed" },
      });
      expect(await settled(reader.read())).toMatchObject({
        ok: false,
        error: { message: "Cancel failed" },
      });
    });

    test("doesn't call cancel when the readable side is cancelled after closing", async () => {
      const calls: string[] = [];
      const stream = new TS({
        flush() {
          calls.push("flush");
        },
        cancel() {
          calls.push("cancel");
        },
      });
      const writer = stream.writable.getWriter();
      await writer.close();
      await stream.readable.cancel();
      expect(calls).toEqual(["flush"]);
    });

    test("doesn't call cancel when the readable side is cancelled during flush", async () => {
      const calls: string[] = [];
      const stream = new TS({
        async flush() {
          await delay(10);
          calls.push("flush");
        },
        cancel() {
          calls.push("cancel");
        },
      });
      const writer = stream.writable.getWriter();
      const closed = settled(writer.close());
      await delay(1);
      await stream.readable.cancel();
      expect(await closed).toEqual({ ok: true, value: undefined });
      expect(calls).toEqual(["flush"]);
    });

    test("errors both sides if flush throws", async () => {
      const stream = new TS({
        flush() {
          throw new Error("Failed");
        },
      });
      const reader = stream.readable.getReader();
      expect(await settled(stream.writable.getWriter().close())).toMatchObject({
        ok: false,
        error: { message: "Failed" },
      });
      expect(await settled(reader.read())).toMatchObject({
        ok: false,
        error: { message: "Failed" },
      });
    });

    for (const [how, transform] of [
      [
        "throws",
        () => {
          throw new Error("Failed");
        },
      ],
      [
        "rejects",
        async () => {
          throw new Error("Failed");
        },
      ],
      [
        "calls controller.error()",
        (_chunk: unknown, controller: TransformStreamDefaultController) => {
          controller.error(new Error("Failed"));
        },
      ],
    ] as const) {
      test(`errors both sides, without calling cancel, if transform ${how}`, async () => {
        const calls: string[] = [];
        const stream = new TS({
          transform,
          cancel() {
            calls.push("cancel");
          },
        });
        const reader = stream.readable.getReader();
        const writer = stream.writable.getWriter();
        const read = settled(reader.read());
        await settled(writer.write(1));
        expect(await read).toMatchObject({
          ok: false,
          error: { message: "Failed" },
        });
        expect(await settled(writer.closed)).toMatchObject({
          ok: false,
          error: { message: "Failed" },
        });
        expect(calls).toEqual([]);
      });
    }

    test("controller.terminate() closes the readable side and errors the writable side", async () => {
      const calls: string[] = [];
      const stream = new TS<number, number>({
        transform(chunk, controller) {
          controller.enqueue(chunk);
          controller.terminate();
        },
        flush() {
          calls.push("flush");
        },
        cancel() {
          calls.push("cancel");
        },
      });
      const writer = stream.writable.getWriter();
      void writer.write(1).catch(() => {});
      expect(await readableStreamResults(stream.readable)).toEqual([
        { ok: true, value: 1 },
      ]);
      expect(await settled(writer.closed)).toMatchObject({
        ok: false,
        error: { name: "TypeError" },
      });
      expect(calls).toEqual([]);
    });

    test("enqueue throws after the readable side is cancelled", async () => {
      let controller!: TransformStreamDefaultController<number>;
      const stream = new TS<number, number>({
        start(c) {
          controller = c;
        },
      });
      await stream.readable.cancel();
      expect(() => controller.enqueue(1)).toThrow(TypeError);
    });

    test("only transforms when the output is read (default strategies)", async () => {
      const transformed: number[] = [];
      const stream = new TS<number, number>({
        transform(chunk, controller) {
          transformed.push(chunk);
          controller.enqueue(chunk);
        },
      });
      const writer = stream.writable.getWriter();
      expect(writer.desiredSize).toBe(1);
      void writer.write(1);
      void writer.write(2);
      void writer.write(3);
      await delay(5);
      expect(transformed).toEqual([]);
      expect(writer.desiredSize).toBe(-2);
      const reader = stream.readable.getReader();
      expect(await reader.read()).toEqual({ done: false, value: 1 });
      await delay(5);
      expect(transformed).toEqual([1]);
      expect(await reader.read()).toEqual({ done: false, value: 2 });
      await delay(5);
      expect(transformed).toEqual([1, 2]);
    });

    test("uses the queuing strategies", async () => {
      const transformed: number[] = [];
      const desiredSizes: (number | null)[] = [];
      const stream = new TS<number, number>(
        {
          transform(chunk, controller) {
            transformed.push(chunk);
            controller.enqueue(chunk);
            desiredSizes.push(controller.desiredSize);
          },
        },
        { highWaterMark: 3 },
        { highWaterMark: 2 },
      );
      const writer = stream.writable.getWriter();
      expect(writer.desiredSize).toBe(3);
      for (let i = 1; i <= 5; i++) void writer.write(i);
      await delay(5);
      // The readable side buffers 2 chunks before applying backpressure
      expect(transformed).toEqual([1, 2]);
      expect(desiredSizes).toEqual([1, 0]);
      expect(writer.desiredSize).toBe(0);
    });

    test("uses the readable strategy's size", async () => {
      const transformed: string[] = [];
      const stream = new TS<string, string>(
        {
          transform(chunk, controller) {
            transformed.push(chunk);
            controller.enqueue(chunk);
          },
        },
        undefined,
        { highWaterMark: 4, size: (chunk) => chunk.length },
      );
      const writer = stream.writable.getWriter();
      for (const chunk of ["ab", "cd", "ef"]) void writer.write(chunk);
      await delay(5);
      expect(transformed).toEqual(["ab", "cd"]);
    });

    testTransformRobustness({
      name,
      create: () =>
        new TS<number, number>({
          transform(chunk, controller) {
            if (chunk < 0) throw new Error("Invalid chunk");
            controller.enqueue(chunk);
          },
        }),
      validInput: [1, 2, 3],
      invalidInput: -1,
    });
  });
}
