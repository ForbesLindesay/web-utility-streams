import { test, expect } from "vitest";
import readableStreamToGenerator from "./readableStreamToGenerator.js";

test("readableStreamToGenerator - Finishing normally works fine", async () => {
  let i = 0;
  const stream = new ReadableStream<number>({
    pull(controller) {
      if (i === 3) {
        controller.close();
      } else {
        controller.enqueue(i++);
      }
    },
  });
  const output = [];
  for await (const v of readableStreamToGenerator(stream)) {
    output.push(v);
  }
  expect(output).toEqual([0, 1, 2]);
});

class TestStream extends ReadableStream<number> {
  cancelled = false;
  cancelReason: unknown = undefined;
  constructor({ blockedForever } = { blockedForever: false }) {
    let i = 0;
    super({
      pull: async (controller) => {
        if (blockedForever) {
          await new Promise(() => {});
        } else {
          controller.enqueue(i++);
        }
      },
      cancel: async (reason) => {
        await new Promise((resolve) => setTimeout(resolve, 100));
        this.cancelled = true;
        this.cancelReason = reason;
      },
    });
  }
}

test("readableStreamToGenerator - Cancelling is immediate even if we're waiting for a value", async () => {
  const stream = new TestStream({ blockedForever: true });
  const generator = readableStreamToGenerator(stream);
  let gotChunk = false;
  const chunk = generator.next().then((v) => {
    gotChunk = true;
    return v;
  });
  await new Promise((resolve) => setTimeout(resolve, 100));
  expect(gotChunk).toBe(false);
  await generator.return();
  expect(stream.cancelled).toBe(true);
  expect(stream.cancelReason).toBeUndefined();
  expect(await chunk).toEqual({ done: true, value: undefined });
});

test("readableStreamToGenerator - breaking out of loop cancels stream", async () => {
  const stream = new TestStream();

  const output = [];
  for await (const v of readableStreamToGenerator(stream)) {
    output.push(v);
    if (v >= 2) {
      break;
    }
  }
  expect(output).toEqual([0, 1, 2]);
  expect(stream.cancelled).toBe(true);
  expect(stream.cancelReason).toBeUndefined();

  // Make sure calling cancel again is a no-op
  await stream.cancel();
});

test("readableStreamToGenerator - throwing out of loop cancels stream", async () => {
  const stream = new TestStream();

  const output = [];
  let thrown;
  try {
    for await (const v of readableStreamToGenerator(stream)) {
      output.push(v);
      if (v >= 2) {
        throw "Test error";
      }
    }
  } catch (e) {
    thrown = e;
  }
  expect(thrown).toBe("Test error");
  expect(output).toEqual([0, 1, 2]);
  expect(stream.cancelled).toBe(true);
  expect(stream.cancelReason).toBeUndefined();

  // Make sure calling cancel again is a no-op
  await stream.cancel();
});

test("readableStreamToGenerator - stream errors forwarded", async () => {
  let i = 0;
  const stream = new ReadableStream<number>({
    pull(controller) {
      if (i === 3) {
        controller.error("Test stream error");
      } else {
        controller.enqueue(i++);
      }
    },
  });
  const output = [];
  let thrown;
  try {
    for await (const v of readableStreamToGenerator(stream)) {
      output.push(v);
    }
  } catch (e) {
    thrown = e;
  }
  expect(thrown).toBe("Test stream error");
  expect(output).toEqual([0, 1, 2]);

  // Make sure calling cancel again throws the same error
  thrown = undefined;
  try {
    await stream.cancel();
  } catch (e) {
    thrown = e;
  }
  expect(thrown).toBe("Test stream error");
});

test("readableStreamToGenerator - calling return/dispose after the stream has ended is a no-op", async () => {
  const stream = new ReadableStream<number>({
    start(controller) {
      controller.enqueue(1);
      controller.close();
    },
  });
  const generator = readableStreamToGenerator(stream);
  expect(await generator.next()).toEqual({ done: false, value: 1 });
  expect(await generator.next()).toEqual({ done: true, value: undefined });
  expect(await generator.next()).toEqual({ done: true, value: undefined });
  expect(await generator.return()).toEqual({ done: true, value: undefined });
  await generator[Symbol.asyncDispose]();
  // The lock is released, so the stream can be read again
  expect(stream.locked).toBe(false);
});

test("readableStreamToGenerator - calling next after return returns done", async () => {
  const stream = new TestStream();
  const generator = readableStreamToGenerator(stream);
  expect(await generator.next()).toEqual({ done: false, value: 0 });
  await generator.return();
  expect(await generator.next()).toEqual({ done: true, value: undefined });
  expect(stream.locked).toBe(false);
});

test("readableStreamToGenerator - calling next after an error returns done", async () => {
  const stream = new ReadableStream<number>({
    start(controller) {
      controller.error(new Error("Test stream error"));
    },
  });
  const generator = readableStreamToGenerator(stream);
  await expect(generator.next()).rejects.toThrow("Test stream error");
  expect(await generator.next()).toEqual({ done: true, value: undefined });
  expect(await generator.return()).toEqual({ done: true, value: undefined });
});

test("readableStreamToGenerator - throw cancels the stream with the error and rethrows it", async () => {
  const stream = new TestStream();
  const generator = readableStreamToGenerator(stream);
  await generator.next();
  await expect(generator.throw("Some error")).rejects.toBe("Some error");
  expect(stream.cancelled).toBe(true);
  expect(stream.cancelReason).toBe("Some error");
});

test("readableStreamToGenerator - works with await using", async () => {
  const stream = new TestStream();
  {
    await using generator = readableStreamToGenerator(stream);
    expect(await generator.next()).toEqual({ done: false, value: 0 });
  }
  expect(stream.cancelled).toBe(true);
});
