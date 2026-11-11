import { describe, expect, test } from "vitest";
import readableStreamToGenerator from "../readableStreamToGenerator.js";

export async function delay(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}
export class Delay {
  private readonly _ms: number;
  constructor(ms: number) {
    this._ms = ms;
  }
  async wait(): Promise<void> {
    await delay(this._ms);
  }
}

export function readableStream<T>(
  chunks: (T | Delay)[],
  { err, onCancel }: { err?: boolean; onCancel?: () => void } = {},
): ReadableStream<T> {
  let cancelled = false;
  return new ReadableStream<T>({
    async start(controller) {
      for (const chunk of chunks) {
        if (cancelled) return;
        if (chunk instanceof Delay) {
          await chunk.wait();
        } else {
          controller.enqueue(chunk);
          await delay(15); // delay to ensure chunks are delivered separately
        }
      }
      if (cancelled) return;
      if (err) {
        controller.error(new Error("Example error"));
      } else {
        controller.close();
      }
    },
    cancel() {
      cancelled = true;
      if (onCancel) onCancel();
    },
  });
}

export type ReadableStreamResult<T> = { ok: true; value: T } | { ok: false };
export async function readableStreamResults<T>(
  stream: ReadableStream<T>,
): Promise<ReadableStreamResult<T>[]> {
  const results: ReadableStreamResult<T>[] = [];
  try {
    for await (const value of readableStreamToGenerator(stream)) {
      results.push({ ok: true, value });
    }
  } catch {
    results.push({ ok: false });
  }
  return results;
}

export interface TrackedSource<T> {
  stream: ReadableStream<T>;
  state: { cancelled: boolean };
}
/**
 * A source stream (see `readableStream`) that records whether it was cancelled.
 */
export function trackedSource<T>(
  chunks: (T | Delay)[],
  options: { err?: boolean } = {},
): TrackedSource<T> {
  const state = { cancelled: false };
  const stream = readableStream(chunks, {
    ...options,
    onCancel: () => (state.cancelled = true),
  });
  return { stream, state };
}

export interface Destination {
  stream: WritableStream<unknown>;
  written: unknown[];
}
/**
 * A destination stream that records the chunks written to it, and optionally
 * fails when writing the chunk at index `failOnWrite`.
 */
export function destination({
  failOnWrite,
}: { failOnWrite?: number } = {}): Destination {
  const written: unknown[] = [];
  const stream = new WritableStream({
    async write(chunk) {
      if (written.length === failOnWrite) {
        throw new Error("Destination failed");
      }
      written.push(chunk);
      await delay(2);
    },
  });
  return { stream, written };
}

export async function runPipeline(
  promise: Promise<unknown>,
): Promise<{ ok: true } | { ok: false; error: unknown }> {
  try {
    await promise;
    return { ok: true };
  } catch (error) {
    return { ok: false, error };
  }
}

export interface TransformCase {
  name: string;
  create: () => ReadableWritablePair<any, any>;
  /**
   * Input that is processed successfully and produces at least one output chunk.
   */
  validInput: unknown[];
  /**
   * Valid input that produces several output chunks, for tests that stop the
   * pipeline part way through. Defaults to `validInput` repeated twice, which
   * doesn't work for transforms that expect a single document.
   */
  longInput?: unknown[];
  /**
   * Input that causes the transform itself to error, if there is such input.
   */
  invalidInput?: unknown;
}

/**
 * Every transform in this package should be safe to use in a pipeline like:
 *
 *   try {
 *     await source.pipeThrough(transform).pipeTo(destination);
 *   } catch (ex) {
 *     // handle error
 *   }
 *
 * regardless of which part of the pipeline errors or is cancelled. The error
 * should be propagated to the `catch` block, and the source should be
 * cancelled. Unhandled rejections (which would crash a Node.js application)
 * are checked for every test in `testSetup.ts`.
 */
export function testTransformRobustness(c: TransformCase): void {
  const longInput = c.longInput ?? [...c.validInput, ...c.validInput];
  describe(`${c.name} - robustness`, () => {
    test("completes normally", async () => {
      const source = trackedSource(c.validInput);
      const dest = destination();
      const result = await runPipeline(
        source.stream.pipeThrough(c.create()).pipeTo(dest.stream),
      );
      expect(result).toEqual({ ok: true });
      expect(dest.written.length).toBeGreaterThan(0);
    });

    test("source errors", async () => {
      const source = trackedSource(c.validInput, { err: true });
      const dest = destination();
      const result = await runPipeline(
        source.stream.pipeThrough(c.create()).pipeTo(dest.stream),
      );
      expect(result).toMatchObject({
        ok: false,
        error: { message: "Example error" },
      });
    });

    test("destination errors", async () => {
      const source = trackedSource(longInput);
      const dest = destination({ failOnWrite: 1 });
      const result = await runPipeline(
        source.stream.pipeThrough(c.create()).pipeTo(dest.stream),
      );
      expect(result).toMatchObject({
        ok: false,
        error: { message: "Destination failed" },
      });
      await delay(50);
      expect(source.state.cancelled).toBe(true);
    });

    test("pipe is aborted", async () => {
      const source = trackedSource(longInput);
      const dest = destination();
      const controller = new AbortController();
      setTimeout(() => controller.abort(new Error("Aborted")), 20);
      const result = await runPipeline(
        source.stream
          .pipeThrough(c.create())
          .pipeTo(dest.stream, { signal: controller.signal }),
      );
      expect(result).toMatchObject({
        ok: false,
        error: { message: "Aborted" },
      });
      await delay(50);
      expect(source.state.cancelled).toBe(true);
    });

    test("consumer cancels the readable side", async () => {
      const source = trackedSource(longInput);
      const iterator = readableStreamToGenerator(
        source.stream.pipeThrough(c.create()),
      );
      await iterator.next();
      await iterator.return();
      await delay(50);
      expect(source.state.cancelled).toBe(true);
    });

    test("a later transform errors", async () => {
      const source = trackedSource(longInput);
      const dest = destination();
      let count = 0;
      const result = await runPipeline(
        source.stream
          .pipeThrough(c.create())
          .pipeThrough(
            new TransformStream({
              transform() {
                if (count++ === 1) throw new Error("Later transform failed");
              },
            }),
          )
          .pipeTo(dest.stream),
      );
      expect(result).toMatchObject({
        ok: false,
        error: { message: "Later transform failed" },
      });
      await delay(50);
      expect(source.state.cancelled).toBe(true);
    });

    if ("invalidInput" in c) {
      test("the transform errors", async () => {
        const source = trackedSource([
          ...c.validInput,
          c.invalidInput,
          ...c.validInput,
        ]);
        const dest = destination();
        const result = await runPipeline(
          source.stream.pipeThrough(c.create()).pipeTo(dest.stream),
        );
        expect(result.ok).toBe(false);
        await delay(50);
        expect(source.state.cancelled).toBe(true);
      });
    }
  });
}
