import isPromiseLike from "./internal/isPromiseLike.js";

/**
 * A `Transformer`, as passed to `new TransformStream()`, including the
 * `cancel` method, which TypeScript's DOM types don't include yet.
 */
export interface TransformerWithCancel<I = any, O = any> {
  start?: (controller: TransformStreamDefaultController<O>) => any;
  transform?: (
    chunk: I,
    controller: TransformStreamDefaultController<O>,
  ) => void | PromiseLike<void>;
  flush?: (
    controller: TransformStreamDefaultController<O>,
  ) => void | PromiseLike<void>;
  /**
   * Called when the readable side is cancelled, or the writable side is
   * aborted, with the reason.
   */
  cancel?: (reason: any) => void | PromiseLike<void>;
  readableType?: undefined;
  writableType?: undefined;
}

export interface TransformStreamWithCancelConstructor {
  new <I = any, O = any>(
    transformer?: TransformerWithCancel<I, O>,
    writableStrategy?: QueuingStrategy<I>,
    readableStrategy?: QueuingStrategy<O>,
  ): TransformStream<I, O>;
}

/**
 * Whether this runtime's `TransformStream` calls the transformer's `cancel`
 * method. The constructor only reads the transformer properties it knows
 * about, so we can check without creating any streams other than this one.
 */
export const supportsTransformerCancel: boolean = (() => {
  let supported = false;
  try {
    new TransformStream({
      get cancel() {
        supported = true;
        return undefined;
      },
    } as Transformer);
  } catch {
    // Treat an unexpected error as not supported, and use the polyfill
  }
  return supported;
})();

const resolved: Promise<void> = Promise.resolve();

/**
 * A `TransformStream` that calls the transformer's `cancel` method, built
 * from a `ReadableStream` and a `WritableStream`, following the algorithms in
 * the Streams standard. `TransformStreamWithCancel` is this class in runtimes
 * whose `TransformStream` doesn't call `cancel`. Prefer using
 * `TransformStreamWithCancel`, unless you specifically need the polyfill.
 *
 * Unlike a native `TransformStream`, `instanceof TransformStream` is false.
 */
export class TransformStreamWithCancelPolyfill<
  I = any,
  O = any,
> implements TransformStream<I, O> {
  public readonly readable: ReadableStream<O>;
  public readonly writable: WritableStream<I>;

  constructor(
    transformer: TransformerWithCancel<I, O> = {},
    writableStrategy: QueuingStrategy<I> = {},
    readableStrategy: QueuingStrategy<O> = {},
  ) {
    if (transformer.readableType !== undefined) {
      throw new RangeError("Invalid readableType specified");
    }
    if (transformer.writableType !== undefined) {
      throw new RangeError("Invalid writableType specified");
    }
    const { start, transform, flush, cancel } = transformer;

    let readableController!: ReadableStreamDefaultController<O>;
    let writableController!: WritableStreamDefaultController;

    // The state of each side, as far as we need to know it. Every change to
    // either side's state happens through this class, except aborting the
    // writable side, which is visible through `writableController.signal`.
    // Whether the readable side can still be closed or enqueued to
    let readableOpen = true;
    let readableCancelled = false;
    let readableErrored = false;
    let readableError: unknown;
    let writableErrored = false;
    let writableError: unknown;

    // Whether the readable side's queue is full, so writes must wait until
    // it's read from. The standard creates a promise each time this changes,
    // but that's slow, so we only create one when a write has to wait.
    let backpressure = true;
    let releaseWrite: (() => void) | undefined;
    const releaseBackpressure = () => {
      backpressure = false;
      if (releaseWrite !== undefined) {
        const release = releaseWrite;
        releaseWrite = undefined;
        release();
      }
    };

    // Set once the stream is finishing (closing, cancelling or aborting), so
    // `flush` and `cancel` are called at most once between them
    let finishPromise: Promise<void> | undefined;

    const errorReadable = (reason: unknown) => {
      if (readableErrored || readableCancelled) return;
      readableOpen = false;
      // This does nothing if the readable side has closed (after a close was
      // requested, and its queue has been read), which we can't otherwise
      // tell apart from a close that has been requested
      readableController.error(reason);
      if (readableController.desiredSize === null) {
        readableErrored = true;
        readableError = reason;
      }
    };
    const errorWritableAndUnblockWrite = (reason: unknown) => {
      if (!writableErrored) {
        writableErrored = true;
        writableError = reason;
        writableController.error(reason);
      }
      if (backpressure) releaseBackpressure();
    };
    const errorStream = (reason: unknown) => {
      errorReadable(reason);
      errorWritableAndUnblockWrite(reason);
    };

    const controller: TransformStreamDefaultController<O> = {
      get desiredSize() {
        return readableController.desiredSize;
      },
      enqueue(chunk?: O) {
        if (!readableOpen) {
          throw new TypeError(
            "Readable side is not in a state that permits enqueue",
          );
        }
        try {
          readableController.enqueue(chunk!);
        } catch (err) {
          errorWritableAndUnblockWrite(err);
          throw err;
        }
        if (!(readableController.desiredSize! > 0)) backpressure = true;
      },
      error(reason?: unknown) {
        errorStream(reason);
      },
      terminate() {
        if (readableOpen) {
          readableOpen = false;
          readableController.close();
        }
        errorWritableAndUnblockWrite(
          new TypeError("TransformStream terminated"),
        );
      },
    };

    const performTransform: (chunk: I) => void | Promise<void> = transform
      ? (chunk) => {
          try {
            const result = transform.call(transformer, chunk, controller);
            if (isPromiseLike(result)) {
              return Promise.resolve(result).then(undefined, (err) => {
                errorStream(err);
                throw err;
              });
            }
          } catch (err) {
            errorStream(err);
            throw err;
          }
        }
      : (chunk) => {
          controller.enqueue(chunk as unknown as O);
        };

    // Call `flush` or `cancel` (if there is one), as a promise
    const call = (
      fn: ((arg: any) => void | PromiseLike<void>) | undefined,
      arg: unknown,
    ): Promise<void> => {
      if (fn === undefined) return resolved;
      try {
        return Promise.resolve(fn.call(transformer, arg));
      } catch (err) {
        return Promise.reject(err);
      }
    };

    let resolveStart!: (value: unknown) => void;
    const startPromise = new Promise((resolve) => (resolveStart = resolve));

    this.writable = new WritableStream<I>(
      {
        start(c) {
          writableController = c;
          return startPromise;
        },
        write(chunk) {
          if (!backpressure) return performTransform(chunk);
          return new Promise<void>((resolve) => (releaseWrite = resolve)).then(
            () => {
              if (writableErrored) throw writableError;
              const signal = writableController.signal;
              if (signal?.aborted) throw signal.reason;
              return performTransform(chunk);
            },
          );
        },
        close() {
          if (finishPromise) return finishPromise;
          return (finishPromise = call(flush, controller).then(
            () => {
              if (readableErrored) throw readableError;
              if (readableOpen) {
                readableOpen = false;
                readableController.close();
              }
            },
            (err) => {
              errorReadable(err);
              throw err;
            },
          ));
        },
        abort(reason) {
          if (finishPromise) return finishPromise;
          writableErrored = true;
          writableError = reason;
          return (finishPromise = call(cancel, reason).then(
            () => {
              if (readableErrored) throw readableError;
              errorReadable(reason);
            },
            (err) => {
              errorReadable(err);
              throw err;
            },
          ));
        },
      },
      writableStrategy,
    );

    this.readable = new ReadableStream<O>(
      {
        start(c) {
          readableController = c;
          return startPromise;
        },
        pull() {
          // The readable side only calls `pull` when it wants more chunks.
          // Returning nothing (rather than a promise for the next change in
          // backpressure, like the standard) means it may call `pull` more
          // than once, which is harmless.
          releaseBackpressure();
        },
        cancel(reason) {
          readableOpen = false;
          readableCancelled = true;
          if (finishPromise) return finishPromise;
          return (finishPromise = call(cancel, reason).then(
            () => {
              if (writableErrored) throw writableError;
              errorWritableAndUnblockWrite(reason);
            },
            (err) => {
              errorWritableAndUnblockWrite(err);
              throw err;
            },
          ));
        },
      },
      {
        highWaterMark: readableStrategy.highWaterMark ?? 0,
        size: readableStrategy.size,
      },
    );

    resolveStart(start?.call(transformer, controller));
  }
}

/**
 * A `TransformStream` that calls the transformer's `cancel` method when the
 * readable side is cancelled or the writable side is aborted, even in
 * runtimes whose `TransformStream` doesn't support `cancel` yet. In runtimes
 * that do, this is just `TransformStream`.
 */
const TransformStreamWithCancel: TransformStreamWithCancelConstructor =
  supportsTransformerCancel
    ? TransformStream
    : TransformStreamWithCancelPolyfill;
type TransformStreamWithCancel<I = any, O = any> = TransformStream<I, O>;
export default TransformStreamWithCancel;
