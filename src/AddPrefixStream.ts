export type AddPrefixStreamPrefix<TChunk> =
  | AsyncIterable<TChunk>
  | Iterable<TChunk>
  | PromiseLike<AsyncIterable<TChunk> | Iterable<TChunk>>;

const noop = () => {};

/**
 * Pass every chunk through unchanged, after first emitting the chunks of
 * `prefix`. The prefix can be an iterable, an async iterable, or a promise
 * for either.
 *
 * - The prefix is read one chunk at a time, as the output is read, so
 *   backpressure is preserved. Chunks written to this stream wait until the
 *   whole prefix has been emitted. After that, up to 4 chunks are read ahead
 *   of the output, which makes passing them through much faster.
 * - If the promise rejects, or the prefix throws while it is being read, the
 *   stream is errored, so the source of a pipeline is cancelled.
 * - If the writable side is aborted (e.g. the source of a pipeline errors),
 *   the output is errored immediately, even if the prefix hasn't finished.
 * - If the output is cancelled, or the stream errors, before the prefix has
 *   finished, the prefix's iterator is closed (by calling its `return`
 *   method).
 */
export default class AddPrefixStream<TChunk> implements TransformStream<
  TChunk,
  TChunk
> {
  public readonly readable: ReadableStream<TChunk>;
  public readonly writable: WritableStream<TChunk>;

  constructor(prefix: AddPrefixStreamPrefix<TChunk>) {
    // Both sides are implemented directly, rather than with a
    // `TransformStream`, because:
    //
    // - A `TransformStream`'s writable side can't be aborted while a write is
    //   waiting for backpressure, so a source error wouldn't reach the output
    //   until the whole prefix had been read.
    // - Passing each chunk straight from the writable side to the readable
    //   side is faster.

    let readableController!: ReadableStreamDefaultController<TChunk>;
    let writableController!: WritableStreamDefaultController;
    // Set once either side has closed, errored or been cancelled
    let isFinished = false;

    // The prefix's iterator, until the whole prefix has been read
    let iterator: Iterator<TChunk> | AsyncIterator<TChunk> | undefined;
    let isAsyncIterator = false;
    let isPrefixDone = false;

    // A chunk that has been written, but is waiting for space in the
    // readable side's queue
    let pendingWrite:
      | { chunk: TChunk; resolve: () => void; reject: (err: unknown) => void }
      | undefined;
    let isWritableClosed = false;

    // Stop reading the prefix and the writable side
    const finish = async (reason: unknown): Promise<void> => {
      isFinished = true;
      pendingWrite?.reject(reason);
      await iterator?.return?.(reason);
    };

    // Error both sides. Erroring the writable side cancels the source of a
    // pipeline.
    const fail = (err: unknown): void => {
      if (isFinished) return;
      readableController.error(err);
      writableController.error(err);
      finish(err).catch(noop);
    };

    // The writable side has been aborted, e.g. because the source of a
    // pipeline errored
    const abort = (reason: unknown): void => {
      if (isFinished) return;
      readableController.error(reason);
      finish(reason).catch(noop);
    };

    // Pass on a chunk that has been written, or close the readable side if
    // the writable side has closed
    const pullWritten = (): void => {
      if (pendingWrite) {
        const { chunk, resolve } = pendingWrite;
        pendingWrite = undefined;
        readableController.enqueue(chunk);
        resolve();
      } else if (isWritableClosed) {
        isFinished = true;
        readableController.close();
      }
      // Otherwise, the next chunk written will be enqueued straight away
    };

    const onPrefixResult = (result: IteratorResult<TChunk>): void => {
      if (isFinished) return;
      if (!result.done) {
        readableController.enqueue(result.value);
        return;
      }
      iterator = undefined;
      isPrefixDone = true;
      pullWritten();
    };

    this.writable = new WritableStream<TChunk>({
      start(controller) {
        writableController = controller;
        // The signal is aborted as soon as `abort` is called, even if a
        // write is waiting for the prefix (when the `abort` callback would
        // only be called once that write had finished).
        controller.signal?.addEventListener("abort", () =>
          abort(controller.signal.reason),
        );
      },
      write(chunk) {
        // Once the prefix is done, a chunk goes straight into the readable
        // side's queue, as long as there are at most two chunks already in it
        // (with a highWaterMark of 0, `desiredSize` is minus the number of
        // queued chunks). Letting a few chunks wait in the queue means the
        // next chunk is usually ready before the output is read again, which
        // makes passing chunks through much faster. In benchmarks, allowing
        // 3 chunks is about 35% faster than allowing 2, but allowing more
        // makes no difference. In total, this reads at most 4 chunks ahead
        // of the output.
        if (isPrefixDone && readableController.desiredSize! >= -2) {
          readableController.enqueue(chunk);
          return;
        }
        return new Promise<void>((resolve, reject) => {
          pendingWrite = { chunk, resolve, reject };
        });
      },
      close() {
        isWritableClosed = true;
        // `close` is only called once every write has finished, so if the
        // prefix is done, every chunk has been enqueued. Otherwise, the
        // readable side is closed once the prefix is done.
        if (isPrefixDone && !isFinished) {
          isFinished = true;
          readableController.close();
        }
      },
      // For environments that don't support `controller.signal`
      abort,
    });

    this.readable = new ReadableStream<TChunk>(
      {
        async start(controller) {
          readableController = controller;
          try {
            const iterable = await prefix;
            // Not using `in`, as that throws for strings
            isAsyncIterator =
              typeof (iterable as AsyncIterable<TChunk>)[
                Symbol.asyncIterator
              ] === "function";
            iterator = isAsyncIterator
              ? (iterable as AsyncIterable<TChunk>)[Symbol.asyncIterator]()
              : (iterable as Iterable<TChunk>)[Symbol.iterator]();
            // If we were cancelled while waiting, close the iterator
            if (isFinished) return await Promise.resolve(iterator.return?.());
          } catch (ex) {
            fail(ex);
          }
        },
        pull() {
          if (isPrefixDone) return pullWritten();
          if (isAsyncIterator) {
            return (iterator as AsyncIterator<TChunk>)
              .next()
              .then(onPrefixResult, fail);
          }
          let result;
          try {
            result = (iterator as Iterator<TChunk>).next();
          } catch (ex) {
            return fail(ex);
          }
          onPrefixResult(result);
        },
        cancel(reason) {
          if (isFinished) return;
          writableController.error(reason);
          return finish(reason);
        },
      },
      // Only read from the prefix when the output is read
      { highWaterMark: 0 },
    );
  }
}
