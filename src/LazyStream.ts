/**
 * A `ReadableStream` that only calls `getStream` to create its underlying
 * stream when it is first read. `getStream` can return a `ReadableStream`, or
 * a promise for one.
 *
 * - If `getStream` throws, or its promise rejects (or does not resolve to an
 *   unlocked `ReadableStream`), the stream errors with that error.
 * - Errors from the underlying stream are forwarded.
 * - Cancelling this stream before it is read does not call `getStream`.
 *   Cancelling it later cancels the underlying stream, waiting for the promise
 *   to resolve first if necessary.
 *
 * Nothing is read from the underlying stream until this stream is read, so
 * backpressure is preserved.
 */
export default class LazyStream<T> extends ReadableStream<T> {
  constructor(
    getStream: () => ReadableStream<T> | PromiseLike<ReadableStream<T>>,
  ) {
    let isCancelled = false;
    let controller!: ReadableStreamDefaultController<T>;
    let readerPromise: Promise<ReadableStreamDefaultReader<T>> | undefined;
    // Set once the stream is available, so we don't need to wait for the
    // promise for every chunk
    let reader: ReadableStreamDefaultReader<T> | undefined;

    const onResult = (result: ReadableStreamReadResult<T>) => {
      if (isCancelled) return;
      if (result.done) controller.close();
      else controller.enqueue(result.value);
    };

    super(
      {
        start(c) {
          controller = c;
        },
        // If `getStream` throws, its promise rejects or the read fails, the
        // returned promise rejects, which errors this stream. Not an `async`
        // function, as the extra promise it creates for every chunk has a
        // noticeable cost.
        pull() {
          if (reader) return reader.read().then(onResult);
          readerPromise ??= Promise.resolve(getStream()).then(
            (stream) => (reader = stream.getReader()),
          );
          return readerPromise.then((reader) => {
            if (isCancelled) return;
            return reader.read().then(onResult);
          });
        },
        async cancel(reason) {
          isCancelled = true;
          let reader;
          try {
            reader = await readerPromise;
          } catch {
            // There is no underlying stream to cancel
            return;
          }
          await reader?.cancel(reason);
        },
      },
      // Don't buffer anything in this stream, only read from the underlying
      // stream when this stream is read.
      { highWaterMark: 0 },
    );
  }
}
