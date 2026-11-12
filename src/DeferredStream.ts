/**
 * A `ReadableStream` that is available immediately, but whose contents come
 * from a stream that will only be available later, once the promise resolves.
 *
 * - If the promise rejects (or does not resolve to an unlocked
 *   `ReadableStream`), the stream errors with that error when read.
 * - Errors from the underlying stream are forwarded.
 * - Cancelling this stream cancels the underlying stream, waiting for the
 *   promise to resolve first if necessary.
 *
 * Nothing is read from the underlying stream until this stream is read, so
 * backpressure is preserved.
 */
export default class DeferredStream<T> extends ReadableStream<T> {
  constructor(stream: PromiseLike<ReadableStream<T>>) {
    let isCancelled = false;
    let controller!: ReadableStreamDefaultController<T>;
    // Set once the promise resolves, so we don't need to wait for the
    // promise for every chunk
    let reader: ReadableStreamDefaultReader<T> | undefined;
    const readerPromise = Promise.resolve(stream).then(
      (stream) => (reader = stream.getReader()),
    );
    // If this stream is never read or cancelled, nothing else handles the
    // rejection, and we don't want that to crash the application.
    readerPromise.catch(() => {});

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
        // If the promise rejects or the read fails, the returned promise
        // rejects, which errors this stream. Not an `async` function, as the
        // extra promise it creates for every chunk has a noticeable cost.
        pull() {
          if (reader) return reader.read().then(onResult);
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
          await reader.cancel(reason);
        },
      },
      // Don't buffer anything in this stream, only read from the underlying
      // stream when this stream is read.
      { highWaterMark: 0 },
    );
  }
}
