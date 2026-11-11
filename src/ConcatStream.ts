export type ConcatStreamEvent<T extends readonly ReadableStream[]> =
  T[number] extends ReadableStream<infer U> ? U : never;

/**
 * Join multiple source streams into a single output stream, by reading all
 * events from the first stream, then all events from the second stream, and
 * so on.
 *
 * All the source streams are locked immediately, but each one is only read
 * once all the streams before it have closed. Backpressure is handled, so if
 * you pause reading from the output, we will also pause reading from the
 * current source stream.
 *
 * The output closes once the last source stream has closed. If any source
 * stream errors, the output is errored and all other source streams are
 * cancelled. If the output is cancelled, all source streams are cancelled.
 */
export default class ConcatStream<
  const T extends readonly ReadableStream[],
> extends ReadableStream<ConcatStreamEvent<T>> {
  constructor(...streams: T) {
    const remainingReaders = streams
      .map((stream) => stream.getReader())
      .reverse();
    let currentReader = remainingReaders.pop();
    let isFinished = false;

    const cancelRemainingReaders = async (reason: unknown) => {
      const results = await Promise.allSettled(
        remainingReaders.map((reader) => reader.cancel(reason)),
      );
      for (const result of results) {
        if (result.status === "rejected") throw result.reason;
      }
    };

    super({
      async pull(controller) {
        while (currentReader) {
          let result;
          try {
            result = await currentReader.read();
          } catch (err) {
            if (isFinished) return;
            isFinished = true;
            controller.error(err);
            cancelRemainingReaders(err).catch(() => {
              // Ignore errors here as the other streams may already
              // have been cancelled or errored.
            });
            return;
          }
          if (isFinished) return;
          if (!result.done) {
            controller.enqueue(result.value);
            return;
          }
          // The current stream has closed, so move on to the next one
          currentReader = remainingReaders.pop();
        }
        isFinished = true;
        controller.close();
      },
      async cancel(reason) {
        if (isFinished) return;
        isFinished = true;
        if (currentReader) remainingReaders.push(currentReader);
        await cancelRemainingReaders(reason);
      },
    });
  }
}
