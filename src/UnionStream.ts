export type UnionStreamEvent<T extends readonly ReadableStream[]> =
  T[number] extends ReadableStream<infer U> ? U : never;

/**
 * Merge events from multiple source streams into a single output stream.
 * Whenever reading, always read from all streams in parallel.
 *
 * Backpressure is handled, so if you pause reading from the output, we
 * will also pause reading from the source streams.
 *
 * The output closes once all source streams have closed. If any source stream
 * errors, the output is errored and all other source streams are cancelled.
 * If the output is cancelled, all source streams are cancelled.
 */
export default class UnionStream<
  const T extends readonly ReadableStream[],
> extends ReadableStream<UnionStreamEvent<T>> {
  constructor(...streams: T) {
    const allReaders = streams.map((stream) => stream.getReader());
    const activeReads = new Set<Promise<void>>();
    let idleReaders = allReaders;
    let isFinished = false;

    const cancelAll = async (reason: unknown) => {
      const results = await Promise.allSettled(
        allReaders.map((reader) => reader.cancel(reason)),
      );
      for (const result of results) {
        if (result.status === "rejected") throw result.reason;
      }
    };

    super({
      start(controller) {
        if (!allReaders.length) {
          isFinished = true;
          controller.close();
        }
      },
      async pull(controller) {
        const readers = idleReaders;
        idleReaders = [];
        for (const reader of readers) {
          const read: Promise<void> = reader.read().then(
            ({ done, value }) => {
              activeReads.delete(read);
              if (isFinished) return;
              if (done) {
                if (!idleReaders.length && !activeReads.size) {
                  isFinished = true;
                  controller.close();
                }
              } else {
                idleReaders.push(reader);
                controller.enqueue(value);
              }
            },
            (err) => {
              activeReads.delete(read);
              if (isFinished) return;
              isFinished = true;
              controller.error(err);
              cancelAll(err).catch(() => {
                // Ignore errors here as the other streams may already
                // have been cancelled or errored.
              });
            },
          );
          activeReads.add(read);
        }
        if (activeReads.size) {
          await Promise.race(activeReads);
        }
      },
      async cancel(reason) {
        if (isFinished) return;
        isFinished = true;
        await cancelAll(reason);
      },
    });
  }
}
