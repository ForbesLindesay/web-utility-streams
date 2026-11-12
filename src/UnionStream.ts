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
 *
 * The optional `queuingStrategy` controls how far ahead of the reader the
 * source streams are read. By default, one chunk is read ahead. With
 * `{ highWaterMark: 0 }`, the sources are only read when the output is read.
 * Because the sources are read in parallel, the output can buffer one more
 * chunk per source than the high water mark allows.
 */
export default class UnionStream<
  const T extends readonly ReadableStream[],
> extends ReadableStream<UnionStreamEvent<T>> {
  constructor(
    streams: T,
    queuingStrategy?: QueuingStrategy<UnionStreamEvent<T>>,
  ) {
    let controller!: ReadableStreamDefaultController<UnionStreamEvent<T>>;
    let isFinished = false;
    // The number of reads that are waiting for a result
    let activeReads = 0;

    const onError = (err: unknown) => {
      activeReads--;
      if (isFinished) return;
      isFinished = true;
      controller.error(err);
      cancelAll(err).catch(() => {
        // Ignore errors here as the other streams may already have been
        // cancelled or errored.
      });
    };

    // The callbacks for each source's reads are created once, rather than for
    // every read, as that has a noticeable cost.
    interface Source {
      readonly reader: ReadableStreamDefaultReader<UnionStreamEvent<T>>;
      readonly onResult: (
        result: ReadableStreamReadResult<UnionStreamEvent<T>>,
      ) => void;
    }
    const sources = streams.map((stream): Source => {
      const source: Source = {
        reader: stream.getReader(),
        onResult: (result) => {
          activeReads--;
          if (isFinished) return;
          if (result.done) {
            if (!idleSources.length && !activeReads) {
              isFinished = true;
              controller.close();
            }
          } else {
            // This source is ready to be read again, the next time this
            // stream is pulled
            idleSources.push(source);
            controller.enqueue(result.value);
          }
        },
      };
      return source;
    });
    let idleSources = sources;

    const cancelAll = async (reason: unknown) => {
      const results = await Promise.allSettled(
        sources.map((source) => source.reader.cancel(reason)),
      );
      for (const result of results) {
        if (result.status === "rejected") throw result.reason;
      }
    };

    super(
      {
        start(c) {
          controller = c;
          if (!sources.length) {
            isFinished = true;
            controller.close();
          }
        },
        // Start reading from all the sources that aren't already being read.
        // This doesn't wait for the reads to complete, as the stream calls
        // `pull` again whenever a chunk is enqueued, if it wants more.
        pull() {
          const readers = idleSources;
          idleSources = [];
          for (const source of readers) {
            activeReads++;
            source.reader.read().then(source.onResult, onError);
          }
        },
        async cancel(reason) {
          if (isFinished) return;
          isFinished = true;
          await cancelAll(reason);
        },
      },
      queuingStrategy,
    );
  }
}
