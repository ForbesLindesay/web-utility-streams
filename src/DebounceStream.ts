export interface DebounceStreamOptions {
  debounceMs: number;
  /**
   * Which chunks of each period to emit:
   * - `"BOTH"` (the default): the first chunk straight away, and the most
   *   recent chunk at the end of the period, if there was another.
   * - `"LEADING"`: only the first chunk, straight away.
   * - `"TRAILING"`: only the most recent chunk, at the end of the period.
   */
  mode?: "LEADING" | "TRAILING" | "BOTH";
  /**
   * Extend the period each time a chunk is written, so it only ends once no
   * chunks have been written for `debounceMs`.
   *
   * @default false
   */
  restartTimerOnChunk?: boolean;
}

/**
 * Emit at most one chunk every `debounceMs` milliseconds, dropping the rest.
 *
 * - A period starts when a chunk is written and nothing has been emitted in
 *   the last `debounceMs`. Emitting a trailing chunk starts a new period.
 * - When the writable side closes, a trailing chunk that is waiting is
 *   emitted straight away.
 * - Writes never wait for the output to be read. If the output isn't read
 *   before the next chunk is emitted, only the most recent one is kept.
 */
export default class DebounceStream<TChunk> implements TransformStream<
  TChunk,
  TChunk
> {
  public readonly readable: ReadableStream<TChunk>;
  public readonly writable: WritableStream<TChunk>;

  constructor({
    debounceMs,
    mode = "BOTH",
    restartTimerOnChunk = false,
  }: DebounceStreamOptions) {
    if (!(debounceMs >= 0)) {
      throw new RangeError(
        `debounceMs must be a number >= 0, but got ${debounceMs}`,
      );
    }
    if (mode !== "LEADING" && mode !== "TRAILING" && mode !== "BOTH") {
      throw new TypeError(
        `mode must be "LEADING", "TRAILING" or "BOTH", but got ${mode}`,
      );
    }
    const leading = mode !== "TRAILING";
    const trailing = mode !== "LEADING";

    let readableController!: ReadableStreamDefaultController<TChunk>;
    let writableController!: WritableStreamDefaultController;

    // Set while a period is in progress
    let timer: ReturnType<typeof setTimeout> | undefined;
    // With `restartTimerOnChunk`, when the period ends. Restarting the timer
    // on every chunk is slow, so instead the timer is restarted when it fires
    // if the period has been extended.
    let periodEnd = 0;
    // The most recent chunk written during the period, to emit at the end
    let hasTrailing = false;
    let trailingChunk: TChunk | undefined;

    // An emitted chunk that is waiting for the output to be read
    let hasOutput = false;
    let outputChunk: TChunk | undefined;
    // Whether the output is being read, and is waiting for a chunk
    let isReadWaiting = false;

    const emit = (chunk: TChunk) => {
      if (isReadWaiting) {
        isReadWaiting = false;
        readableController.enqueue(chunk);
      } else {
        // Replaces any older chunk that hasn't been read yet
        hasOutput = true;
        outputChunk = chunk;
      }
    };

    const startPeriod = () => {
      if (restartTimerOnChunk) periodEnd = performance.now() + debounceMs;
      timer = setTimeout(endPeriod, debounceMs);
    };

    const endPeriod = () => {
      if (restartTimerOnChunk) {
        const remaining = periodEnd - performance.now();
        if (remaining > 0) {
          timer = setTimeout(endPeriod, remaining);
          return;
        }
      }
      timer = undefined;
      if (!hasTrailing) return;
      const chunk = trailingChunk!;
      hasTrailing = false;
      trailingChunk = undefined;
      emit(chunk);
      startPeriod();
    };

    this.readable = new ReadableStream<TChunk>(
      {
        start(c) {
          readableController = c;
        },
        pull() {
          if (hasOutput) {
            const chunk = outputChunk!;
            hasOutput = false;
            outputChunk = undefined;
            readableController.enqueue(chunk);
          } else {
            isReadWaiting = true;
          }
        },
        cancel(reason) {
          clearTimeout(timer);
          // Error the writable side, so the source of a pipeline is cancelled
          writableController.error(reason);
        },
      },
      // Chunks are only enqueued when the output is read, so we can replace
      // chunks that haven't been read yet
      { highWaterMark: 0 },
    );

    this.writable = new WritableStream<TChunk>({
      start(c) {
        writableController = c;
      },
      write(chunk) {
        if (timer === undefined) {
          startPeriod();
          if (leading) return emit(chunk);
        } else if (restartTimerOnChunk) {
          periodEnd = performance.now() + debounceMs;
        }
        if (trailing) {
          hasTrailing = true;
          trailingChunk = chunk;
        }
      },
      close() {
        if (hasTrailing) emit(trailingChunk!);
        if (hasOutput) readableController.enqueue(outputChunk!);
        clearTimeout(timer);
        readableController.close();
      },
      abort(reason) {
        clearTimeout(timer);
        readableController.error(reason);
      },
    });
  }
}
