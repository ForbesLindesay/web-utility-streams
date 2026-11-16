export interface DebounceStreamOptions {
  debounceMs: number;
  /**
   * - `"LEADING"`: emit the first chunk straight away, then ignore chunks for
   *   `debounceMs`.
   * - `"TRAILING"`: wait `debounceMs` after the first chunk, then emit the
   *   most recent chunk.
   * - `"BOTH"` (the default): emit the first chunk straight away, and then
   *   the most recent chunk at the end of the period, if there was another.
   */
  mode?: "LEADING" | "TRAILING" | "BOTH";
  /**
   * Restart the timer on each chunk. When paired with "TRAILING", this ensures
   * that a chunk is only emitted when no different values have been received
   * for at least debounceMs.
   *
   * @default false
   */
  restartTimerOnChunk?: boolean;
}

/**
 * Emit at most one event every `debounceMs` milliseconds, dropping the rest.
 *
 * - When a chunk is written and nothing has been emitted in the last
 *   `debounceMs`, a period starts. In `"LEADING"` and `"BOTH"` modes, the
 *   chunk is emitted straight away.
 * - Chunks written during the period are dropped, except that in
 *   `"TRAILING"` and `"BOTH"` modes, the most recent one is emitted at the
 *   end of the period, which starts a new period.
 * - When the writable side is closed, a chunk waiting for the end of the
 *   period is emitted straight away, and then the output closes.
 * - Writes never wait for the output to be read, so the source is never
 *   slowed down. If the output isn't read before the next chunk is emitted,
 *   only the most recent one is kept.
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
    let isFinished = false;

    // Set while a period is in progress
    let timer: ReturnType<typeof setTimeout> | undefined;
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

    const endPeriod = () => {
      timer = undefined;
      if (!hasTrailing) return;
      const chunk = trailingChunk!;
      hasTrailing = false;
      trailingChunk = undefined;
      emit(chunk);
      timer = setTimeout(endPeriod, debounceMs);
    };

    const finish = () => {
      isFinished = true;
      clearTimeout(timer);
      timer = undefined;
      hasTrailing = false;
      trailingChunk = undefined;
      hasOutput = false;
      outputChunk = undefined;
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
          if (isFinished) return;
          finish();
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
        if (isFinished) return;
        if (timer !== undefined) {
          if (restartTimerOnChunk) {
            clearTimeout(timer);
            timer = setTimeout(endPeriod, debounceMs);
          }
          if (trailing) {
            hasTrailing = true;
            trailingChunk = chunk;
          }
          return;
        }
        if (leading) {
          emit(chunk);
        } else {
          hasTrailing = true;
          trailingChunk = chunk;
        }
        timer = setTimeout(endPeriod, debounceMs);
      },
      close() {
        if (isFinished) return;
        if (hasTrailing) emit(trailingChunk!);
        const hasUnreadOutput = hasOutput;
        const chunk = outputChunk!;
        finish();
        if (hasUnreadOutput) readableController.enqueue(chunk);
        readableController.close();
      },
      abort(reason) {
        if (isFinished) return;
        finish();
        readableController.error(reason);
      },
    });
  }
}
