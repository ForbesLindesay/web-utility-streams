import TransformStreamWithCancel, {
  type TransformerWithCancel,
} from "./TransformStreamWithCancel.js";

/**
 * Choose the timeout, in milliseconds, each time a chunk is written.
 *
 * @param remainingMs - How long was left of the previous timeout, or 0 when
 *   choosing the first timeout, which starts when the stream is created.
 * @param chunk - The chunk that was just written, or `undefined` when choosing
 *   the first timeout.
 */
export type TimeoutGetter<TChunk> = (
  remainingMs: number,
  chunk?: TChunk,
) => number;

export interface TimeoutStreamOptions<TChunk> {
  /**
   * How long to wait for the next chunk, in milliseconds, or a function that
   * chooses it each time a chunk is written.
   */
  timeoutMs: number | TimeoutGetter<TChunk>;
  /**
   * Called when the timeout is reached. By default, the stream is errored with
   * a `TimeoutStreamError`. You could instead error it with your own error,
   * end it with `controller.terminate()`, or even enqueue a chunk. If the
   * stream isn't errored or terminated, the timeout starts again when the
   * next chunk is written. If this function throws, the stream is errored.
   */
  onTimeout?: (controller: TransformStreamDefaultController<TChunk>) => void;
}

/**
 * The error a `TimeoutStream` is errored with by default
 */
export class TimeoutStreamError extends Error {
  public readonly code = "TIMEOUT_STREAM_TIMEOUT";
  constructor(timeoutMs: number) {
    super(
      `TimeoutStream: no chunk was received for ${Math.round(timeoutMs)}ms.`,
    );
    this.name = "TimeoutStreamError";
  }
}

/**
 * Pass chunks through unchanged, erroring the stream if no chunk is written
 * for `timeoutMs` milliseconds. The timeout starts when the stream is created,
 * and is stopped when the stream finishes.
 *
 * Chunks are only written when the output has room for them, so if the output
 * is read slowly (e.g. a slow destination in a pipeline), that can cause a
 * timeout too.
 */
export default class TimeoutStream<TChunk> extends TransformStreamWithCancel<
  TChunk,
  TChunk
> {
  constructor({ timeoutMs, onTimeout }: TimeoutStreamOptions<TChunk>) {
    const getTimeout = typeof timeoutMs === "function" ? timeoutMs : undefined;
    const fixedTimeoutMs = getTimeout ? 0 : (timeoutMs as number);
    if (!getTimeout) checkTimeout(fixedTimeoutMs);

    let controller!: TransformStreamDefaultController<TChunk>;
    let timer: ReturnType<typeof setTimeout> | undefined;
    // When the timeout is reached. Restarting a timer for each chunk is
    // slow, so each chunk just moves the deadline, and when the timer fires
    // it is restarted if the deadline has moved.
    let deadline = 0;
    // When the timer is due to fire
    let timerDue = 0;
    // The length of the current timeout
    let currentTimeoutMs = 0;

    const startTimer = (now: number) => {
      timerDue = deadline;
      timer = setTimeout(onTimer, deadline - now);
    };
    const onTimer = () => {
      const now = performance.now();
      if (deadline > now) return startTimer(now);
      timer = undefined;
      try {
        if (onTimeout) onTimeout(controller);
        else controller.error(new TimeoutStreamError(currentTimeoutMs));
      } catch (err) {
        controller.error(err);
      }
    };
    const stopTimer = () => {
      clearTimeout(timer);
      timer = undefined;
      // Stop the timer being restarted, e.g. by a write that is in progress
      deadline = Infinity;
    };

    super(
      {
        start(c) {
          controller = c;
          const now = performance.now();
          currentTimeoutMs = getTimeout
            ? checkTimeout(getTimeout(0))
            : fixedTimeoutMs;
          deadline = now + currentTimeoutMs;
          startTimer(now);
        },
        transform: getTimeout
          ? (chunk, controller) => {
              if (deadline !== Infinity) {
                const now = performance.now();
                try {
                  currentTimeoutMs = checkTimeout(
                    getTimeout(Math.max(0, deadline - now), chunk),
                  );
                } catch (err) {
                  // The stream is errored, so stop the timer
                  stopTimer();
                  throw err;
                }
                deadline = now + currentTimeoutMs;
                // The timeout might be shorter than the remaining time, in
                // which case the timer needs to fire sooner
                if (timer === undefined) {
                  startTimer(now);
                } else if (deadline < timerDue) {
                  clearTimeout(timer);
                  startTimer(now);
                }
              }
              controller.enqueue(chunk);
            }
          : (chunk, controller) => {
              if (deadline !== Infinity) {
                const now = performance.now();
                deadline = now + fixedTimeoutMs;
                // Restart the timeout after `onTimeout` didn't end the stream
                if (timer === undefined) startTimer(now);
              }
              controller.enqueue(chunk);
            },
        flush: stopTimer,
        cancel: stopTimer,
      },
      undefined,
      // Read one chunk ahead of the output, which is faster, and means a slow
      // reader has to fall behind by a chunk before it can cause a timeout
      { highWaterMark: 1 },
    );
  }
}

function checkTimeout(timeoutMs: number): number {
  if (!(timeoutMs >= 0)) {
    throw new RangeError(
      `TimeoutStream: timeoutMs must be a number >= 0, but got ${timeoutMs}`,
    );
  }
  return timeoutMs;
}
