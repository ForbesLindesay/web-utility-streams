import isPromiseLike from "./internal/isPromiseLike.js";

/**
 * Transform each chunk using a (sync or async) function, dropping any chunks
 * for which the function returns `undefined`. Chunks are processed one at a
 * time, in order. If `fn` throws, the stream is errored.
 */
export default class FilterMapStream<TInput, TOutput> extends TransformStream<
  TInput,
  TOutput
> {
  constructor(
    fn: (
      data: TInput,
    ) => TOutput | undefined | PromiseLike<TOutput | undefined>,
  ) {
    super({
      // If `fn` throws (or its promise rejects), or `controller.enqueue`
      // throws because the readable side has been cancelled, the
      // TransformStream is errored.
      transform(chunk, controller) {
        const result = fn(chunk);
        // Only wait when we need to, as waiting for a promise for every chunk
        // is much slower when `fn` is synchronous.
        if (isPromiseLike(result)) {
          return Promise.resolve(result).then((value) => {
            if (value !== undefined) controller.enqueue(value);
          });
        }
        if (result !== undefined) controller.enqueue(result);
      },
    });
  }
}
