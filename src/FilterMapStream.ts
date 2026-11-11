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
      async transform(chunk, controller) {
        // If `fn` throws, or `controller.enqueue` throws because the readable
        // side has been cancelled, the returned promise rejects, which errors
        // the TransformStream.
        const result = await fn(chunk);
        if (result === undefined) return;
        controller.enqueue(result);
      },
    });
  }
}
