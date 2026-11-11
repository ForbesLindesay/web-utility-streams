/**
 * Transform each chunk using a (sync or async) function. Chunks are processed
 * one at a time, in order. If `fn` throws, the stream is errored.
 */
export default class MapStream<TInput, TOutput> extends TransformStream<
  TInput,
  TOutput
> {
  constructor(fn: (data: TInput) => TOutput | PromiseLike<TOutput>) {
    super({
      async transform(chunk, controller) {
        // If `fn` throws, or `controller.enqueue` throws because the readable
        // side has been cancelled, the returned promise rejects, which errors
        // the TransformStream.
        controller.enqueue(await fn(chunk));
      },
    });
  }
}
