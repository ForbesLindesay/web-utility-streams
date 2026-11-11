export default class TransformPipelineStream<
  In,
  Out,
> implements TransformStream<In, Out> {
  public readonly writable: WritableStream<In>;
  public readonly readable: ReadableStream<Out>;
  constructor(
    buildPipeline: (input: {
      pipeThrough: <T>(
        transform: ReadableWritablePair<T, In>,
      ) => ReadableStream<T>;
    }) => ReadableStream<Out>,
  ) {
    let writable: WritableStream<In> | undefined;
    const readable = buildPipeline({
      pipeThrough: (firstTransform) => {
        if (writable !== undefined) {
          throw new Error("Cannot pipe the input multiple times");
        }
        writable = firstTransform.writable;
        return firstTransform.readable;
      },
    });
    if (writable === undefined) {
      throw new Error(
        "The function passed to `new TransformPipelineStream(...)` must call input.pipeThrough",
      );
    }
    this.writable = writable;
    this.readable = readable;
  }
}
