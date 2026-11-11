export default class FilterStream<
  TInput,
  TOutput extends TInput = TInput,
> extends TransformStream<TInput, TOutput> {
  constructor(filter: (data: TInput) => data is TOutput);
  constructor(
    filter: TInput extends TOutput ? (data: TInput) => boolean : never,
  );
  constructor(filter: (data: TInput) => data is TOutput) {
    super({
      transform(chunk, controller) {
        if (filter(chunk)) {
          controller.enqueue(chunk);
        }
      },
    });
  }
}
