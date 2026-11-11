export default class DebugStream<TInput> extends TransformStream<
  TInput,
  TInput
> {
  constructor(
    onValue: (value: TInput) => void = (value) => {
      console.log(value);
    },
  ) {
    super({
      transform(chunk, controller) {
        onValue(chunk);
        controller.enqueue(chunk);
      },
    });
  }
}
