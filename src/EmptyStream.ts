// Note: using `never` instead of `any` ensures we don't accidentally
// lose type safety when returning the stream from functions with
// inferred types.
export default class EmptyStream extends ReadableStream<never> {
  constructor() {
    super({
      start(controller) {
        controller.close();
      },
    });
  }
}
