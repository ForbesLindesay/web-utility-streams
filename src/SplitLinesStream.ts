import LineSplitter from "./internal/LineSplitter.js";

/**
 * Split a stream of text into lines. Lines may be terminated by `\n`, `\r\n`
 * or `\r`. The line terminators are not included in the output. A final line
 * without a trailing line terminator is emitted when the input ends.
 */
export default class SplitLinesStream extends TransformStream<string, string> {
  constructor() {
    let splitter!: LineSplitter;
    super({
      start(controller) {
        splitter = new LineSplitter((text, start, end) =>
          controller.enqueue(text.slice(start, end)),
        );
      },
      transform(chunk, controller) {
        splitter.write(chunk);
      },
      flush(controller) {
        splitter.end();
      },
    });
  }
}
