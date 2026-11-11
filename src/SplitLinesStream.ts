const LINE_BREAK = /\r\n|\r|\n/g;

/**
 * Split a stream of text into lines. Lines may be terminated by `\n`, `\r\n`
 * or `\r`. The line terminators are not included in the output. A final line
 * without a trailing line terminator is emitted when the input ends.
 */
export default class SplitLinesStream extends TransformStream<string, string> {
  constructor() {
    let currentBuffer = "";
    // If a chunk ends with "\r", we emit the line immediately (to avoid
    // delaying output for streams that use "\r" as the line terminator), but
    // we need to remember to skip a "\n" at the start of the next chunk so
    // that a "\r\n" split across two chunks is treated as one line break.
    let skipLeadingLineFeed = false;
    super({
      transform(chunk, controller) {
        if (!chunk) return;
        if (skipLeadingLineFeed && chunk.startsWith("\n")) {
          chunk = chunk.slice(1);
        }
        skipLeadingLineFeed = false;
        if (!chunk) return;

        // Only search the newly added text for line breaks, as we already
        // know the existing buffer does not contain any.
        let lineStart = 0;
        LINE_BREAK.lastIndex = currentBuffer.length;
        currentBuffer += chunk;
        let match;
        while ((match = LINE_BREAK.exec(currentBuffer)) !== null) {
          controller.enqueue(currentBuffer.slice(lineStart, match.index));
          lineStart = LINE_BREAK.lastIndex;
        }
        currentBuffer = currentBuffer.slice(lineStart);
        skipLeadingLineFeed = lineStart > 0 && chunk.endsWith("\r");
      },
      flush(controller) {
        if (currentBuffer.length) {
          controller.enqueue(currentBuffer);
        }
      },
    });
  }
}
