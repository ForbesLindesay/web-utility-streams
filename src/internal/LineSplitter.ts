// Internal module, not exported from the package. Shared by `SplitLinesStream`
// and `NewlineDelimitedJsonDecoderStream`, so that
// `NewlineDelimitedJsonDecoderStream` can process lines directly, without the
// overhead of passing each line through a separate stream.

const LINE_BREAK = /\r\n|\r|\n/g;
const LINE_FEED = 10;
const CARRIAGE_RETURN = 13;

/**
 * Called for each line, which is `text.slice(start, end)`. Lines are passed
 * like this, rather than as strings, so that callers can examine a line
 * without creating a new string for it (e.g. to check its first characters).
 */
export type OnLine = (text: string, start: number, end: number) => void;

/**
 * Split text, which can be split into chunks at any point, into lines. Lines
 * may be terminated by `\n`, `\r\n` or `\r`. The line terminators are not
 * included in the output.
 */
export default class LineSplitter {
  // The start of a line that continues in the next chunk
  private _partialLine = "";
  // If a chunk ends with "\r", we emit the line immediately (to avoid
  // delaying output for streams that use "\r" as the line terminator), but
  // we need to remember to skip a "\n" at the start of the next chunk so
  // that a "\r\n" split across two chunks is treated as one line break.
  private _skipLeadingLineFeed = false;
  private readonly _onLine: OnLine;
  constructor(onLine: OnLine) {
    this._onLine = onLine;
  }
  /**
   * Emit a line that ends in this chunk, including the part of it from
   * previous chunks
   */
  private _emitLine(chunk: string, start: number, end: number): void {
    if (this._partialLine) {
      const line = this._partialLine + chunk.slice(start, end);
      this._partialLine = "";
      this._onLine(line, 0, line.length);
    } else {
      this._onLine(chunk, start, end);
    }
  }
  /**
   * Process the next chunk of text, calling `onLine` for each complete line
   */
  write(chunk: string): void {
    let lineStart = 0;
    if (this._skipLeadingLineFeed && chunk.charCodeAt(0) === LINE_FEED) {
      lineStart = 1;
    }
    if (chunk.length > 0) this._skipLeadingLineFeed = false;

    // We only search the new chunk for line breaks, as we already know
    // `partialLine` does not contain any. Most text only uses "\n", which
    // is quicker to search for with `indexOf` than with a regex.
    if (!chunk.includes("\r")) {
      let lineEnd: number;
      while ((lineEnd = chunk.indexOf("\n", lineStart)) !== -1) {
        this._emitLine(chunk, lineStart, lineEnd);
        lineStart = lineEnd + 1;
      }
    } else {
      LINE_BREAK.lastIndex = lineStart;
      let match;
      while ((match = LINE_BREAK.exec(chunk)) !== null) {
        this._emitLine(chunk, lineStart, match.index);
        lineStart = LINE_BREAK.lastIndex;
      }
      this._skipLeadingLineFeed =
        chunk.charCodeAt(chunk.length - 1) === CARRIAGE_RETURN;
    }
    this._partialLine += chunk.slice(lineStart);
  }
  /**
   * Call once there is no more text, to emit the final line (if it wasn't
   * terminated by a line break)
   */
  end(): void {
    if (this._partialLine.length) {
      const line = this._partialLine;
      this._partialLine = "";
      this._onLine(line, 0, line.length);
    }
  }
}
