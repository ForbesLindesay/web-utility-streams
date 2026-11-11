// Internal module, not exported from the package. Shared by
// `JsonTokenDecoderStream` and `JsonDecoderStream`, so that
// `JsonDecoderStream` can process tokens directly, without the overhead of
// passing each token through a stream.

/**
 * A token in a JSON document. `position` is the index of the token's first
 * character in the input text.
 *
 * A string is a `KEY` token if it is directly inside an object, immediately
 * after the `{` or a `,`. All other strings are `LITERAL` tokens.
 */
export type JsonToken =
  | {
      readonly type: "PUNCTUATION";
      readonly value: "{" | "}" | "[" | "]" | ":" | ",";
      readonly position: number;
    }
  | {
      readonly type: "KEY";
      readonly value: string;
      readonly position: number;
    }
  | {
      readonly type: "LITERAL";
      readonly value: string | number | boolean | null;
      readonly position: number;
    };

/**
 * Called with the complete text of a captured object/array, and the position
 * of its first character. Return `false` if the text could not be parsed, and
 * the tokenizer will emit its tokens instead, so the problem can be found and
 * reported.
 */
export type CapturedValueHandler = (text: string, position: number) => boolean;

/**
 * Objects/arrays larger than this are emitted as tokens, rather than
 * captured, so that the text of a huge (or invalid and never-ending) value is
 * not kept in memory.
 */
const MAX_CAPTURE_LENGTH = 1 << 20;

// Character codes
const TAB = 9;
const LINE_FEED = 10;
const CARRIAGE_RETURN = 13;
const SPACE = 32;
const QUOTE = 34;
const COMMA = 44;
const MINUS = 45;
const COLON = 58;
const OPEN_BRACKET = 91;
const BACKSLASH = 92;
const CLOSE_BRACKET = 93;
const OPEN_BRACE = 123;
const CLOSE_BRACE = 125;

const isNumberChar = (code: number) =>
  (code >= 48 && code <= 57) || // 0-9
  code === 46 || // .
  code === 43 || // +
  code === MINUS ||
  code === 101 || // e
  code === 69; // E

const isLetter = (code: number) =>
  (code >= 97 && code <= 122) || (code >= 65 && code <= 90);

/**
 * Split JSON text, which can be split into chunks at any point, into tokens.
 * Throws a `SyntaxError`, with a message starting with `name`, for invalid
 * tokens.
 */
export default class JsonTokenizer {
  private readonly _name: string;
  private readonly _onToken: (token: JsonToken) => void;
  // Strings, numbers and literals can be split across chunks, so we
  // accumulate the text of the current token until it is complete.
  private _tokenKind: "string" | "number" | "literal" | null = null;
  private _tokenText = "";
  private _tokenPosition = 0;
  private _isEscaped = false;
  // Whether the current string has any escapes or control characters. If
  // not, we don't need JSON.parse to get its value.
  private _stringNeedsParsing = false;
  // The number of characters in all previous chunks
  private _offset = 0;
  // To tell keys apart from other strings, we track which brackets are open,
  // and whether the next token is in the position of an object key. We don't
  // check that the brackets match, as we don't check the document structure.
  private readonly _openBrackets: ("{" | "[")[] = [];
  private _isKeyPosition = false;

  // While capturing an object/array, we only track strings and the depth of
  // brackets, to find where it ends. The text is checked by `onCaptured`.
  private _onCaptured: CapturedValueHandler | null = null;
  private _captureRequested = false;
  private _capturePosition = 0;
  private _captureText = "";
  private _captureDepth = 0;
  private _captureInString = false;
  private _captureIsEscaped = false;
  // Set while re-emitting a captured value as tokens, so that it isn't
  // captured again
  private _preventCapture = false;

  constructor(name: string, onToken: (token: JsonToken) => void) {
    this._name = name;
    this._onToken = onToken;
  }

  private _unexpected(description: string, position: number): never {
    throw new SyntaxError(
      `${this._name}: Unexpected ${description} at position ${position}`,
    );
  }

  private _endToken(): JsonToken {
    const text = this._tokenText;
    const kind = this._tokenKind;
    const position = this._tokenPosition;
    const isKey = this._isKeyPosition;
    this._tokenKind = null;
    this._tokenText = "";
    this._isKeyPosition = false;
    if (kind === "literal") {
      switch (text) {
        case "true":
          return { type: "LITERAL", value: true, position };
        case "false":
          return { type: "LITERAL", value: false, position };
        case "null":
          return { type: "LITERAL", value: null, position };
        default:
          return this._unexpected(`token ${text}`, position);
      }
    }
    let value: string | number;
    if (kind === "string" && !this._stringNeedsParsing) {
      value = text.slice(1, -1);
    } else {
      // JSON.parse validates the string escapes or number format
      try {
        value = JSON.parse(text);
      } catch {
        return this._unexpected(
          kind === "string" ? "string" : `number ${text}`,
          position,
        );
      }
    }
    return kind === "string" && isKey
      ? { type: "KEY", value: value as string, position }
      : { type: "LITERAL", value, position };
  }

  /**
   * Stop capturing, and process the captured text (plus `rest`, the
   * remainder of the current chunk) as tokens instead.
   */
  private _emitCapturedAsTokens(text: string, rest: string): void {
    this._onCaptured = null;
    this._captureText = "";
    this._preventCapture = true;
    this._offset = this._capturePosition;
    this.write(text + rest);
  }

  /**
   * Scan for the end of the value being captured, starting at `start`.
   * Returns the index after the end of the value, or -1 if it continues in
   * the next chunk.
   */
  private _scanCapture(chunk: string, start: number): number {
    let depth = this._captureDepth;
    let inString = this._captureInString;
    let j = start;
    if (this._captureIsEscaped && j < chunk.length) {
      // The previous chunk ended with a backslash in a string
      this._captureIsEscaped = false;
      j++;
    }
    while (j < chunk.length) {
      if (inString) {
        // Jump to the next quote, and check whether it's escaped by
        // counting the backslashes before it
        const quote = chunk.indexOf('"', j);
        const stop = quote === -1 ? chunk.length : quote;
        let backslashes = 0;
        while (
          stop - backslashes > j &&
          chunk.charCodeAt(stop - backslashes - 1) === BACKSLASH
        ) {
          backslashes++;
        }
        if (quote === -1) {
          // The string continues in the next chunk
          this._captureIsEscaped = backslashes % 2 === 1;
          break;
        }
        if (backslashes % 2 === 0) inString = false;
        j = quote + 1;
        continue;
      }
      const code = chunk.charCodeAt(j);
      if (code === QUOTE) {
        inString = true;
      } else if (code === OPEN_BRACE || code === OPEN_BRACKET) {
        depth++;
      } else if (code === CLOSE_BRACE || code === CLOSE_BRACKET) {
        if (--depth === 0) {
          this._captureDepth = 0;
          return j + 1;
        }
      }
      j++;
    }
    this._captureDepth = depth;
    this._captureInString = inString;
    return -1;
  }

  /**
   * Process the next chunk of text, calling `onToken` for each complete token
   */
  write(chunk: string): void {
    const onToken = this._onToken;
    let i = 0;
    while (i < chunk.length) {
      if (this._onCaptured !== null) {
        const end = this._scanCapture(chunk, i);
        if (end === -1) {
          // The value continues in the next chunk
          this._captureText += chunk.slice(i);
          if (this._captureText.length > MAX_CAPTURE_LENGTH) {
            this._emitCapturedAsTokens(this._captureText, "");
            return;
          }
          break;
        }
        const text = this._captureText + chunk.slice(i, end);
        const handler = this._onCaptured;
        this._onCaptured = null;
        this._captureText = "";
        if (!handler(text, this._capturePosition)) {
          this._emitCapturedAsTokens(text, chunk.slice(end));
          return;
        }
        this._isKeyPosition = false;
        i = end;
        continue;
      }
      if (this._tokenKind === "string") {
        let end = i;
        let isEscaped = this._isEscaped;
        let needsParsing = this._stringNeedsParsing;
        for (; end < chunk.length; end++) {
          const code = chunk.charCodeAt(end);
          if (isEscaped) {
            isEscaped = false;
          } else if (code === BACKSLASH) {
            isEscaped = true;
            needsParsing = true;
          } else if (code === QUOTE) {
            break;
          } else if (code < SPACE) {
            // Control characters must be escaped, so JSON.parse will
            // report this as an error
            needsParsing = true;
          }
        }
        this._isEscaped = isEscaped;
        this._stringNeedsParsing = needsParsing;
        if (end === chunk.length) {
          // The string continues in the next chunk
          this._tokenText += chunk.slice(i);
          i = end;
        } else {
          this._tokenText += chunk.slice(i, end + 1);
          i = end + 1;
          onToken(this._endToken());
        }
        continue;
      }
      if (this._tokenKind !== null) {
        const isPart = this._tokenKind === "number" ? isNumberChar : isLetter;
        let end = i;
        while (end < chunk.length && isPart(chunk.charCodeAt(end))) end++;
        this._tokenText += chunk.slice(i, end);
        i = end;
        // If we reached the end of the chunk, the token may continue in
        // the next chunk. Otherwise, the next character ends the token.
        if (end < chunk.length) onToken(this._endToken());
        continue;
      }

      const code = chunk.charCodeAt(i);
      const position = this._offset + i;
      switch (code) {
        case SPACE:
        case LINE_FEED:
        case CARRIAGE_RETURN:
        case TAB:
          i++;
          break;
        case OPEN_BRACE:
        case OPEN_BRACKET: {
          const char = code === OPEN_BRACE ? "{" : "[";
          this._openBrackets.push(char);
          this._isKeyPosition = code === OPEN_BRACE;
          this._captureRequested = false;
          onToken({ type: "PUNCTUATION", value: char, position });
          this._preventCapture = false;
          if (this._captureRequested) {
            // Scan from the bracket, so it's included in the text
            this._openBrackets.pop();
            this._capturePosition = position;
            this._captureDepth = 0;
            this._captureInString = false;
            this._captureIsEscaped = false;
          } else {
            i++;
          }
          break;
        }
        case CLOSE_BRACE:
        case CLOSE_BRACKET:
        case COLON:
        case COMMA: {
          if (code === CLOSE_BRACE || code === CLOSE_BRACKET) {
            this._openBrackets.pop();
          }
          this._isKeyPosition =
            code === COMMA &&
            this._openBrackets[this._openBrackets.length - 1] === "{";
          onToken({
            type: "PUNCTUATION",
            value: chunk[i] as "}" | "]" | ":" | ",",
            position,
          });
          i++;
          break;
        }
        case QUOTE:
          this._tokenKind = "string";
          this._stringNeedsParsing = false;
          // Consume the opening quote, so the string scanning above
          // doesn't treat it as the closing quote. Numbers and literals
          // are consumed by the scanning above.
          this._tokenText = '"';
          this._tokenPosition = position;
          i++;
          break;
        default:
          if (code === MINUS || (code >= 48 && code <= 57)) {
            this._tokenKind = "number";
          } else if (isLetter(code)) {
            this._tokenKind = "literal";
          } else {
            this._unexpected(`character ${JSON.stringify(chunk[i])}`, position);
          }
          this._tokenPosition = position;
      }
    }
    this._offset += chunk.length;
  }

  /**
   * Call once there is no more text, to emit any final token
   */
  end(): void {
    // If the input ended part way through a captured value, emit its
    // tokens, which will report the problem. Values inside it may be
    // captured in turn, so repeat until nothing is being captured.
    while (this._onCaptured !== null) {
      this._emitCapturedAsTokens(this._captureText, "");
    }
    if (this._tokenKind === "string") {
      throw new SyntaxError(`${this._name}: Unexpected end of JSON input`);
    }
    if (this._tokenKind !== null) {
      // A number or literal at the very end of the input is complete
      this._onToken(this._endToken());
    }
  }

  /**
   * Call from `onToken`, for a `{` or `[` token, to receive the text of the
   * whole object/array in a single call to `onCaptured`, rather than as
   * tokens. This is much faster when the caller needs the value, as it can
   * pass the text to `JSON.parse`.
   *
   * Returns `false` (and nothing is captured) if this object/array is being
   * re-emitted as tokens, because its text could not be parsed or was too
   * large to capture.
   */
  captureValue(onCaptured: CapturedValueHandler): boolean {
    if (this._preventCapture) return false;
    this._onCaptured = onCaptured;
    this._captureRequested = true;
    return true;
  }
}
