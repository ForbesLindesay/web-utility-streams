// Internal module, not exported from the package. Shared by
// `JsonTokenDecoderStream` and `JsonDecoderStream`, so that
// `JsonDecoderStream` can process tokens directly, without the overhead of
// passing each token through a stream.

// Character codes
export const TAB = 9;
export const LINE_FEED = 10;
export const CARRIAGE_RETURN = 13;
export const SPACE = 32;
export const QUOTE = 34;
export const COMMA = 44;
export const MINUS = 45;
export const COLON = 58;
export const OPEN_ARRAY = 91; // [
export const BACKSLASH = 92;
export const CLOSE_ARRAY = 93; // ]
export const OPEN_OBJECT = 123; // {
export const CLOSE_OBJECT = 125; // }

// What can come next in the document.
// Each state is a power of 2 so it can be checked using bitwise operators

/** `}` or `]`: after a value or an open `{` or `[` */
const STATE_CLOSE = 1;
/** A value: at the start of the document, after a `:`, or after a `,` in an array */
const STATE_VALUE = 2;
/** A key: after a `{` or a `,` in an object */
const STATE_KEY = 4;
/** A `:`: after a key */
const STATE_COLON = 8;
/** A `,` or the end of the object/array: after a value */
const STATE_COMMA = 16;
/** Nothing (other than whitespace): after the document's root value */
const STATE_DONE = 32;

// In a STATE_VALUE or STATE_KEY we add these to indicate what type of value it is
const STATE_STRING = 64;
const STATE_NUMBER = 128;
const STATE_LITERAL = 256;

const STATE_CAPTURE_REQUESTED = 512;
// In a string, is the next character escaped
const STATE_ESCAPED = 1024;
// Whether the current string has any escapes or control characters. If
// not, we don't need JSON.parse to get its value.
const STATE_NEEDS_PARSING = 2048;

type State = number;

const isNumberChar = (code: number) =>
  (code >= 48 && code <= 57) || // 0-9
  code === 46 || // .
  code === 43 || // +
  code === MINUS ||
  code === 101 || // e
  code === 69; // E

const isLetter = (code: number) =>
  (code >= 97 && code <= 122) || (code >= 65 && code <= 90);

export interface Handlers {
  onLiteral: (value: unknown, position: number) => void;
  onKey: (value: string, position: number) => void;
  // Return true to capture the value and pass it to `onLiteral`
  onOpen: (
    code: typeof OPEN_ARRAY | typeof OPEN_OBJECT,
    position: number,
  ) => boolean;
  onClose: (
    code: typeof CLOSE_ARRAY | typeof CLOSE_OBJECT,
    position: number,
  ) => void;
  onPunctuation: (code: typeof COMMA | typeof COLON, position: number) => void;
}
const IGNORE_ALL_TOKENS_HANDLERS: Handlers = {
  onLiteral() {},
  onKey() {},
  onOpen: () => false,
  onClose() {},
  onPunctuation() {},
};
/**
 * Split JSON text, which can be split into chunks at any point, into tokens.
 * The text must be a single JSON value (with optional whitespace around it).
 * Throws a `SyntaxError`, with a message starting with `name`, for an invalid
 * token, or a token in the wrong place, as soon as it is found.
 */
export default class JsonTokenizer {
  private readonly _name: string;
  private readonly _maxValueLength: number;
  // Return true to "capture" the value
  private _handlers: Handlers;
  private _tokenText = "";
  private _tokenPosition = 0;
  // The number of characters in all previous chunks
  private _offset = 0;
  // The structure of the document: the brackets that are open (as character
  // codes), and what can come next. This is also how we tell keys apart from
  // other strings.
  private readonly _openBrackets: (typeof OPEN_ARRAY | typeof OPEN_OBJECT)[] =
    [];
  private _state: State = STATE_VALUE;

  private _capturePosition = 0;
  private _captureText = "";
  private _captureDepth = 0;

  constructor(name: string, maxValueLength: number, handlers: Handlers) {
    this._name = name;
    this._maxValueLength = maxValueLength;
    this._handlers = handlers;
  }

  private _unexpected(description: string, position: number): never {
    throw new SyntaxError(
      `${this._name}: Unexpected ${description} at position ${position}`,
    );
  }

  /**
   * A value (of any type) has ended
   */
  private _endValue(): void {
    this._state =
      this._openBrackets.length === 0 ? STATE_DONE : STATE_COMMA | STATE_CLOSE;
  }

  private _endLiteral(): void {
    const text = this._tokenText;
    this._tokenText = "";

    if ((this._state & STATE_VALUE) !== STATE_VALUE) {
      return this._unexpected(`token ${text}`, this._tokenPosition);
    }

    let value: boolean | null;
    switch (text) {
      case "true":
        value = true;
        break;
      case "false":
        value = false;
        break;
      case "null":
        value = null;
        break;
      default:
        return this._unexpected(`token ${text}`, this._tokenPosition);
    }

    this._handlers.onLiteral(value, this._tokenPosition);
    this._endValue();
  }
  private _endNumber(): void {
    const text = this._tokenText;
    this._tokenText = "";

    if ((this._state & STATE_VALUE) !== STATE_VALUE) {
      return this._unexpected(`number ${text}`, this._tokenPosition);
    }

    let value: number;
    // JSON.parse validates the number format
    try {
      value = JSON.parse(text);
    } catch {
      return this._unexpected(`number ${text}`, this._tokenPosition);
    }

    this._handlers.onLiteral(value, this._tokenPosition);
    this._endValue();
  }
  private _endString(): void {
    let value = this._tokenText;
    this._tokenText = "";
    if ((this._state & STATE_NEEDS_PARSING) !== STATE_NEEDS_PARSING) {
      value = value.slice(1, -1);
    } else {
      // JSON.parse validates the string escapes
      try {
        value = JSON.parse(value);
      } catch {
        return this._unexpected(`string`, this._tokenPosition);
      }
    }
    if ((this._state & STATE_KEY) === STATE_KEY) {
      this._handlers.onKey(value, this._tokenPosition);
      this._state = STATE_COLON;
    } else if ((this._state & STATE_VALUE) === STATE_VALUE) {
      this._handlers.onLiteral(value, this._tokenPosition);
      this._endValue();
    } else {
      this._unexpected(`string`, this._tokenPosition);
    }
  }

  /**
   * Stop capturing, and process `text` as tokens instead. `text` must start
   * at the beginning of the captured value (`_capturePosition`), and can
   * include the rest of the current chunk after it.
   */
  private _evaluateCapturedAsTokens(text: string): void {
    this._state = STATE_VALUE;
    this._captureText = "";
    this._handlers = IGNORE_ALL_TOKENS_HANDLERS;
    this._offset = this._capturePosition;
    this.write(text);
  }

  /**
   * Scan for the end of the value being captured, starting at `start`.
   * Returns the index after the end of the value, or -1 if it continues in
   * the next chunk.
   */
  private _scanCapture(chunk: string, start: number): number {
    let depth = this._captureDepth;
    let inString = (this._state & STATE_STRING) === STATE_STRING;
    let j = start;
    if ((this._state & STATE_ESCAPED) === STATE_ESCAPED && j < chunk.length) {
      // The previous chunk ended with a backslash in a string
      this._state = this._state & ~STATE_ESCAPED;
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
          this._state =
            backslashes % 2 === 1
              ? this._state | STATE_ESCAPED
              : this._state & ~STATE_ESCAPED;
          break;
        }
        if (backslashes % 2 === 0) inString = false;
        j = quote + 1;
        continue;
      }
      const code = chunk.charCodeAt(j);
      if (code === QUOTE) {
        inString = true;
      } else if (code === OPEN_OBJECT || code === OPEN_ARRAY) {
        depth++;
      } else if (code === CLOSE_OBJECT || code === CLOSE_ARRAY) {
        if (--depth === 0) {
          this._captureDepth = 0;
          return j + 1;
        }
      }
      j++;
    }
    this._captureDepth = depth;
    this._state = inString
      ? this._state | STATE_STRING
      : this._state & ~STATE_STRING;
    return -1;
  }

  private _valueExceedsMaxLength(position: number): never {
    throw new Error(
      `${this._name}: Value at position ${position} exceeds the configured maximum value length of ${this._maxValueLength.toLocaleString()}`,
    );
  }
  /**
   * Process the next chunk of text, calling `onToken` for each complete token
   */
  write(chunk: string): void {
    let i = 0;
    while (i < chunk.length) {
      if ((this._state & STATE_CAPTURE_REQUESTED) === STATE_CAPTURE_REQUESTED) {
        const end = this._scanCapture(chunk, i);
        if (end === -1) {
          // The value continues in the next chunk
          this._captureText += chunk.slice(i);
          if (this._captureText.length > this._maxValueLength) {
            this._valueExceedsMaxLength(this._capturePosition);
          }
          break;
        }
        const text = this._captureText + chunk.slice(i, end);
        let captured;
        try {
          captured = JSON.parse(text);
        } catch {
          // Re-read from the start of the value, which is at `i` in this chunk
          this._evaluateCapturedAsTokens(this._captureText + chunk.slice(i));
          return;
        }
        this._captureText = "";
        this._handlers.onLiteral(captured, this._capturePosition);
        this._endValue();
        i = end;
        continue;
      }
      if ((this._state & STATE_STRING) === STATE_STRING) {
        let end = i;
        let isEscaped = (this._state & STATE_ESCAPED) === STATE_ESCAPED;
        for (; end < chunk.length; end++) {
          const code = chunk.charCodeAt(end);
          if (isEscaped) {
            isEscaped = false;
          } else if (code === BACKSLASH) {
            isEscaped = true;
            this._state = this._state | STATE_NEEDS_PARSING;
          } else if (code === QUOTE) {
            break;
          } else if (code < SPACE) {
            // Control characters must be escaped, so JSON.parse will
            // report this as an error
            this._state = this._state | STATE_NEEDS_PARSING;
          }
        }
        this._state = isEscaped
          ? this._state | STATE_ESCAPED
          : this._state & ~STATE_ESCAPED;
        if (end === chunk.length) {
          // The string continues in the next chunk
          this._tokenText += chunk.slice(i);
          if (this._tokenText.length > this._maxValueLength) {
            this._valueExceedsMaxLength(this._tokenPosition);
          }
          i = end;
        } else {
          this._tokenText += chunk.slice(i, end + 1);
          i = end + 1;
          this._endString();
        }
        continue;
      }
      const isLiteral = (this._state & STATE_LITERAL) === STATE_LITERAL;
      const isNumber = (this._state & STATE_NUMBER) === STATE_NUMBER;
      if (isLiteral || isNumber) {
        const isPart = isNumber ? isNumberChar : isLetter;
        let end = i;
        while (end < chunk.length && isPart(chunk.charCodeAt(end))) end++;
        this._tokenText += chunk.slice(i, end);
        i = end;
        // If we reached the end of the chunk, the token may continue in
        // the next chunk. Otherwise, the next character ends the token.
        if (end < chunk.length) {
          if (isNumber) this._endNumber();
          else this._endLiteral();
        } else if (this._tokenText.length > this._maxValueLength) {
          this._valueExceedsMaxLength(this._tokenPosition);
        }
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
        case OPEN_OBJECT:
        case OPEN_ARRAY: {
          if ((this._state & STATE_VALUE) !== STATE_VALUE) {
            this._unexpected(`token ${chunk[i]}`, position);
          }
          if (this._handlers.onOpen(code, position)) {
            this._state = STATE_CAPTURE_REQUESTED;
            this._capturePosition = position;
            this._captureDepth = 0;
          } else {
            this._openBrackets.push(code);
            this._state =
              (code === OPEN_OBJECT ? STATE_KEY : STATE_VALUE) | STATE_CLOSE;
            i++;
          }
          break;
        }
        case COLON:
          if (this._state !== STATE_COLON) {
            this._unexpected("token :", position);
          }
          this._state = STATE_VALUE;
          this._handlers.onPunctuation?.(code, position);
          i++;
          break;
        case COMMA:
          if ((this._state & STATE_COMMA) !== STATE_COMMA) {
            this._unexpected("token ,", position);
          }
          this._state =
            this._openBrackets[this._openBrackets.length - 1] === OPEN_OBJECT
              ? STATE_KEY
              : STATE_VALUE;
          this._handlers.onPunctuation?.(code, position);
          i++;
          break;
        case CLOSE_OBJECT:
        case CLOSE_ARRAY: {
          // `}` and `]` are 2 more than `{` and `[`
          if (
            this._openBrackets[this._openBrackets.length - 1] !== code - 2 ||
            (this._state & STATE_CLOSE) !== STATE_CLOSE
          ) {
            this._unexpected(`token ${chunk[i]}`, position);
          }
          this._openBrackets.pop();
          this._endValue();
          this._handlers.onClose(code, position);
          i++;
          break;
        }
        case QUOTE:
          // Whether the string is allowed here is checked once it has ended,
          // when we know whether it's valid
          this._state = STATE_STRING | this._state;
          // Consume the opening quote, so the string scanning above
          // doesn't treat it as the closing quote. Numbers and literals
          // are consumed by the scanning above.
          this._tokenText = '"';
          this._tokenPosition = position;
          i++;
          break;
        default:
          if (code === MINUS || (code >= 48 && code <= 57)) {
            this._state = STATE_NUMBER | this._state;
          } else if (isLetter(code)) {
            this._state = STATE_LITERAL | this._state;
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
    // If the input ended part way through a captured value, evaluate its
    // tokens to produce the error at the right position
    if ((this._state & STATE_CAPTURE_REQUESTED) === STATE_CAPTURE_REQUESTED) {
      this._evaluateCapturedAsTokens(this._captureText);
    }
    if ((this._state & STATE_NUMBER) === STATE_NUMBER) {
      // A number at the very end of the input is complete
      this._endNumber();
    }
    if ((this._state & STATE_LITERAL) === STATE_LITERAL) {
      // A literal at the very end of the input is complete
      this._endLiteral();
    }
    if (this._state !== STATE_DONE) {
      throw new SyntaxError(`${this._name}: Unexpected end of JSON input`);
    }
  }
}
