import { getCsvChar } from "./internal/CsvOptions.js";

export interface CsvDecoderOptions {
  /**
   * The character that separates fields. By default, it is guessed from the
   * first line: whichever of `","`, `";"` and `"\t"` appears most often
   * (outside of quoted fields), or `","` if none of them do.
   * It must be passed if `quote` or `escape` is one of these characters.
   */
  readonly delimiter?: string;
  /**
   * If `delimiter` isn't passed, the maximum number of characters to read
   * before guessing it, if the first line is longer than this. This limits
   * how much text is held, and how long rows are delayed, while guessing. The
   * limit is checked after each chunk, so all of the chunk that reaches it is
   * used. Defaults to 10,000.
   */
  readonly maxCharsToGuessDelimiter?: number;
  /**
   * The character used to quote fields. Defaults to `'"'`. Pass `false` to
   * disable quoting, so quotes are treated like any other character.
   */
  readonly quote?: string | boolean;
  /**
   * The character used to escape a quote within a quoted field. Defaults to
   * the quote character, so a quote is escaped by doubling it (`""`). If it is
   * a different character (e.g. `"\\"`), it also escapes itself, and is kept
   * as it is if it is followed by any other character.
   */
  readonly escape?: string;
  /**
   * Skip blank lines (lines that are empty, or only contain spaces and tabs),
   * rather than emitting them as a row with a single field.
   */
  readonly ignoreEmpty?: boolean;
}

const LF = 10;
const CR = 13;
const SPACE = 32;
const TAB = 9;
const BOM = 0xfeff;

// What the decoder is in the middle of parsing
const FIELD_START = 0;
const UNQUOTED = 1;
const QUOTED = 2;
const AFTER_QUOTE = 3;
type State =
  typeof FIELD_START | typeof UNQUOTED | typeof QUOTED | typeof AFTER_QUOTE;

function getChar(name: string, value: string): string {
  return getCsvChar("CsvDecoder", name, value);
}

/**
 * Find `char` in `text`, starting at `from`. Returns `end` if it is not found
 * before `end`, so that the result can be compared with other positions
 * without checking for -1.
 */
function find(text: string, char: string, from: number, end: number): number {
  const index = text.indexOf(char, from);
  return index === -1 || index > end ? end : index;
}

/**
 * The delimiters we choose between when guessing, in order of preference
 */
const DELIMITER_CANDIDATES = [",", ";", "\t"].map((c) => c.charCodeAt(0));

/**
 * Guesses the delimiter by counting the candidate delimiters in the first
 * line, ignoring any that are in quoted fields. The text can be passed in
 * chunks, and each character is only looked at once.
 */
class DelimiterDetector {
  private readonly _counts: number[];
  private readonly _quoteCode: number;
  private readonly _escapeCode: number;
  private _inQuotes = false;
  /** Whether the previous character was an escape character in quotes */
  private _escaping = false;
  /** Whether we have not yet seen any text (so we need to skip a BOM) */
  private _atStart = true;
  /**
   * Whether we are at the start of a field (after the start of the line or
   * a candidate delimiter, and any spaces), where a quote starts a quoted
   * field. Elsewhere, the decoder treats quotes as normal characters.
   */
  private _atFieldStart = true;
  /** Whether the previous character closed a quoted field */
  private _quoteClosed = false;
  constructor(quote: string, escape: string) {
    this._counts = DELIMITER_CANDIDATES.map(() => 0);
    this._quoteCode = quote ? quote.charCodeAt(0) : -1;
    // Only needed if it's different from the quote character
    this._escapeCode = escape && escape !== quote ? escape.charCodeAt(0) : -1;
  }
  /**
   * Count the delimiters in the next chunk of text. Returns true once the
   * end of the first line has been reached.
   */
  write(chunk: string): boolean {
    const quoteCode = this._quoteCode;
    let i = 0;
    if (this._atStart && chunk.length !== 0) {
      this._atStart = false;
      if (chunk.charCodeAt(0) === BOM) i = 1;
    }
    for (; i < chunk.length; i++) {
      const code = chunk.charCodeAt(i);
      if (this._inQuotes) {
        if (this._escaping) this._escaping = false;
        else if (code === this._escapeCode) this._escaping = true;
        else if (code === quoteCode) {
          this._inQuotes = false;
          this._quoteClosed = true;
        }
        continue;
      }
      if (code === quoteCode && (this._atFieldStart || this._quoteClosed)) {
        // A doubled quote leaves and re-enters the quoted field, so it's
        // handled by re-entering the field straight after it closes
        this._inQuotes = true;
        this._atFieldStart = false;
        this._quoteClosed = false;
        continue;
      }
      this._quoteClosed = false;
      if (code === LF || code === CR) return true;
      const index = DELIMITER_CANDIDATES.indexOf(code);
      if (index !== -1) {
        this._counts[index]!++;
        this._atFieldStart = true;
      } else if (code !== SPACE) {
        // Spaces (and tabs, which are candidates) can come before a quoted
        // field
        this._atFieldStart = false;
      }
    }
    return false;
  }
  /**
   * The most common delimiter, preferring earlier candidates if there's a tie
   */
  result(): string {
    let best = 0;
    for (let i = 1; i < this._counts.length; i++) {
      if (this._counts[i]! > this._counts[best]!) best = i;
    }
    return String.fromCharCode(DELIMITER_CANDIDATES[best]!);
  }
}

/**
 * The CSV parser used by `CsvDecoderStream`. You can use it directly if you
 * want to parse CSV text without using streams.
 */
export class CsvDecoder {
  private readonly _onRow: (row: string[]) => void;

  // These are set once the delimiter has been guessed, if it wasn't passed
  private _delimiter = "";
  private _delimiterCode = -1;
  private readonly _maxCharsToDetectDelimiter: number;
  private readonly _quote: string;
  private readonly _quoteCode: number;
  private readonly _escape: string;
  private readonly _escapeCode: number;
  private readonly _ignoreEmpty: boolean;

  // State that is kept between calls to `parse`
  /** Text that hasn't been parsed yet, as it depends on the next chunk */
  private _buffer = "";
  /** Set until we have guessed the delimiter, if it wasn't passed */
  private _detector: DelimiterDetector | undefined;
  private _state: State = FIELD_START;
  /** The fields of the current row that have been parsed so far */
  private _row: string[] = [];
  /** The start of the current field, if it continues in the next chunk */
  private _field = "";
  /** Whether we have not yet seen any text (so we need to skip a BOM) */
  private _atStart = true;
  /**
   * Whether the previous chunk ended with a "\r" that ended a row, so a "\n"
   * at the start of the next chunk is part of the same line break.
   */
  private _skipLineFeed = false;
  /** The number of rows that have ended so far, for error messages */
  private _rowCount = 0;

  public constructor(onRow: (row: string[]) => void, opts?: CsvDecoderOptions) {
    this._onRow = onRow;

    const quote = opts?.quote ?? true;
    this._quote =
      quote === true ? '"' : quote === false ? "" : getChar("quote", quote);
    this._quoteCode = this._quote ? this._quote.charCodeAt(0) : -1;
    this._escape = this._quote
      ? getChar("escape", opts?.escape ?? this._quote)
      : "";
    this._escapeCode = this._escape ? this._escape.charCodeAt(0) : -1;
    this._maxCharsToDetectDelimiter = opts?.maxCharsToGuessDelimiter ?? 10_000;
    if (opts?.delimiter === undefined) {
      for (const [name, value] of [
        ["quote", this._quote],
        ["escape", this._escape],
      ] as const) {
        if (value && DELIMITER_CANDIDATES.includes(value.charCodeAt(0))) {
          throw new TypeError(
            `CsvDecoder: delimiter must be passed if ${name} is ${JSON.stringify(value)}, as the delimiter could be guessed to be the same character`,
          );
        }
      }
      this._detector = new DelimiterDetector(this._quote, this._escape);
    } else {
      this._delimiter = getChar("delimiter", opts.delimiter);
      this._delimiterCode = this._delimiter.charCodeAt(0);
      if (this._quote && this._delimiter === this._quote) {
        throw new TypeError(
          "CsvDecoder: delimiter and quote must be different",
        );
      }
    }
    this._ignoreEmpty = opts?.ignoreEmpty ?? false;
  }

  /**
   * Parse the next chunk of text, calling `onRow` for each row that is
   * completed. The text can be split into chunks at any point. Pass
   * `isFinal: true` with the last chunk (which can be empty) to emit the
   * final row, if the text doesn't end with a line break.
   *
   * The decoder keeps track of the parts of an unfinished row that it has
   * already parsed, so it only needs to keep a few characters of text (e.g.
   * a quote, which could be the start of an escaped quote) until the next
   * chunk.
   */
  public parse(chunk: string, isFinal: boolean): void {
    const buffer = this._buffer === "" ? chunk : this._buffer + chunk;
    if (this._detector) {
      // Wait until we've seen the whole first line (or all of the input)
      const foundLineEnd = this._detector.write(chunk);
      if (
        !foundLineEnd &&
        !isFinal &&
        buffer.length < this._maxCharsToDetectDelimiter
      ) {
        this._buffer = buffer;
        return;
      }
      this._delimiter = this._detector.result();
      this._delimiterCode = this._delimiter.charCodeAt(0);
      this._detector = undefined;
    }
    this._buffer = "";
    let pos = 0;
    if (buffer.length === 0) {
      if (isFinal) this._finish();
      return;
    }
    if (this._atStart) {
      this._atStart = false;
      if (buffer.charCodeAt(0) === BOM) pos = 1;
    }
    if (this._skipLineFeed) {
      this._skipLineFeed = false;
      if (buffer.charCodeAt(pos) === LF) pos++;
    }

    const end = buffer.length;
    const parsedTo = this._parse(buffer, pos, end, isFinal);
    if (isFinal) {
      this._finish();
      return;
    }
    if (
      parsedTo === end &&
      this._state === FIELD_START &&
      this._row.length === 0 &&
      buffer.charCodeAt(end - 1) === CR
    ) {
      // The chunk ended with a "\r" that ended a row
      this._skipLineFeed = true;
    }
    if (parsedTo !== buffer.length) this._buffer = buffer.slice(parsedTo);
  }

  /**
   * Emit the final row, once there is no more input
   */
  private _finish(): void {
    const state = this._state;
    if (state === QUOTED) {
      throw this._error("missing closing quote");
    }
    if (state === FIELD_START && this._row.length === 0) return;
    const row = this._row;
    row.push(this._field);
    this._row = [];
    this._field = "";
    this._state = FIELD_START;
    this._endRow(row, state === UNQUOTED);
  }

  private _endRow(row: string[], lastFieldUnquoted: boolean): void {
    this._rowCount++;
    if (
      this._ignoreEmpty &&
      lastFieldUnquoted &&
      row.length === 1 &&
      isBlank(row[0]!)
    ) {
      return;
    }
    this._onRow(row);
  }

  private _error(message: string): SyntaxError {
    return new SyntaxError(
      `CsvDecoder: Invalid CSV on row ${this._rowCount + 1}: ${message}`,
    );
  }

  /**
   * Returns the length of the line break at `pos`, or 0 if there isn't one.
   * `pos` must be less than `end`.
   */
  private _lineBreakAt(text: string, pos: number, end: number): number {
    const code = text.charCodeAt(pos);
    if (code === LF) return 1;
    if (code === CR) {
      return pos + 1 < end && text.charCodeAt(pos + 1) === LF ? 2 : 1;
    }
    return 0;
  }

  /**
   * Parse `text` from `pos` to `end`. Returns the position up to which the
   * text has been parsed. Any text after that must be passed again, followed
   * by the next chunk.
   */
  private _parse(
    text: string,
    pos: number,
    end: number,
    isFinal: boolean,
  ): number {
    const delimiter = this._delimiter;
    const delimiterCode = this._delimiterCode;
    const quoteCode = this._quoteCode;

    let state = this._state;
    let row = this._row;
    let field = this._field;

    // The next delimiter, "\n" and "\r" at or after `pos` (or `end` if there
    // aren't any). These are cached, and only searched for again once we have
    // gone past them, which makes parsing unquoted fields much faster than
    // checking one character at a time.
    let nextDelimiter = -1;
    let nextLF = -1;
    let nextCR = -1;

    while (pos < end) {
      if (state === FIELD_START) {
        const code = text.charCodeAt(pos);
        if (code === quoteCode) {
          pos++;
          state = QUOTED;
        } else if (
          (code === SPACE || code === TAB) &&
          code !== delimiterCode &&
          quoteCode !== -1
        ) {
          // Spaces before a quoted field are ignored
          let p = pos + 1;
          while (p < end) {
            const c = text.charCodeAt(p);
            if ((c !== SPACE && c !== TAB) || c === delimiterCode) break;
            p++;
          }
          if (p === end && !isFinal) {
            // We can't tell yet whether this is a quoted field
            break;
          }
          if (p < end && text.charCodeAt(p) === quoteCode) {
            pos = p + 1;
            state = QUOTED;
          } else {
            state = UNQUOTED;
          }
        } else {
          state = UNQUOTED;
        }
      }

      if (state === UNQUOTED) {
        // Find the end of the field
        if (nextDelimiter < pos) {
          nextDelimiter = find(text, delimiter, pos, end);
        }
        if (nextLF < pos) nextLF = find(text, "\n", pos, end);
        if (nextCR < pos) nextCR = find(text, "\r", pos, end);
        const lineBreak = nextLF < nextCR ? nextLF : nextCR;
        const lineBreakLength =
          lineBreak < nextDelimiter
            ? this._lineBreakAt(text, lineBreak, end)
            : 0;

        if (nextDelimiter < lineBreak) {
          const value =
            field === ""
              ? text.slice(pos, nextDelimiter)
              : field + text.slice(pos, nextDelimiter);
          field = "";
          row.push(value);
          pos = nextDelimiter + 1;
          state = FIELD_START;
        } else if (lineBreak < end) {
          const value =
            field === ""
              ? text.slice(pos, lineBreak)
              : field + text.slice(pos, lineBreak);
          field = "";
          row.push(value);
          pos = lineBreak + lineBreakLength;
          state = FIELD_START;
          const completed = row;
          row = [];
          this._endRow(completed, true);
        } else {
          // The field continues in the next chunk
          field += text.slice(pos, end);
          pos = end;
        }
        continue;
      }

      if (state === QUOTED) {
        const result =
          this._escapeCode === quoteCode
            ? this._parseQuotedDoubled(text, pos, end, isFinal, field)
            : this._parseQuotedEscaped(text, pos, end, isFinal, field);
        field = this._quotedValue;
        if (result < 0) {
          // The field continues in the next chunk
          this._state = state;
          this._row = row;
          this._field = field;
          return -result - 1;
        }
        pos = result;
        state = AFTER_QUOTE;
      }

      // state === AFTER_QUOTE
      // The closing quote must be followed by a delimiter or line break,
      // optionally with spaces before it.
      while (pos < end) {
        const code = text.charCodeAt(pos);
        if (code === delimiterCode) {
          row.push(field);
          field = "";
          pos++;
          state = FIELD_START;
          break;
        }
        const lineBreakLength = this._lineBreakAt(text, pos, end);
        if (lineBreakLength !== 0) {
          row.push(field);
          field = "";
          pos += lineBreakLength;
          state = FIELD_START;
          const completed = row;
          row = [];
          this._endRow(completed, false);
          break;
        }
        if (code === SPACE || code === TAB) {
          pos++;
          continue;
        }
        this._state = state;
        this._row = row;
        this._field = field;
        throw this._error(
          `expected ${JSON.stringify(delimiter)} or a line break after the closing quote, but got ${JSON.stringify(text[pos])}`,
        );
      }
    }

    this._state = state;
    this._row = row;
    this._field = field;
    return pos;
  }

  // The value of the quoted field, set by `_parseQuoted*`, so they can
  // return the position without allocating an object
  private _quotedValue = "";

  /**
   * Parse a quoted field where quotes are escaped by doubling them, starting
   * after the opening quote (or where the previous chunk ended). Returns the
   * position after the closing quote, or `-(parsedTo + 1)` if the field
   * continues in the next chunk.
   */
  private _parseQuotedDoubled(
    text: string,
    pos: number,
    end: number,
    isFinal: boolean,
    field: string,
  ): number {
    const quote = this._quote;
    const quoteCode = this._quoteCode;
    for (;;) {
      const q = find(text, quote, pos, end);
      if (q === end) {
        this._quotedValue = field + text.slice(pos, end);
        if (isFinal) {
          this._field = this._quotedValue;
          this._state = QUOTED;
          throw this._error("missing closing quote");
        }
        return -end - 1;
      }
      if (q + 1 < end) {
        if (text.charCodeAt(q + 1) === quoteCode) {
          // An escaped quote
          field += text.slice(pos, q + 1);
          pos = q + 2;
          continue;
        }
      } else if (!isFinal) {
        // We can't tell yet whether this is an escaped quote
        this._quotedValue = field + text.slice(pos, q);
        return -q - 1;
      }
      this._quotedValue =
        field === "" ? text.slice(pos, q) : field + text.slice(pos, q);
      return q + 1;
    }
  }

  /**
   * Parse a quoted field where quotes (and the escape character itself) are
   * escaped by a different escape character. See `_parseQuotedDoubled`.
   */
  private _parseQuotedEscaped(
    text: string,
    pos: number,
    end: number,
    isFinal: boolean,
    field: string,
  ): number {
    const quote = this._quote;
    const quoteCode = this._quoteCode;
    const escape = this._escape;
    const escapeCode = this._escapeCode;
    let nextEscape = -1;
    for (;;) {
      const q = find(text, quote, pos, end);
      if (nextEscape < pos) nextEscape = find(text, escape, pos, end);
      if (nextEscape < q) {
        const e = nextEscape;
        if (e + 1 === end && !isFinal) {
          // We can't tell yet what is being escaped
          this._quotedValue = field + text.slice(pos, e);
          return -e - 1;
        }
        const next = e + 1 < end ? text.charCodeAt(e + 1) : -1;
        if (next === quoteCode || next === escapeCode) {
          field += text.slice(pos, e) + text[e + 1];
          pos = e + 2;
        } else {
          // The escape character isn't escaping anything
          field += text.slice(pos, e + 1);
          pos = e + 1;
        }
        continue;
      }
      if (q === end) {
        this._quotedValue = field + text.slice(pos, end);
        if (isFinal) {
          this._field = this._quotedValue;
          this._state = QUOTED;
          throw this._error("missing closing quote");
        }
        return -end - 1;
      }
      this._quotedValue = field + text.slice(pos, q);
      return q + 1;
    }
  }
}

function isBlank(value: string): boolean {
  for (let i = 0; i < value.length; i++) {
    const code = value.charCodeAt(i);
    if (code !== SPACE && code !== TAB) return false;
  }
  return true;
}

/**
 * Decode a stream of CSV text into rows, where each row is an array of
 * strings. The text can be split into chunks at any point.
 */
export default class CsvDecoderStream extends TransformStream<
  string,
  string[]
> {
  constructor(options?: CsvDecoderOptions) {
    let decoder!: CsvDecoder;
    super({
      start(controller) {
        decoder = new CsvDecoder((row) => controller.enqueue(row), options);
      },
      transform(chunk) {
        decoder.parse(chunk, false);
      },
      flush() {
        decoder.parse("", true);
      },
    });
  }
}
