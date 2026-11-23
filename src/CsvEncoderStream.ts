import { getCsvChar } from "./internal/CsvOptions.js";
import type { LineEnding } from "./internal/LineEndings.js";

const BOM = "\ufeff";
export interface CsvEncoderOptions {
  /** The text between fields. Defaults to `","`. */
  readonly delimiter?: string;
  /** The text between rows. Defaults to `"\n"`. */
  readonly rowDelimiter?: LineEnding;
  /**
   * The character used to quote fields. Defaults to `'"'`.
   */
  readonly quote?: string;
  /**
   * The character used to escape quotes in quoted fields. Defaults to the
   * quote character. If it is a different character, it is also used to
   * escape itself.
   */
  readonly escape?: string;
  /**
   * Defaults to "AUTO", which quotes fields if they need to (i.e. if they
   * contain any special characters). Set this to `"ALWAYS"` to add quotes
   * even if they are not needed, and set it to `"NEVER"` to never add quotes.
   */
  readonly quoteMode?: "ALWAYS" | "AUTO" | "NEVER";
  /**
   * Write `rowDelimiter` after the last row. Defaults to `false`, in which
   * case each row except the first starts with `rowDelimiter`.
   */
  readonly includeEndRowDelimiter?: boolean;
  /** Write a byte order mark at the start of the output. Defaults to `false`. */
  readonly writeBOM?: boolean;
}

/**
 * The CSV encoder used by `CsvEncoderStream`. You can use it directly if you
 * want to encode CSV without using streams.
 */
export class CsvEncoder {
  private readonly _delimiter: string;
  private readonly _rowDelimiter: LineEnding;
  private readonly _quote: string;
  private readonly _escape: string;
  private readonly _alwaysQuote: boolean;
  private readonly _neverQuote: boolean;
  private readonly _includeEndRowDelimiter: boolean;
  private readonly _writeBOM: boolean;
  private readonly _specialRegexp: RegExp;
  private readonly _escapeRegexp: RegExp;
  private readonly _escapeReplacement: string;

  public constructor(opts?: CsvEncoderOptions) {
    this._delimiter = getCsvChar(
      "CsvEncoder",
      "delimiter",
      opts?.delimiter ?? ",",
    );
    this._rowDelimiter = opts?.rowDelimiter ?? "\n";

    this._quote = getCsvChar("CsvEncoder", "quote", opts?.quote ?? `"`);
    this._escape = getCsvChar(
      "CsvEncoder",
      "escape",
      opts?.escape ?? this._quote,
    );
    const quoteMode = opts?.quoteMode ?? "AUTO";
    if (
      quoteMode !== "ALWAYS" &&
      quoteMode !== "AUTO" &&
      quoteMode !== "NEVER"
    ) {
      throw new TypeError(
        `CsvEncoder: quoteMode must be "ALWAYS", "AUTO" or "NEVER", got ${JSON.stringify(quoteMode)}`,
      );
    }
    this._alwaysQuote = quoteMode === "ALWAYS";
    this._neverQuote = quoteMode === "NEVER";
    if (!this._neverQuote && this._delimiter === this._quote) {
      throw new TypeError("CsvEncoder: delimiter and quote must be different");
    }

    this._includeEndRowDelimiter = opts?.includeEndRowDelimiter ?? false;
    this._writeBOM = opts?.writeBOM ?? false;

    // Fields that contain any of these characters must be quoted (or, for
    // "\0", have it removed)
    this._specialRegexp = new RegExp(
      `[${charClass(this._delimiter + this._quote + this._rowDelimiter + "\r\n\0")}]`,
    );
    // Within a quoted field, the quote character (and the escape character,
    // if it's different) must be escaped
    this._escapeRegexp = new RegExp(
      `[${charClass(this._quote + this._escape)}]`,
      "g",
    );
    this._escapeReplacement = this._escape.replace(/\$/g, "$$$$") + "$&";
  }
  /**
   * Format one field. `quoteIfBlank` is set for a row with only one field, as
   * that row would otherwise be a blank line, which is lost if it's the last
   * row (and skipped by decoders that ignore blank lines).
   */
  private _formatField(value: string, quoteIfBlank: boolean): string {
    // Most fields don't need quoting, and don't contain null characters, so
    // we check for both with a single regex
    let special = this._specialRegexp.test(value);
    if (special && value.includes("\0")) {
      value = value.replace(/\0/g, "");
      special = this._specialRegexp.test(value);
    }
    if (
      !this._neverQuote &&
      (special || this._alwaysQuote || (quoteIfBlank && isBlank(value)))
    ) {
      return (
        this._quote +
        value.replace(this._escapeRegexp, this._escapeReplacement) +
        this._quote
      );
    }
    return value;
  }

  public formatRow(columns: readonly string[], isFirst: boolean): string {
    let formattedCols = isFirst
      ? this._writeBOM
        ? BOM
        : ""
      : !this._includeEndRowDelimiter
        ? this._rowDelimiter
        : "";
    if (columns.length === 1) {
      // A row with a single empty column is quoted so it isn't lost
      formattedCols += this._formatField(columns[0]!, true);
    } else {
      for (let i = 0; i < columns.length; i++) {
        if (i !== 0) formattedCols += this._delimiter;
        formattedCols += this._formatField(columns[i]!, false);
      }
    }
    if (this._includeEndRowDelimiter) return formattedCols + this._rowDelimiter;
    else return formattedCols;
  }
}

/**
 * Whether `value` is empty, or only contains spaces and tabs
 */
function isBlank(value: string): boolean {
  for (let i = 0; i < value.length; i++) {
    const code = value.charCodeAt(i);
    if (code !== 32 && code !== 9) return false;
  }
  return true;
}

/**
 * The characters in `chars`, escaped so they can be used in a regex
 * character class
 */
function charClass(chars: string): string {
  let result = "";
  for (let i = 0; i < chars.length; i++) {
    result += `\\u${chars.charCodeAt(i).toString(16).padStart(4, "0")}`;
  }
  return result;
}

/**
 * Encode a stream of rows, where each row is an array of strings, as CSV
 * text. Each row is emitted as a separate chunk.
 */
export default class CsvEncoderStream extends TransformStream<
  readonly string[],
  string
> {
  constructor(options?: CsvEncoderOptions) {
    const encoder = new CsvEncoder(options);
    let isFirst = true;
    super({
      transform(chunk, controller) {
        controller.enqueue(encoder.formatRow(chunk, isFirst));
        isFirst = false;
      },
    });
  }
}
