export type TableCellParser<T> =
  ((value: string) => T) | { readonly parse: (value: string) => T };
export interface TableRow<TColumnName extends string = string> {
  /**
   * The 0-based index of this row, not counting the header row. This is not
   * the line number, as quoted fields can contain line breaks.
   */
  readonly index: number;
  /**
   * Get the value of a column. Throws if the column is not in the headers, or
   * appears more than once.
   */
  readonly get: (columnName: TColumnName) => string;
  /**
   * Get the value of a column, or `undefined` if the column is not in the
   * headers. Throws if the column appears more than once.
   */
  readonly getOptional: (columnName: TColumnName) => string | undefined;
  /**
   * Get the value of a column (as with `get`), and parse it. If the parser
   * throws, the cell is treated as invalid.
   */
  readonly parse: <T>(columnName: TColumnName, parser: TableCellParser<T>) => T;
  /**
   * Get the value of a column (as with `getOptional`), and parse it if the
   * column exists. If the parser throws, the cell is treated as invalid.
   */
  readonly parseOptional: <T>(
    columnName: TColumnName,
    parser: TableCellParser<T>,
  ) => T | undefined;
  /**
   * Get the row as an object, keyed by column name. Throws if any column
   * appears more than once.
   */
  readonly toObject: () => Record<TColumnName, string>;
}

export interface TableDecoderOptions<TObject> {
  /**
   * Called with the header row, before any objects are emitted.
   */
  onHeaders?: (headers: readonly string[]) => void;
  onDuplicateColumn?: (
    columnName: string,
    controller: TransformStreamDefaultController<TObject>,
  ) => void;
  onMissingColumn?: (
    columnName: string,
    controller: TransformStreamDefaultController<TObject>,
  ) => void;
  onInvalidCell?: (
    rowNumber: number,
    columnName: string,
    reason: Error,
    controller: TransformStreamDefaultController<TObject>,
  ) => void;
  onInvalidRow?: (
    rowNumber: number,
    reason: Error,
    controller: TransformStreamDefaultController<TObject>,
  ) => void;
}

/**
 * Used in the column index map for headers that appear more than once.
 */
const DUPLICATE = -1;

export default class TableDecoder<TObject> {
  private readonly _getObject: (row: TableRow) => TObject;
  private _headers: Headers | undefined;
  private _index = 0;

  private _onHeaders?: (headers: readonly string[]) => void;
  private _onDuplicateColumn?: (
    columnName: string,
    controller: TransformStreamDefaultController<TObject>,
  ) => void;
  private _onMissingColumn?: (
    columnName: string,
    controller: TransformStreamDefaultController<TObject>,
  ) => void;
  private _onInvalidCell?: (
    rowNumber: number,
    columnName: string,
    reason: Error,
    controller: TransformStreamDefaultController<TObject>,
  ) => void;
  private _onInvalidRow?: (
    rowNumber: number,
    reason: Error,
    controller: TransformStreamDefaultController<TObject>,
  ) => void;
  constructor(
    getObject: (row: TableRow) => TObject,
    options?: TableDecoderOptions<TObject>,
  ) {
    this._getObject = getObject;
    this._onHeaders = options?.onHeaders;
    this._onDuplicateColumn = options?.onDuplicateColumn;
    this._onMissingColumn = options?.onMissingColumn;
    this._onInvalidCell = options?.onInvalidCell;
    this._onInvalidRow = options?.onInvalidRow;
  }
  transform(
    chunk: readonly string[],
    controller: TransformStreamDefaultController<TObject>,
  ): void {
    if (!this._headers) {
      this._headers = new Headers(chunk);
      this._onHeaders?.(this._headers.names);
      return;
    }
    const row = new Row(this._index++, this._headers, chunk);
    let obj: TObject;
    try {
      if (chunk.length > this._headers.names.length) {
        throw new Error(
          `Expected ${this._headers.names.length} columns, got ${chunk.length}`,
        );
      }
      obj = this._getObject(row);
    } catch (err) {
      if (row.columnError && row.columnError.err === err) {
        if (row.columnError.code === "Duplicate" && this._onDuplicateColumn) {
          this._onDuplicateColumn(row.columnError.columnName, controller);
          return;
        } else if (
          row.columnError.code === "Missing" &&
          this._onMissingColumn
        ) {
          this._onMissingColumn(row.columnError.columnName, controller);
          return;
        } else if (row.columnError.code === "Invalid") {
          if (this._onInvalidCell) {
            const reason = row.columnError.err.cause ?? row.columnError.err;
            this._onInvalidCell(
              row.index,
              row.columnError.columnName,
              reason instanceof Error ? reason : new Error(String(reason)),
              controller,
            );
            return;
          }
        }
      }
      if (this._onInvalidRow) {
        this._onInvalidRow(
          row.index,
          err instanceof Error ? err : new Error(String(err)),
          controller,
        );
      } else {
        throw new Error(
          `Error decoding row ${row.index}: ${err instanceof Error ? err.message : String(err)}`,
          { cause: err },
        );
      }
      return;
    }
    controller.enqueue(obj);
  }
}

class Headers {
  public readonly names: readonly string[];
  public readonly indexes: ReadonlyMap<string, number>;
  constructor(names: readonly string[]) {
    this.names = names;
    const indexes = new Map<string, number>();
    names.forEach((name, i) => {
      if (indexes.has(name)) {
        indexes.set(name, DUPLICATE);
      } else {
        indexes.set(name, i);
      }
    });
    this.indexes = indexes;
  }
}

class Row implements TableRow {
  public readonly index: number;
  public columnError:
    | undefined
    | {
        code: "Duplicate" | "Invalid" | "Missing";
        columnName: string;
        err: Error;
      };
  private readonly _headers: Headers;
  private readonly _cells: readonly string[];
  constructor(index: number, headers: Headers, cells: readonly string[]) {
    this.index = index;
    this._headers = headers;
    this._cells = cells;
  }
  private _throwColumn(
    code: "Duplicate" | "Invalid" | "Missing",
    columnName: string,
    cause?: unknown,
  ): never {
    const err = new Error(`${code} column: ${columnName}`, { cause });
    this.columnError = { code, columnName, err };
    throw err;
  }
  public get(columnName: string): string {
    const value = this.getOptional(columnName);
    if (value === undefined) return this._throwColumn("Missing", columnName);
    return value;
  }
  public getOptional(columnName: string): string | undefined {
    const i = this._headers.indexes.get(columnName);
    if (i === undefined) return undefined;
    if (i === DUPLICATE) return this._throwColumn("Duplicate", columnName);
    return this._cells[i] ?? "";
  }
  public _parse<T>(
    columnName: string,
    value: string,
    parser: TableCellParser<T>,
  ): T {
    try {
      return typeof parser === "function" ? parser(value) : parser.parse(value);
    } catch (err) {
      return this._throwColumn("Invalid", columnName, err);
    }
  }
  public parse<T>(columnName: string, parser: TableCellParser<T>): T {
    const value = this.get(columnName);
    return this._parse(columnName, value, parser);
  }
  public parseOptional<T>(
    columnName: string,
    parser: TableCellParser<T>,
  ): T | undefined {
    const value = this.getOptional(columnName);
    if (value === undefined) return undefined;
    return this._parse(columnName, value, parser);
  }
  public toObject(): Record<string, string> {
    // Object.fromEntries defines own properties, so a "__proto__" header is safe
    return Object.fromEntries(
      this._headers.names.map((columnName, i) => {
        if (this._headers.indexes.get(columnName) === DUPLICATE) {
          return this._throwColumn("Duplicate", columnName);
        }
        return [columnName, this._cells[i] ?? ""];
      }),
    );
  }
}
