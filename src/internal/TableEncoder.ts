/**
 * A column header, and a function to get that column's value for an object.
 * `index` is the 0-based index of the object, not counting the header row.
 */
export type TableColumn<
  TObject,
  TColumnName extends string = string,
> = readonly [
  header: TColumnName,
  getValue: (obj: TObject, index: number) => string,
];

export interface TableEncoderOptions {
  /**
   * Don't write the header row (e.g. when appending to an existing table).
   */
  skipHeaders?: boolean;
}

/**
 * Converts objects into rows (arrays of strings), using a function per
 * column. Shared by `TableEncoderStream` and `CsvObjectEncoderStream`.
 */
export default class TableEncoder<TObject> {
  /**
   * The header row, or `undefined` if `skipHeaders` was set
   */
  public readonly headers: readonly string[] | undefined;
  private readonly _getters: readonly ((
    obj: TObject,
    index: number,
  ) => string)[];
  private _index = 0;
  constructor(
    columns: readonly TableColumn<TObject>[],
    { skipHeaders = false }: TableEncoderOptions = {},
  ) {
    this.headers = skipHeaders ? undefined : columns.map(([header]) => header);
    this._getters = columns.map(([, getValue]) => getValue);
  }
  /**
   * Get the row for the next object
   */
  public row(obj: TObject): string[] {
    const index = this._index++;
    const getters = this._getters;
    const row = new Array<string>(getters.length);
    for (let i = 0; i < getters.length; i++) row[i] = getters[i]!(obj, index);
    return row;
  }
}
