import TableDecoder, {
  type TableDecoderOptions,
  type TableRow,
} from "./internal/TableDecoder.js";

export type { TableCellParser, TableRow } from "./internal/TableDecoder.js";

export interface TableDecoderStreamOptions<
  TObject,
> extends TableDecoderOptions<TObject> {}

export default class TableDecoderStream<TObject> extends TransformStream<
  readonly string[],
  TObject
> {
  constructor(
    getObject: (row: TableRow) => TObject,
    options: TableDecoderStreamOptions<TObject> = {},
  ) {
    const tableDecoder = new TableDecoder(getObject, options);
    super({
      transform(chunk, controller) {
        tableDecoder.transform(chunk, controller);
      },
    });
  }
}
