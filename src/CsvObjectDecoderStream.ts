import { CsvDecoder, type CsvDecoderOptions } from "./CsvDecoderStream.js";
import TableDecoder, {
  type TableCellParser,
  type TableDecoderOptions,
  type TableRow,
} from "./internal/TableDecoder.js";

export type CsvCellParser<T> = TableCellParser<T>;
export type CsvRow = TableRow;

export interface CsvObjectDecoderStreamOptions<TObject>
  extends CsvDecoderOptions, TableDecoderOptions<TObject> {}

export default class CsvObjectDecoderStream<TObject> extends TransformStream<
  string,
  TObject
> {
  constructor(
    getObject: (row: CsvRow) => TObject,
    options: CsvObjectDecoderStreamOptions<TObject> = {},
  ) {
    let decoder!: CsvDecoder;
    super({
      start(controller) {
        const tableDecoder = new TableDecoder(getObject, options);
        decoder = new CsvDecoder(
          (chunk) => tableDecoder.transform(chunk, controller),
          options,
        );
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
