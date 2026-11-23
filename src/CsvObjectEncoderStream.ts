import { CsvEncoder, type CsvEncoderOptions } from "./CsvEncoderStream.js";
import TableEncoder, {
  type TableColumn,
  type TableEncoderOptions,
} from "./internal/TableEncoder.js";

export type { TableColumn } from "./internal/TableEncoder.js";

export interface CsvObjectEncoderStreamOptions
  extends CsvEncoderOptions, TableEncoderOptions {}

export default class CsvObjectEncoderStream<TObject> extends TransformStream<
  TObject,
  string
> {
  constructor(
    columns: readonly TableColumn<TObject>[],
    options: CsvObjectEncoderStreamOptions = {},
  ) {
    const tableEncoder = new TableEncoder(columns, options);
    const encoder = new CsvEncoder(options);
    super({
      start(controller) {
        if (tableEncoder.headers) {
          controller.enqueue(encoder.formatRow(tableEncoder.headers));
        }
      },
      transform(chunk, controller) {
        controller.enqueue(encoder.formatRow(tableEncoder.row(chunk)));
      },
    });
  }
}
