import TableEncoder, {
  type TableColumn,
  type TableEncoderOptions,
} from "./internal/TableEncoder.js";

export type { TableColumn } from "./internal/TableEncoder.js";

export interface TableEncoderStreamOptions extends TableEncoderOptions {}

export default class TableEncoderStream<TObject> extends TransformStream<
  TObject,
  string[]
> {
  constructor(
    columns: readonly TableColumn<TObject>[],
    options: TableEncoderStreamOptions = {},
  ) {
    const tableEncoder = new TableEncoder(columns, options);
    super({
      start(controller) {
        if (tableEncoder.headers) controller.enqueue([...tableEncoder.headers]);
      },
      transform(chunk, controller) {
        controller.enqueue(tableEncoder.row(chunk));
      },
    });
  }
}
