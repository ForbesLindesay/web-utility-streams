import { Readable } from "node:stream";
import CsvEncoderStream from "web-utility-streams/CsvEncoderStream";
import CsvObjectEncoderStream, {
  type CsvColumn,
} from "web-utility-streams/CsvObjectEncoderStream";
import MapStream from "web-utility-streams/MapStream";
import TableEncoderStream from "web-utility-streams/TableEncoderStream";
import { format as fastCsvFormat } from "fast-csv";
import { parse as csvParseSync } from "csv-parse/sync";
import {
  RECORD_ROW_HEADERS,
  recordRow,
  records,
  type DataRecord,
} from "../data.ts";
import {
  OURS,
  sinkEvents,
  sinkWeb,
  webRecords,
  type Suite,
  type WithId,
} from "../shared.ts";

/**
 * The same fields as `recordRow`, as columns
 */
const columns: CsvColumn<DataRecord>[] = [
  ["id", (r) => String(r.id)],
  ["name", (r) => r.name],
  ["email", (r) => r.email],
  ["active", (r) => String(r.active)],
  ["score", (r) => String(r.score)],
  ["tags", (r) => r.tags.join(";")],
  ["street", (r) => r.address.street],
  ["city", (r) => r.address.city],
  ["zip", (r) => r.address.zip],
  ["bio", (r) => r.bio],
  ["manager", (r) => (r.manager === null ? "" : String(r.manager))],
  ["createdAt", (r) => r.createdAt],
];

/**
 * Parse encoded CSV (with a header row) with csv-parse, rather than our own
 * decoder, so the runner can check the output
 */
function* parseCsv(text: string): Generator<WithId> {
  for (const row of csvParseSync(text, { columns: true }) as Record<
    string,
    string
  >[]) {
    yield { id: Number(row.id) };
  }
}

function* parseCsvWithoutHeaders(text: string): Generator<WithId> {
  for (const row of csvParseSync(text) as string[][]) {
    yield { id: Number(row[0]) };
  }
}

export default {
  format: "csv",
  encoder: true,
  description:
    "Encode generated records as CSV with a header row, using a function per column to get each field, comparing CsvObjectEncoderStream with piping TableEncoderStream into CsvEncoderStream",
  cases: [
    {
      library: OURS,
      name: "CsvObjectEncoderStream",
      ours: true,
      run: ({ count, verify }) =>
        sinkWeb(
          webRecords(count).pipeThrough(
            new CsvObjectEncoderStream(columns, {
              includeEndRowDelimiter: true,
            }),
          ),
          verify,
          parseCsv,
        ),
    },
    {
      library: OURS,
      name: "TableEncoderStream → CsvEncoderStream",
      ours: true,
      run: ({ count, verify }) =>
        sinkWeb(
          webRecords(count)
            .pipeThrough(new TableEncoderStream(columns))
            .pipeThrough(
              new CsvEncoderStream({ includeEndRowDelimiter: true }),
            ),
          verify,
          parseCsv,
        ),
    },
    {
      library: OURS,
      name: "MapStream(recordRow) → CsvEncoderStream (no header row)",
      ours: true,
      run: ({ count, verify }) =>
        sinkWeb(
          webRecords(count)
            .pipeThrough(new MapStream(recordRow))
            .pipeThrough(
              new CsvEncoderStream({ includeEndRowDelimiter: true }),
            ),
          verify,
          parseCsvWithoutHeaders,
        ),
    },
    {
      library: "fast-csv",
      name: "format({ headers }) (Node streams)",
      run: ({ count, verify }) =>
        sinkEvents(
          Readable.from(records(count)).pipe(
            fastCsvFormat<DataRecord, string[]>({
              headers: RECORD_ROW_HEADERS,
              includeEndRowDelimiter: true,
              transform: (record: DataRecord) => recordRow(record),
            }),
          ),
          verify,
          parseCsv,
        ),
    },
    {
      library: "baseline",
      name: "Generate the records only",
      baseline: true,
      run: async ({ count }) => {
        let ids = 0;
        for await (const record of webRecords(count)) ids += record.id;
        return { chars: 0, ignored: ids };
      },
    },
  ],
} satisfies Suite;
