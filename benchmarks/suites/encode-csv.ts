import { Readable } from "node:stream";
import CsvEncoderStream from "web-utility-streams/CsvEncoderStream";
import { format as fastCsvFormat } from "fast-csv";
import { stringify as csvStringify } from "csv-stringify";
import { parse as csvParseSync } from "csv-parse/sync";
import { recordRows } from "../data.ts";
import {
  OURS,
  sinkEvents,
  sinkWeb,
  type Suite,
  type WithId,
} from "../shared.ts";

/**
 * Parse encoded CSV with csv-parse (rather than our own decoder), so the
 * runner can check the output
 */
function* parseCsv(text: string): Generator<WithId> {
  for (const row of csvParseSync(text) as string[][]) {
    yield { id: Number(row[0]) };
  }
}

export default {
  format: "csv",
  encoder: true,
  description:
    "Encode generated records as CSV rows of 12 fields, discarding the output. Converting the records to arrays of strings is included in every case",
  cases: [
    {
      library: OURS,
      name: "CsvEncoderStream",
      ours: true,
      run: ({ count, verify }) =>
        sinkWeb(
          ReadableStream.from(recordRows(count)).pipeThrough(
            new CsvEncoderStream({ includeEndRowDelimiter: true }),
          ),
          verify,
          parseCsv,
        ),
    },
    {
      library: "csv-stringify",
      name: "stringify() (Node streams)",
      run: ({ count, verify }) =>
        sinkEvents(
          Readable.from(recordRows(count)).pipe(csvStringify()),
          verify,
          parseCsv,
        ),
    },
    {
      library: "fast-csv",
      name: "format() (Node streams)",
      run: ({ count, verify }) =>
        sinkEvents(
          Readable.from(recordRows(count)).pipe(
            fastCsvFormat({ includeEndRowDelimiter: true }),
          ),
          verify,
          parseCsv,
        ),
    },
    {
      library: "baseline",
      name: "Generate the rows only",
      baseline: true,
      run: async ({ count }) => {
        let ids = 0;
        for await (const row of ReadableStream.from(recordRows(count))) {
          ids += row.length;
        }
        return { chars: 0, ignored: ids };
      },
    },
  ],
} satisfies Suite;
