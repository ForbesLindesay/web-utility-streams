import CsvDecoderStream from "web-utility-streams/CsvDecoderStream";
import { parse as fastCsvParse } from "fast-csv";
import { parse as csvParse } from "csv-parse";
import { parse as csvParseWeb } from "csv-parse/stream";
import Papa from "papaparse";
import {
  countIterable,
  countWeb,
  nodeFile,
  OURS,
  readFileBaseline,
  type Suite,
  type WithId,
} from "../shared.ts";

/**
 * The id of a row, which is its first field
 */
const rowRecord = (row: string[]): WithId => ({ id: Number(row[0]) });

export default {
  format: "csv",
  description:
    "Decode a CSV file with one row of 12 fields per record (no header row), where one field is quoted and contains escaped quotes and a line break",
  cases: [
    {
      library: OURS,
      name: "CsvDecoderStream",
      ours: true,
      run: async ({ file }) =>
        countWeb(
          file,
          (stream) =>
            stream
              .pipeThrough(new TextDecoderStream())
              .pipeThrough(new CsvDecoderStream()),
          rowRecord,
        ),
    },
    {
      library: "csv-parse",
      name: "parse() (Node streams)",
      run: async ({ file }) =>
        countIterable<string[]>(
          (await nodeFile(file)).pipe(csvParse()),
          rowRecord,
        ),
    },
    {
      library: "csv-parse",
      name: "csv-parse/stream (web streams)",
      run: async ({ file }) =>
        countWeb(
          file,
          (stream) =>
            stream.pipeThrough(
              csvParseWeb() as TransformStream<Uint8Array, string[]>,
            ),
          rowRecord,
        ),
    },
    {
      library: "papaparse",
      name: "Papa.parse(NODE_STREAM_INPUT)",
      run: async ({ file }) =>
        countIterable<string[]>(
          (await nodeFile(file)).pipe(
            Papa.parse(Papa.NODE_STREAM_INPUT, { skipEmptyLines: true }),
          ),
          rowRecord,
        ),
    },
    {
      library: "fast-csv",
      name: "parse() (Node streams)",
      run: async ({ file }) =>
        countIterable<string[]>(
          (await nodeFile(file)).pipe(fastCsvParse()),
          rowRecord,
        ),
    },
    readFileBaseline,
  ],
} satisfies Suite;
