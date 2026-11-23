import { Readable } from "node:stream";
import CsvDecoderStream from "web-utility-streams/CsvDecoderStream";
import CsvObjectDecoderStream from "web-utility-streams/CsvObjectDecoderStream";
import FilterMapStream from "web-utility-streams/FilterMapStream";
import TableDecoderStream, {
  type TableRow,
} from "web-utility-streams/TableDecoderStream";
import { parse as fastCsvParse } from "fast-csv";
import { RECORD_ROW_HEADERS, type DataRecord } from "../data.ts";
import {
  countIterable,
  countWeb,
  nodeFile,
  OURS,
  readFileBaseline,
  type Suite,
} from "../shared.ts";

// The CSV data file has no header row (so it can be shared with decode-csv),
// so every case reads it with the header row added in front. This adds one
// chunk, and one step per 16KiB/64KiB chunk of the file, to every case.
const HEADER_LINE = `${RECORD_ROW_HEADERS.join(",")}\n`;

/**
 * Decode the file as text, with the header row added in front
 */
function withHeaders(
  stream: ReadableStream<Uint8Array>,
): ReadableStream<string> {
  const reader = stream.pipeThrough(new TextDecoderStream()).getReader();
  let sentHeaders = false;
  return new ReadableStream<string>({
    async pull(controller) {
      if (!sentHeaders) {
        sentHeaders = true;
        controller.enqueue(HEADER_LINE);
        return;
      }
      const { done, value } = await reader.read();
      if (done) controller.close();
      else controller.enqueue(value);
    },
    cancel: (reason) => reader.cancel(reason),
  });
}

/**
 * Convert a row to a record, looking each field up by its column name. The
 * same function is used by every case that uses this library.
 */
function getRecord(row: Pick<TableRow, "get">): DataRecord {
  const tags = row.get("tags");
  const manager = row.get("manager");
  return {
    id: Number(row.get("id")),
    name: row.get("name"),
    email: row.get("email"),
    active: row.get("active") === "true",
    score: Number(row.get("score")),
    tags: tags ? tags.split(";") : [],
    address: {
      street: row.get("street"),
      city: row.get("city"),
      zip: row.get("zip"),
    },
    bio: row.get("bio"),
    manager: manager === "" ? null : Number(manager),
    createdAt: row.get("createdAt"),
  };
}

/**
 * The same as `getRecord`, but for a row as an array, using fixed positions
 * rather than looking up column names
 */
function getRecordByIndex(row: string[]): DataRecord {
  const tags = row[5]!;
  const manager = row[10]!;
  return {
    id: Number(row[0]),
    name: row[1]!,
    email: row[2]!,
    active: row[3] === "true",
    score: Number(row[4]),
    tags: tags ? tags.split(";") : [],
    address: { street: row[6]!, city: row[7]!, zip: row[8]! },
    bio: row[9]!,
    manager: manager === "" ? null : Number(manager),
    createdAt: row[11]!,
  };
}

export default {
  format: "csv",
  description:
    "Decode the decode-csv file (with a header row added) into records, looking each field up by its column name, comparing CsvObjectDecoderStream with piping CsvDecoderStream into TableDecoderStream",
  cases: [
    {
      library: OURS,
      name: "CsvObjectDecoderStream",
      ours: true,
      run: async ({ file }) =>
        countWeb(file, (stream) =>
          withHeaders(stream).pipeThrough(
            new CsvObjectDecoderStream(getRecord),
          ),
        ),
    },
    {
      library: OURS,
      name: "CsvDecoderStream → TableDecoderStream",
      ours: true,
      run: async ({ file }) =>
        countWeb(file, (stream) =>
          withHeaders(stream)
            .pipeThrough(new CsvDecoderStream())
            .pipeThrough(new TableDecoderStream(getRecord)),
        ),
    },
    {
      library: OURS,
      name: "CsvDecoderStream → FilterMapStream (fields by position)",
      ours: true,
      run: async ({ file }) => {
        let isHeader = true;
        return countWeb(file, (stream) =>
          withHeaders(stream)
            .pipeThrough(new CsvDecoderStream())
            .pipeThrough(
              new FilterMapStream((row: string[]) => {
                if (isHeader) {
                  isHeader = false;
                  return undefined;
                }
                return getRecordByIndex(row);
              }),
            ),
        );
      },
    },
    {
      library: "fast-csv",
      name: "parse({ headers: true }) (Node streams)",
      run: async ({ file }) => {
        const input = await nodeFile(file);
        const withHeaderLine = Readable.from(
          (async function* () {
            yield Buffer.from(HEADER_LINE);
            yield* input;
          })(),
        );
        return countIterable<Record<string, string>>(
          withHeaderLine.pipe(fastCsvParse({ headers: true })),
          (obj) => getRecord({ get: (name) => obj[name]! }),
        );
      },
    },
    readFileBaseline,
  ],
} satisfies Suite;
