import CsvDecoderStream from "web-utility-streams/CsvDecoderStream";
import CsvObjectDecoderStream from "web-utility-streams/CsvObjectDecoderStream";
import FilterMapStream from "web-utility-streams/FilterMapStream";
import TableDecoderStream, {
  type TableRow,
} from "web-utility-streams/TableDecoderStream";
import { parse as fastCsvParse } from "fast-csv";
import type { DataRecord } from "../data.ts";
import {
  countIterable,
  countWeb,
  nodeFile,
  OURS,
  readFileBaseline,
  type Suite,
} from "../shared.ts";

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
function getRecordFromObject(row: Record<string, string>): DataRecord {
  const tags = row["tags"];
  const manager = row["manager"];
  return {
    id: Number(row["id"]),
    name: row["name"]!,
    email: row["email"]!,
    active: row["active"] === "true",
    score: Number(row["score"]),
    tags: tags ? tags.split(";") : [],
    address: {
      street: row["street"]!,
      city: row["city"]!,
      zip: row["zip"]!,
    },
    bio: row["bio"]!,
    manager: manager === "" ? null : Number(manager),
    createdAt: row["createdAt"]!,
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
  headers: true,
  description:
    "Decode a CSV file with a header row, and the same rows as decode-csv, into records, looking each field up by its column name, comparing CsvObjectDecoderStream with piping CsvDecoderStream into TableDecoderStream",
  cases: [
    {
      library: OURS,
      name: "CsvObjectDecoderStream",
      ours: true,
      run: async ({ file }) =>
        countWeb(file, (stream) =>
          stream
            .pipeThrough(new TextDecoderStream())
            .pipeThrough(new CsvObjectDecoderStream(getRecord)),
        ),
    },
    {
      library: OURS,
      name: "CsvDecoderStream → TableDecoderStream",
      ours: true,
      run: async ({ file }) =>
        countWeb(file, (stream) =>
          stream
            .pipeThrough(new TextDecoderStream())
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
          stream
            .pipeThrough(new TextDecoderStream())
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
      run: async ({ file }) =>
        countIterable<Record<string, string>>(
          (await nodeFile(file)).pipe(fastCsvParse({ headers: true })),
          getRecordFromObject,
        ),
    },
    readFileBaseline,
  ],
} satisfies Suite;
