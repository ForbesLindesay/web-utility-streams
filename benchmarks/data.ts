// Generates the data used by the benchmarks. Records are deterministic, so
// every run (and every library) sees exactly the same input, and results can
// be checked by counting the records and summing their ids.

import fs from "node:fs";
import path from "node:path";
import { once } from "node:events";

export const DATA_DIR = path.join(import.meta.dirname, ".data");

const CITIES = ["London", "Paris", "Zürich", "São Paulo", "東京", "New York"];
const TAGS = ["alpha", "beta", "gamma", "delta"];

/**
 * The format of a data file: a JSON document, newline delimited JSON, or
 * server-sent events (`text/event-stream`).
 */
export type DataFormat = "json" | "ndjson" | "sse";

export interface DataRecord {
  id: number;
  name: string;
  email: string;
  active: boolean;
  score: number;
  tags: string[];
  address: { street: string; city: string; zip: string };
  bio: string;
  manager: number | null;
  createdAt: string;
}

export interface DataEvent {
  type: string;
  id: string;
  data: string;
}

/**
 * A realistic-looking API record, including strings that need escaping and
 * non-ASCII characters.
 */
export function record(i: number): DataRecord {
  return {
    id: i,
    name: `User ${i}`,
    email: `user${i}@example.com`,
    active: i % 3 !== 0,
    score: ((i * 7919) % 100000) / 100,
    tags: TAGS.slice(0, i % 5),
    address: {
      street: `${i % 1000} Main St`,
      city: CITIES[i % CITIES.length]!,
      zip: String(10000 + (i % 90000)),
    },
    bio: `Says "hello" in ${i % 7} languages\nand likes emoji ✨`,
    manager: i % 10 === 0 ? null : i - (i % 10),
    createdAt: new Date(1.6e12 + i * 1000).toISOString(),
  };
}

export function* records(count: number): Generator<DataRecord> {
  for (let i = 0; i < count; i++) yield record(i);
}

/**
 * A server-sent event, whose data is the JSON of `record(i)`. Every 10th event
 * has multi-line data.
 */
export function event(i: number): DataEvent {
  const value = record(i);
  return {
    type: "record",
    id: String(i),
    data: i % 10 === 0 ? JSON.stringify(value, null, 2) : JSON.stringify(value),
  };
}

export function* events(count: number): Generator<DataEvent> {
  for (let i = 0; i < count; i++) yield event(i);
}

/**
 * The text of `event(i)` in the `text/event-stream` format, with an
 * occasional comment and `retry` field, like a real server might send.
 */
function eventText(i: number): string {
  const { type, id, data } = event(i);
  let text = "";
  if (i % 100 === 0) text += ": keep-alive\n";
  if (i % 1000 === 0) text += "retry: 3000\n";
  text += `id: ${id}\nevent: ${type}\n`;
  for (const line of data.split("\n")) text += `data: ${line}\n`;
  return text + "\n";
}

const AVERAGE_RECORD_BYTES = (() => {
  let total = 0;
  for (let i = 0; i < 10000; i++) {
    total += Buffer.byteLength(JSON.stringify(record(i))) + 2;
  }
  return total / 10000;
})();

/**
 * The number of records needed for roughly `bytes` of JSON
 */
export function recordCountForBytes(bytes: number): number {
  return Math.floor(bytes / AVERAGE_RECORD_BYTES);
}

/**
 * The expected sum of the ids of `count` records, for checking results
 */
export function expectedIdSum(count: number): number {
  return (count * (count - 1)) / 2;
}

export function dataFile(format: DataFormat, sizeName: string): string {
  return path.join(DATA_DIR, `${sizeName}.${format}`);
}

/**
 * Write a JSON file in the shape of a typical paginated API response:
 * `{"total":N,"page":1,"results":[...],"next":null}`, with one record per line.
 * Or, for "ndjson", one record per line, or for "sse", one event per record
 * in the `text/event-stream` format.
 */
export async function writeDataFile(
  file: string,
  format: DataFormat,
  count: number,
): Promise<void> {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const partial = `${file}.partial`;
  const out = fs.createWriteStream(partial);
  const write = async (text: string) => {
    if (!out.write(text)) await once(out, "drain");
  };
  if (format === "json") {
    await write(`{"total":${count},"page":1,"results":[\n`);
  }
  let buffer = "";
  for (let i = 0; i < count; i++) {
    if (format === "sse") {
      buffer += eventText(i);
    } else {
      const json = JSON.stringify(record(i));
      buffer +=
        format === "json" ? (i === 0 ? json : `,\n${json}`) : `${json}\n`;
    }
    if (buffer.length > 1 << 20) {
      await write(buffer);
      buffer = "";
    }
  }
  await write(buffer);
  if (format === "json") await write(`\n],"next":null}\n`);
  out.end();
  await once(out, "finish");
  fs.renameSync(partial, file);
}
