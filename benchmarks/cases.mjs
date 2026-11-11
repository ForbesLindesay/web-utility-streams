// The benchmark cases. Each decoder case reads a data file and returns the
// number of records it decoded and the sum of their ids, so the runner can
// check every library parsed the data correctly. Each encoder case encodes
// generated records and returns the number of characters it output. In a
// separate, untimed "verify" run, the output is also parsed back so the runner
// can check it contains the right records.

import fs, { createReadStream } from "node:fs";
import { open } from "node:fs/promises";
import readline from "node:readline";
import { Readable } from "node:stream";

import JsonDecoderStream from "web-utility-streams/JsonDecoderStream";
import JsonEncoderStream from "web-utility-streams/JsonEncoderStream";
import NewlineDelimitedJsonDecoderStream from "web-utility-streams/NewlineDelimitedJsonDecoderStream";
import NewlineDelimitedJsonEncoderStream from "web-utility-streams/NewlineDelimitedJsonEncoderStream";
import EventSourceDecoderStream from "web-utility-streams/EventSourceDecoderStream";
import EventSourceEncoderStream from "web-utility-streams/EventSourceEncoderStream";
import SplitLinesStream from "web-utility-streams/SplitLinesStream";

import { JSONParser as StreamParserJson } from "@streamparser/json-whatwg";
import bfj from "bfj";
import { EventSourceParserStream } from "eventsource-parser/stream";
import { encode as eventSourceEncode } from "eventsource-encoder";
import JSONStream from "JSONStream";
import ndjson from "ndjson";
import split2 from "split2";
import chain from "stream-chain";
import webChain from "stream-chain/web";
import { parser as streamJsonParser } from "stream-json";
import { disassembler as streamJsonDisassembler } from "stream-json/disassembler.js";
import { pick as streamJsonPick } from "stream-json/filters/pick.js";
import { parser as streamJsonJsonlParser } from "stream-json/jsonl/parser.js";
import { stringer as streamJsonJsonlStringer } from "stream-json/jsonl/stringer.js";
import { streamArray as streamJsonStreamArray } from "stream-json/streamers/stream-array.js";
import { stringer as streamJsonStringer } from "stream-json/stringer.js";
// @microsoft/fetch-event-source doesn't export its parser, and its ESM build
// can't be imported by Node.js, so we import the CommonJS build directly
import fetchEventSourceParse from "@microsoft/fetch-event-source/lib/cjs/parse.js";

import { events, records } from "./data.mjs";

const OURS = "web-utility-streams";

// Whole-file baselines need the entire file in a single string, and V8's
// maximum string length is about 512MB, so they're only run for small files.
const MAX_IN_MEMORY_BYTES = 512e6;

/**
 * Open a file as a web ReadableStream, using `FileHandle.readableWebStream()`,
 * which reads the file in 16KiB chunks. Libraries that use web streams read
 * the file this way.
 */
async function webFile(file) {
  const handle = await open(file);
  return {
    stream: handle.readableWebStream(),
    close: () => handle.close(),
  };
}

/**
 * Open a file as a Node.js Readable stream, using `fs.createReadStream`,
 * which reads the file in 64KiB chunks, for libraries that use Node.js
 * streams.
 */
async function nodeFile(file) {
  const readable = createReadStream(file);
  return readable;
}

async function countWeb(file, build, getRecord = (v) => v) {
  const { stream, close } = await webFile(file);
  try {
    let count = 0;
    let idSum = 0;
    for await (const value of build(stream)) {
      count++;
      idSum += getRecord(value).id;
    }
    return { count, idSum };
  } finally {
    await close();
  }
}

async function countIterable(iterable, getRecord = (v) => v) {
  let count = 0;
  let idSum = 0;
  for await (const value of iterable) {
    count++;
    idSum += getRecord(value).id;
  }
  return { count, idSum };
}

/**
 * For older streams that don't support async iteration
 */
function countEvents(stream, sources = [], getRecord = (v) => v) {
  return new Promise((resolve, reject) => {
    let count = 0;
    let idSum = 0;
    stream.on("data", (value) => {
      count++;
      idSum += getRecord(value).id;
    });
    stream.on("end", () => resolve({ count, idSum }));
    stream.on("error", reject);
    for (const source of sources) source.on("error", reject);
  });
}

/**
 * A web ReadableStream of `count` generated server-sent events
 */
function webEvents(count) {
  const iterator = events(count);
  return new ReadableStream({
    pull(controller) {
      const { done, value } = iterator.next();
      if (done) controller.close();
      else controller.enqueue(value);
    },
  });
}

/**
 * A web ReadableStream of `count` generated records
 */
function webRecords(count) {
  const iterator = records(count);
  return new ReadableStream({
    pull(controller) {
      const { done, value } = iterator.next();
      if (done) controller.close();
      else controller.enqueue(value);
    },
  });
}

/**
 * Consume encoded output. When verifying, the output is kept so it can be
 * checked by `parse`.
 */
function textSink(verify, parse) {
  let chars = 0;
  let text = verify ? "" : undefined;
  return {
    write(chunk) {
      const string = typeof chunk === "string" ? chunk : chunk.toString();
      chars += string.length;
      if (text !== undefined) text += string;
    },
    result() {
      if (text === undefined) return { chars };
      let count = 0;
      let idSum = 0;
      for (const value of parse(text)) {
        count++;
        idSum += value.id;
      }
      return { chars, count, idSum };
    },
  };
}

async function sinkWeb(readable, verify, parse) {
  const sink = textSink(verify, parse);
  for await (const chunk of readable) sink.write(chunk);
  return sink.result();
}

function sinkEvents(stream, verify, parse) {
  const sink = textSink(verify, parse);
  return new Promise((resolve, reject) => {
    stream.on("data", (chunk) => sink.write(chunk));
    stream.on("end", () => resolve(sink.result()));
    stream.on("error", reject);
  });
}

const parseJsonArray = (text) => JSON.parse(text);
const parseJsonResults = (text) => JSON.parse(text).results;
function* parseNdjson(text) {
  for (const line of text.split("\n")) if (line) yield JSON.parse(line);
}

/**
 * Each line of the NDJSON file starts with `{"id":N,`, so we can get the id
 * cheaply, to check every line was split correctly
 */
const lineRecord = (line) => ({ id: Number(line.slice(6, line.indexOf(","))) });

/**
 * An event's id, which is the id of the record in its data
 */
const eventRecord = (event) => ({ id: Number(event.id) });

/**
 * Parse `text/event-stream` output, checking each event's data is the JSON
 * of the record with the event's id. This is deliberately simple and
 * independent of the libraries being measured, so it only supports the "\n"
 * line endings they all output.
 */
function* parseSse(text) {
  for (const block of text.split("\n\n")) {
    if (!block) continue;
    let id;
    const data = [];
    for (const line of block.split("\n")) {
      if (line.startsWith("id: ")) id = Number(line.slice(4));
      else if (line.startsWith("data: ")) data.push(line.slice(6));
    }
    const record = JSON.parse(data.join("\n"));
    if (record.id !== id) {
      throw new Error(`Event ${id} has the data for record ${record.id}`);
    }
    yield record;
  }
}

function* splitLines(text) {
  for (const line of text.split("\n")) if (line) yield line;
}

/**
 * The read-the-file-only baseline, shared by every decoder suite
 */
const readFileBaseline = {
  library: "baseline",
  name: "Read the file through TextDecoderStream only",
  baseline: true,
  run: async ({ file }) => {
    const { stream, close } = await webFile(file);
    try {
      let chars = 0;
      for await (const chunk of stream.pipeThrough(new TextDecoderStream())) {
        chars += chunk.length;
      }
      return { chars };
    } finally {
      await close();
    }
  },
};

export const suites = {
  "decode-json": {
    format: "json",
    description:
      'Decode the elements of the "results" array in `{"total":N,"page":1,"results":[...],"next":null}`',
    cases: [
      {
        library: OURS,
        name: "JsonDecoderStream",
        ours: true,
        run: async ({ file }) =>
          countWeb(file, (stream) =>
            stream
              .pipeThrough(new TextDecoderStream())
              .pipeThrough(new JsonDecoderStream({ path: ["results"] })),
          ),
      },
      // {
      //   library: "stream-json",
      //   name: "chain([parser, pick, streamArray]) (Node streams)",
      //   run: async ({ file }) =>
      //     countIterable(
      //       (await nodeFile(file)).pipe(
      //         chain([
      //           streamJsonParser(),
      //           streamJsonPick({ filter: "results" }),
      //           streamJsonStreamArray(),
      //         ]),
      //       ),
      //       (item) => item.value,
      //     ),
      // },
      {
        library: "stream-json",
        name: "chain([parser, pick, streamArray]) (web streams)",
        run: async ({ file }) =>
          countWeb(
            file,
            (stream) =>
              stream.pipeThrough(
                webChain([
                  streamJsonParser(),
                  streamJsonPick({ filter: "results" }),
                  streamJsonStreamArray(),
                ]),
              ),
            (item) => item.value,
          ),
      },
      {
        library: "@streamparser/json",
        name: "JSONParser (web streams)",
        run: async ({ file }) =>
          countWeb(
            file,
            (stream) =>
              stream.pipeThrough(
                new StreamParserJson({
                  paths: ["$.results.*"],
                  keepStack: false,
                }),
              ),
            (item) => item.value,
          ),
      },
      {
        library: "JSONStream",
        name: 'JSONStream.parse("results.*")',
        run: async ({ file }) => {
          const source = await nodeFile(file);
          return countEvents(source.pipe(JSONStream.parse("results.*")), [
            source,
          ]);
        },
      },
      {
        library: "bfj",
        name: "bfj.match",
        maxSize: MAX_IN_MEMORY_BYTES, // It may look like a streaming API, but it doesn't really seem ot stream
        run: async ({ file }) =>
          countIterable(
            bfj.match(
              await nodeFile(file),
              (_key, _value, depth) => depth === 2,
            ),
          ),
      },
      {
        library: "baseline",
        name: "JSON.parse of the whole file (not streaming)",
        maxSize: MAX_IN_MEMORY_BYTES,
        run: async ({ file }) =>
          countIterable(
            JSON.parse(await fs.promises.readFile(file, "utf8")).results,
          ),
      },
      {
        library: "baseline",
        name: "Read the file through TextDecoderStream only",
        baseline: true,
        run: async ({ file }) => {
          const { stream, close } = await webFile(file);
          try {
            let chars = 0;
            for await (const chunk of stream.pipeThrough(
              new TextDecoderStream(),
            )) {
              chars += chunk.length;
            }
            return { chars };
          } finally {
            await close();
          }
        },
      },
    ],
  },

  "decode-ndjson": {
    format: "ndjson",
    description: "Decode a file with one JSON record per line",
    cases: [
      {
        library: OURS,
        name: "NewlineDelimitedJsonDecoderStream",
        ours: true,
        run: async ({ file }) =>
          countWeb(file, (stream) =>
            stream
              .pipeThrough(new TextDecoderStream())
              .pipeThrough(new NewlineDelimitedJsonDecoderStream()),
          ),
      },
      {
        library: "split2",
        name: "split2(JSON.parse)",
        run: async ({ file }) =>
          countIterable((await nodeFile(file)).pipe(split2(JSON.parse))),
      },
      {
        library: "ndjson",
        name: "ndjson.parse()",
        run: async ({ file }) =>
          countIterable((await nodeFile(file)).pipe(ndjson.parse())),
      },
      // {
      //   library: "stream-json",
      //   name: "jsonl/parser (Node streams)",
      //   run: async ({ file }) =>
      //     countIterable(
      //       (await nodeFile(file)).pipe(streamJsonJsonlParser.asStream()),
      //       (item) => item.value,
      //     ),
      // },
      {
        library: "@streamparser/json",
        name: 'JSONParser with separator "\\n" (web streams)',
        run: async ({ file }) =>
          countWeb(
            file,
            (stream) =>
              stream.pipeThrough(
                new StreamParserJson({ paths: ["$"], separator: "\n" }),
              ),
            (item) => item.value,
          ),
      },
      {
        library: "node:readline",
        name: "readline + JSON.parse",
        run: async ({ file }) =>
          countIterable(
            (async function* () {
              const lines = readline.createInterface({
                input: await nodeFile(file),
                crlfDelay: Infinity,
              });
              for await (const line of lines) {
                if (line) yield JSON.parse(line);
              }
            })(),
          ),
      },
      {
        library: "baseline",
        name: "split + JSON.parse of the whole file (not streaming)",
        maxSize: MAX_IN_MEMORY_BYTES,
        run: async ({ file }) =>
          countIterable(parseNdjson(await fs.promises.readFile(file, "utf8"))),
      },
      {
        library: "baseline",
        name: "Read the file through TextDecoderStream only",
        baseline: true,
        run: async ({ file }) => {
          const { stream, close } = await webFile(file);
          try {
            let chars = 0;
            for await (const chunk of stream.pipeThrough(
              new TextDecoderStream(),
            )) {
              chars += chunk.length;
            }
            return { chars };
          } finally {
            await close();
          }
        },
      },
    ],
  },

  "encode-json": {
    format: "json",
    encoder: true,
    description:
      "Encode generated records as the elements of a JSON array, discarding the output",
    cases: [
      {
        library: OURS,
        name: "JsonEncoderStream",
        ours: true,
        run: ({ count, verify }) =>
          sinkWeb(
            webRecords(count).pipeThrough(new JsonEncoderStream()),
            verify,
            parseJsonArray,
          ),
      },
      {
        library: OURS,
        name: 'JsonEncoderStream with path ["results"] and extraData',
        ours: true,
        run: ({ count, verify }) =>
          sinkWeb(
            webRecords(count).pipeThrough(
              new JsonEncoderStream({
                path: ["results"],
                extraData: { total: count, page: 1 },
              }),
            ),
            verify,
            parseJsonResults,
          ),
      },
      {
        library: "JSONStream",
        name: "JSONStream.stringify()",
        run: ({ count, verify }) =>
          sinkEvents(
            Readable.from(records(count)).pipe(JSONStream.stringify()),
            verify,
            parseJsonArray,
          ),
      },
      {
        library: "stream-json",
        name: "chain([disassembler, stringer]) (Node streams)",
        run: ({ count, verify }) =>
          sinkEvents(
            Readable.from(records(count)).pipe(
              chain([
                streamJsonDisassembler(),
                streamJsonStringer({ makeArray: true }),
              ]),
            ),
            verify,
            parseJsonArray,
          ),
      },
      {
        library: "baseline",
        name: "JSON.stringify of the whole array (not streaming)",
        maxSize: MAX_IN_MEMORY_BYTES,
        run: ({ count, verify }) => {
          const sink = textSink(verify, parseJsonArray);
          sink.write(JSON.stringify([...records(count)]));
          return sink.result();
        },
      },
      {
        library: "baseline",
        name: "Generate the records only",
        baseline: true,
        run: async ({ count }) => {
          let chars = 0;
          for await (const record of webRecords(count)) chars += record.id;
          return { chars: 0, ignored: chars };
        },
      },
    ],
  },

  "encode-ndjson": {
    format: "ndjson",
    encoder: true,
    description:
      "Encode generated records as newline delimited JSON, discarding the output",
    cases: [
      {
        library: OURS,
        name: "NewlineDelimitedJsonEncoderStream",
        ours: true,
        run: ({ count, verify }) =>
          sinkWeb(
            webRecords(count).pipeThrough(
              new NewlineDelimitedJsonEncoderStream(),
            ),
            verify,
            parseNdjson,
          ),
      },
      {
        library: "ndjson",
        name: "ndjson.stringify()",
        run: ({ count, verify }) =>
          sinkEvents(
            Readable.from(records(count)).pipe(ndjson.stringify()),
            verify,
            parseNdjson,
          ),
      },
      {
        library: "stream-json",
        name: "jsonl/stringer (Node streams)",
        run: ({ count, verify }) =>
          sinkEvents(
            Readable.from(records(count)).pipe(streamJsonJsonlStringer()),
            verify,
            parseNdjson,
          ),
      },
      {
        library: "baseline",
        name: "Generate the records only",
        baseline: true,
        run: async ({ count }) => {
          let chars = 0;
          for await (const record of webRecords(count)) chars += record.id;
          return { chars: 0, ignored: chars };
        },
      },
      {
        library: "baseline",
        name: "JSON.stringify + .join('\\n')",
        baseline: true,
        run: async ({ count, verify }) => {
          const sink = textSink(verify, parseNdjson);
          let chars = 0;
          for await (const record of webRecords(count)) {
            sink.write(JSON.stringify(record) + "\n");
          }
          return sink.result();
        },
      },
    ],
  },

  "split-lines": {
    format: "ndjson",
    description:
      "Split a file with one JSON record per line into lines, without parsing them",
    cases: [
      {
        library: OURS,
        name: "SplitLinesStream",
        ours: true,
        run: async ({ file }) =>
          countWeb(
            file,
            (stream) =>
              stream
                .pipeThrough(new TextDecoderStream())
                .pipeThrough(new SplitLinesStream()),
            lineRecord,
          ),
      },
      {
        library: "split2",
        name: "split2()",
        run: async ({ file }) =>
          countIterable((await nodeFile(file)).pipe(split2()), lineRecord),
      },
      {
        library: "node:readline",
        name: "readline.createInterface",
        run: async ({ file }) =>
          countIterable(
            readline.createInterface({
              input: await nodeFile(file),
              crlfDelay: Infinity,
            }),
            lineRecord,
          ),
      },
      {
        library: "baseline",
        name: "split of the whole file (not streaming)",
        maxSize: MAX_IN_MEMORY_BYTES,
        run: async ({ file }) =>
          countIterable(
            splitLines(await fs.promises.readFile(file, "utf8")),
            lineRecord,
          ),
      },
      readFileBaseline,
    ],
  },

  "decode-sse": {
    format: "sse",
    description:
      "Decode a `text/event-stream` file with one event per record, each with an id, a type, and the record's JSON as its data (split over multiple lines for every 10th event)",
    cases: [
      {
        library: OURS,
        name: "EventSourceDecoderStream",
        ours: true,
        run: async ({ file }) =>
          countWeb(
            file,
            (stream) =>
              stream
                .pipeThrough(new TextDecoderStream())
                .pipeThrough(new EventSourceDecoderStream()),
            eventRecord,
          ),
      },
      {
        library: "eventsource-parser",
        name: "EventSourceParserStream",
        run: async ({ file }) =>
          countWeb(
            file,
            (stream) =>
              stream
                .pipeThrough(new TextDecoderStream())
                .pipeThrough(new EventSourceParserStream()),
            eventRecord,
          ),
      },
      {
        library: "@microsoft/fetch-event-source",
        name: "getLines + getMessages (callbacks, decodes bytes)",
        run: async ({ file }) => {
          const { stream, close } = await webFile(file);
          let count = 0;
          let idSum = 0;
          const onChunk = fetchEventSourceParse.getLines(
            fetchEventSourceParse.getMessages(
              () => {},
              () => {},
              (message) => {
                count++;
                idSum += Number(message.id);
              },
            ),
          );
          try {
            for await (const chunk of stream) onChunk(chunk);
          } finally {
            await close();
          }
          return { count, idSum };
        },
      },
      readFileBaseline,
    ],
  },

  "encode-sse": {
    format: "sse",
    encoder: true,
    description:
      "Encode generated events, each with an id, a type, and a record's JSON as its data (multi-line for every 10th event), discarding the output",
    cases: [
      {
        library: OURS,
        name: "EventSourceEncoderStream",
        ours: true,
        run: ({ count, verify }) =>
          sinkWeb(
            webEvents(count).pipeThrough(new EventSourceEncoderStream()),
            verify,
            parseSse,
          ),
      },
      {
        library: "eventsource-encoder",
        name: "encode() in a TransformStream",
        run: ({ count, verify }) =>
          sinkWeb(
            webEvents(count).pipeThrough(
              new TransformStream({
                transform(event, controller) {
                  controller.enqueue(
                    eventSourceEncode({
                      event: event.type,
                      id: event.id,
                      data: event.data,
                    }),
                  );
                },
              }),
            ),
            verify,
            parseSse,
          ),
      },
      {
        library: "baseline",
        name: "Generate the events only",
        baseline: true,
        run: async ({ count }) => {
          let chars = 0;
          for await (const event of webEvents(count))
            chars += event.data.length;
          return { chars: 0, ignored: chars };
        },
      },
    ],
  },
};
