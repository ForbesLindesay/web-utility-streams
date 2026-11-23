# web-utility-streams

Various utility streams that are very useful for working with web streams.

## Contents

**Transform streams**

- [`AddPrefixStream`](#addprefixstream) - Emit some chunks before the rest of the stream.
- [`CsvDecoderStream`](#csvdecoderstream) - Decode CSV text into rows.
- [`CsvEncoderStream`](#csvencoderstream) - Encode rows as CSV text.
- [`DebounceStream`](#debouncestream) - Emit at most one chunk per period, dropping the rest.
- [`DebugStream`](#debugstream) - Call a function with each chunk as it passes through.
- [`EventSourceDecoderStream`](#eventsourcedecoderstream) - Decode `text/event-stream` (Server-Sent Events) text into events.
- [`EventSourceEncoderStream`](#eventsourceencoderstream) - Encode events as `text/event-stream` (Server-Sent Events) text.
- [`FilterMapStream`](#filtermapstream) - Map each chunk, dropping any that map to `undefined`.
- [`FilterStream`](#filterstream) - Only keep the chunks that match a predicate.
- [`JsonDecoderStream`](#jsondecoderstream) - Emit the elements of a JSON array or object as they are parsed.
- [`JsonEncoderStream`](#jsonencoderstream) - Encode a stream of values as a JSON array or object.
- [`JsonTokenDecoderStream`](#jsontokendecoderstream) - Split JSON text into tokens.
- [`MapStream`](#mapstream) - Transform each chunk with a sync or async function.
- [`NewlineDelimitedJsonDecoderStream`](#newlinedelimitedjsondecoderstream) - Parse each line of NDJSON text into a value.
- [`NewlineDelimitedJsonEncoderStream`](#newlinedelimitedjsonencoderstream) - Encode each value as a line of NDJSON.
- [`SplitLinesStream`](#splitlinesstream) - Split text into lines.
- [`TimeoutStream`](#timeoutstream) - Error the stream if no chunk arrives for too long.
- [`TransformPipelineStream`](#transformpipelinestream) - Combine several transform streams into one.
- [`TransformStreamWithCancel`](#transformstreamwithcancel) - A `TransformStream` that calls the transformer's `cancel` method, in any runtime.

**Readable streams**

- [`ConcatStream`](#concatstream) - Join streams one after another.
- [`DeferredStream`](#deferredstream) - Return a stream now whose contents come from a promise.
- [`EmptyStream`](#emptystream) - A stream that closes immediately.
- [`LazyStream`](#lazystream) - Create the underlying stream when it is first read.
- [`UnionStream`](#unionstream) - Merge streams, emitting chunks as they arrive.

**Utilities**

- [`consumers`](#consumers) - Read a whole stream as text, JSON or bytes.
- [`readableStreamToGenerator`](#readablestreamtogenerator) - Convert a stream into an async generator that cancels promptly.

## Installation

```bash
npm install web-utility-streams
```

Each utility is a separate module, imported by name, e.g. `web-utility-streams/MapStream`.

All of the transform streams are safe to use in a pipeline: if any part of the pipeline errors or is cancelled, the error is propagated, and can be handled with a single `try`/`catch` around the call to `pipeTo`, without causing unhandled promise rejections:

```ts
try {
  await source.pipeThrough(new MapStream(fn)).pipeTo(destination);
} catch (ex) {
  // Handle errors from the source, the transform, or the destination
}
```

## Transform streams

### AddPrefixStream

Emit the chunks of a prefix, then pass the rest of the stream through unchanged. The prefix can be an array (or any other iterable), an async iterable, a `ReadableStream`, or a promise for one of these.

```ts
import AddPrefixStream from "web-utility-streams/AddPrefixStream";

const events = updates.pipeThrough(
  new AddPrefixStream(getCurrentState().then((state) => [state])),
);
```

The prefix is only read as the output is read, and written chunks wait until it's done. If the prefix errors, the stream errors. If the stream is cancelled or errors first, the prefix is closed, so a generator's `finally` blocks run and a `ReadableStream` is cancelled.

### CsvDecoderStream

Decode a stream of CSV text into rows, where each row is an array of strings. The text can be split into chunks at any point, and each row is emitted as soon as it is complete.

```ts
import CsvDecoderStream from "web-utility-streams/CsvDecoderStream";

const response = await fetch("https://example.com/users.csv");
const rows = response
  .body!.pipeThrough(new TextDecoderStream())
  .pipeThrough(new CsvDecoderStream());

for await (const [id, name, email] of rows) {
  console.log(id, name, email);
}
```

Fields can be quoted, so they can contain delimiters, quotes and line breaks. A quote within a quoted field is escaped by doubling it (`""`). Spaces and tabs around a quoted field are ignored, but spaces in unquoted fields are kept. Rows can end with `"\r\n"`, `"\n"` or `"\r"`, a final line break doesn't produce an extra row, and a byte order mark at the start of the text is removed. Invalid CSV (text after a closing quote, or a quoted field with no closing quote) errors the stream with a `SyntaxError`. These options can be passed to the constructor:

| Option                     | Description                                                                                                                                                                   | Default                                 |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------- |
| `delimiter`                | The character that separates fields, e.g. `","`, `";"` or `"\t"`.                                                                                                             | Guessed from the first line (see below) |
| `maxCharsToGuessDelimiter` | If `delimiter` isn't passed, the most characters to read before guessing it, if the first line is longer than this (see below).                                               | `10000`                                 |
| `quote`                    | The character used to quote fields, or `false` to treat quotes like any other character.                                                                                      | `'"'`                                   |
| `escape`                   | The character that escapes a quote in a quoted field. If it isn't the quote character (e.g. `"\\"`), it also escapes itself, and is kept as it is before any other character. | The quote character                     |
| `ignoreEmpty`              | Skip blank lines (lines that are empty or only contain spaces and tabs), rather than emitting them as a row with one field.                                                   | `false`                                 |

If you don't pass a `delimiter`, it's guessed from the first line of the text: whichever of `","`, `";"` and `"\t"` appears most often, not counting any in quoted fields. If there's a tie, or none of them appear, `","` is preferred, then `";"`. No rows are emitted until the end of the first line has been read, or `maxCharsToGuessDelimiter` characters (10,000 by default), if that comes first. That limit is checked after each chunk, so all of the chunk that reaches it is used to guess. If you know which delimiter your data uses, it's best to pass it, as a file with a single column could contain one of these characters in its first line. You must pass a `delimiter` if `quote` or `escape` is one of these characters.

The rows are arrays, so if your CSV has a header row, you can use the first row to turn the rest into objects:

```ts
let headers: string[] | undefined;
const users = rows.pipeThrough(
  new FilterMapStream((row) => {
    if (!headers) {
      headers = row;
      return undefined;
    }
    return Object.fromEntries(headers.map((header, i) => [header, row[i]]));
  }),
);
```

The parser is also exported as `CsvDecoder`, if you want to parse CSV text without using streams. Pass a function to call with each row (and any options) to its constructor, then call `parse(chunk, isFinal)` for each chunk of text, with `isFinal: true` for the last one (which can be empty):

```ts
import { CsvDecoder } from "web-utility-streams/CsvDecoderStream";

const rows: string[][] = [];
const decoder = new CsvDecoder((row) => rows.push(row));
decoder.parse('id,name\n1,"Smith, Jane"\n2,', false);
decoder.parse("John Doe", true);
// rows: [["id", "name"], ["1", "Smith, Jane"], ["2", "John Doe"]]
```

Its results match [fast-csv](https://www.npmjs.com/package/fast-csv)'s for the same options (fast-csv always uses `","` by default, rather than guessing), except that blank lines are emitted as `[""]` rather than `[]`, and fields that only contain spaces are always kept as they are. In our benchmarks, it's about twice as fast as papaparse, 5 times as fast as csv-parse, and 7-10 times as fast as fast-csv (see [benchmarks/results/decode-csv.md](benchmarks/results/decode-csv.md)).

### CsvEncoderStream

Encode a stream of rows (arrays of strings) as CSV text. By default, fields are quoted if they contain the delimiter, a quote or a line break, and quotes within them are escaped. Unless `quoteMode` is `"NEVER"`, a row with a single field that is empty (or only contains spaces and tabs) is always quoted, so it isn't written as a blank line. An empty row (`[]`) is written as a blank line. Otherwise, the output is the same as [fast-csv](https://www.npmjs.com/package/fast-csv)'s `format` for the same options. It can be decoded by `CsvDecoderStream`. `delimiter`, `quote` and `escape` must each be a single character that isn't a line break, and `delimiter` and `quote` must be different (unless `quoteMode` is `"NEVER"`). It's about 1.5 times as fast as fast-csv, and twice as fast as csv-stringify (see [benchmarks/results/encode-csv.md](benchmarks/results/encode-csv.md)).

```ts
import CsvEncoderStream from "web-utility-streams/CsvEncoderStream";

return new Response(
  users
    .pipeThrough(
      new MapStream((user) => [String(user.id), user.name, user.email]),
    )
    .pipeThrough(new CsvEncoderStream({ includeEndRowDelimiter: true }))
    .pipeThrough(new TextEncoderStream()),
  { headers: { "Content-Type": "text/csv" } },
);
```

Every field must be a string, so convert other values (e.g. numbers, or `null` to `""`) before encoding them. Null characters (`"\0"`) are removed. These options can be passed to the constructor:

| Option                   | Description                                                                                                                                                      | Default             |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------- |
| `delimiter`              | The text between fields.                                                                                                                                         | `","`               |
| `rowDelimiter`           | The text between rows.                                                                                                                                           | `"\n"`              |
| `includeEndRowDelimiter` | Also write `rowDelimiter` after the last row. Without this, each row's chunk starts with `rowDelimiter` (except the first), rather than ending with it.          | `false`             |
| `quote`                  | The character used to quote fields.                                                                                                                              | `'"'`               |
| `escape`                 | The character used to escape quotes in quoted fields. If it isn't the quote character, it is also used to escape itself.                                         | The quote character |
| `quoteMode`              | `"AUTO"` quotes fields that need it (those containing the delimiter, a quote or a line break), `"ALWAYS"` quotes every field, and `"NEVER"` never quotes fields. | `"AUTO"`            |
| `writeBOM`               | Write a byte order mark at the start of the output, which helps some programs (e.g. Excel) detect that the text is UTF-8.                                        | `false`             |

The encoder is also exported as `CsvEncoder`, whose `formatRow(row, isFirst)` method returns the text for one row. Pass `isFirst: true` for the first row, so the byte order mark (if `writeBOM` is set) is written before it, and the row delimiter isn't written before it.

### DebounceStream

Emit at most one chunk every `debounceMs` milliseconds, dropping the rest. This is useful for things like progress updates, where only the latest value matters.

```ts
import DebounceStream from "web-utility-streams/DebounceStream";

const progress = progressEvents.pipeThrough(
  new DebounceStream({ debounceMs: 100 }),
);
```

A period starts when a chunk is written and nothing has been emitted in the last `debounceMs`. The `mode` option controls which chunks of each period are emitted:

- `"BOTH"` (the default): the first chunk straight away, and the most recent chunk at the end of the period, if there was another.
- `"LEADING"`: only the first chunk, straight away.
- `"TRAILING"`: only the most recent chunk, at the end of the period.

Emitting a trailing chunk starts a new period. Pass `restartTimerOnChunk: true` to extend the period each time a chunk is written, so it only ends after a gap of `debounceMs`. When the input closes, a waiting trailing chunk is emitted straight away. Writes never wait for the output to be read; if it falls behind, only the most recent chunk is kept.

### DebugStream

Pass every chunk through unchanged, calling a function with each one as it goes past. By default, chunks are logged with `console.log`. This is useful for debugging a pipeline. If the function throws, the stream is errored.

```ts
import DebugStream from "web-utility-streams/DebugStream";

const events = response
  .body!.pipeThrough(new TextDecoderStream())
  .pipeThrough(new DebugStream((text) => console.debug("Received:", text)))
  .pipeThrough(new EventSourceDecoderStream())
  .pipeThrough(new DebugStream());
```

### EventSourceDecoderStream

Decode a stream of text in the `text/event-stream` format into `EventSourceEvent` objects (`{type, data, id, retryMilliseconds}`), following the parsing rules in the HTML spec. The text can be split into chunks at any point. The `id` and `retryMilliseconds` properties of each event are the values of any `id` and `retry` fields received since the previous event, or `null` if there were none. An incomplete event at the end of the stream is discarded.

To be notified of every `id` and `retry` field as soon as it is received, even if no event follows it, pass `onId` and/or `onRetry` options. This is useful for implementing reconnection, which needs the last event ID and the latest reconnection time. If either callback throws, the stream is errored.

```ts
let lastEventId = "";
let reconnectDelay = 3000;

const events = response.body!.pipeThrough(new TextDecoderStream()).pipeThrough(
  new EventSourceDecoderStream({
    onId: (id) => (lastEventId = id),
    onRetry: (retryMilliseconds) => (reconnectDelay = retryMilliseconds),
  }),
);
```

```ts
import EventSourceDecoderStream from "web-utility-streams/EventSourceDecoderStream";

const response = await fetch("https://example.com/events");
const events = response
  .body!.pipeThrough(new TextDecoderStream())
  .pipeThrough(new EventSourceDecoderStream());

for await (const event of events) {
  console.log(event.type, event.data);
}
```

The `EventSourceEvent` type for the decoded events is also exported from this module:

```ts
import type { EventSourceEvent } from "web-utility-streams/EventSourceDecoderStream";

function handleEvent(event: EventSourceEvent) {
  if (event.type === "update") {
    console.log(JSON.parse(event.data));
  }
}
```

### EventSourceEncoderStream

Encode events in the [`text/event-stream`](https://html.spec.whatwg.org/multipage/server-sent-events.html) (Server-Sent Events) format. Each event needs a `data` string, and can optionally have a `type` (defaults to `"message"`), an `id` and a `retryMilliseconds` reconnection time (sent as the `retry` field). Multi-line `data` is supported. Lines end with `"\n"` by default, but you can pass `{ lineEnding: "\r\n" }` or `{ lineEnding: "\r" }` to the constructor. Events that can't be encoded (e.g. an `id` that contains a line break) error the stream.

```ts
import EventSourceEncoderStream from "web-utility-streams/EventSourceEncoderStream";

return new Response(
  updates
    .pipeThrough(
      new MapStream((update) => ({
        type: "update",
        id: update.id,
        data: JSON.stringify(update),
      })),
    )
    .pipeThrough(new EventSourceEncoderStream())
    .pipeThrough(new TextEncoderStream()),
  { headers: { "Content-Type": "text/event-stream" } },
);
```

### FilterMapStream

Transform each chunk using a sync or async function, dropping any chunks for which the function returns `undefined`. This is useful when you want to filter and map in a single step.

```ts
import FilterMapStream from "web-utility-streams/FilterMapStream";

const activeUserEmails = userIds.pipeThrough(
  new FilterMapStream(async (id: string) => {
    const user = await db.users.get(id);
    return user.active ? user.email : undefined;
  }),
);
```

### FilterStream

Only keep chunks for which the filter function returns `true`. Type guards can be used to narrow the type of the output.

```ts
import FilterStream from "web-utility-streams/FilterStream";

const strings = values.pipeThrough(
  new FilterStream((v: unknown): v is string => typeof v === "string"),
);
```

### JsonDecoderStream

Decode a JSON array or object from a stream of text, emitting each element as soon as it has been parsed, without waiting for the rest of the document. This lets you start processing a large JSON response before it has finished downloading. For an array, each element is emitted. For an object, each property is emitted as a `[key, value]` tuple. By default the root of the document is streamed; if the array or object is nested inside other objects, pass the keys that lead to it as `path`.

The whole document is validated, but only the elements being emitted are kept in memory, unless you pass `onExtraData` (see below). If the document isn't valid JSON, the stream errors when the problem is found. For speed, each element (and each object or array being discarded) is parsed in one go once its end is reached, so a problem inside one is found at its end. Any single value longer than `maxValueLength` characters (default 100 MiB) errors the stream as soon as that length is exceeded, so a huge or never-ending value can't use unlimited memory. If there's no array or object at `path`, the stream errors once the whole document has been parsed. Because elements are emitted as soon as they're parsed, some may be emitted before an error later in the document is found. If a streamed object repeats a key, a tuple is emitted for each occurrence.

```ts
import JsonDecoderStream from "web-utility-streams/JsonDecoderStream";

// Response body: {"total": 1000, "results": [{...}, {...}, ...]}
const response = await fetch("https://example.com/api/search");
const results = response
  .body!.pipeThrough(new TextDecoderStream())
  .pipeThrough(new JsonDecoderStream({ path: ["results"] }));

for await (const result of results) {
  console.log(result);
}

// Response body: {"users": {"alice": {...}, "bob": {...}, ...}}
const users = (await fetch("https://example.com/api/users"))
  .body!.pipeThrough(new TextDecoderStream())
  .pipeThrough(new JsonDecoderStream({ path: ["users"] }));

for await (const [name, user] of users as ReadableStream<[string, unknown]>) {
  console.log(name, user);
}
```

To also get the rest of the document, pass an `onExtraData` callback. Once the whole document has been parsed, it's called with the document with the value at `path` replaced by `undefined` (the key is still there). This is the same shape as the `extraData` option of `JsonEncoderStream`, so the two round trip. If `path` is empty, it's called with `undefined`. It's called as soon as the end of the document is reached, which may be before you've read all the elements from the stream, and it isn't called if the document turns out to be invalid before then. If it throws, the stream errors. Note that if `onExtraData` is provided, everything outside `path` is kept in memory until the end of the document.

```ts
// Response body: {"total": 1000, "results": [...], "next": "abc"}
let pageInfo: unknown;
const results = response.body!.pipeThrough(new TextDecoderStream()).pipeThrough(
  new JsonDecoderStream({
    path: ["results"],
    onExtraData: (data) => (pageInfo = data),
  }),
);

for await (const result of results) {
  console.log(result);
}
console.log(pageInfo); // { total: 1000, results: undefined, next: "abc" }
```

### JsonEncoderStream

Encode a stream of values as a single JSON document, with the values as the elements of an array. With `{ mode: "object" }`, the input must be `[key, value]` tuples, which become the properties of an object. This is the opposite of `JsonDecoderStream`, and lets you start sending a large JSON response before all the data is ready.

By default the array or object is the root of the document. To nest it inside other objects, pass the keys that lead to it as `path`, and any other properties as `extraData` (which must not have a value at `path`). The other properties are written before the streamed values, so readers get them straight away. Values are encoded the same way `JSON.stringify` encodes them inside an array or object: `undefined`, functions and symbols become `null` in an array, and those properties are left out of an object. Values `JSON.stringify` can't encode, such as `BigInt` values or circular structures, error the stream.

The start of the document is output immediately, each value is output as its own chunk, and the end of the document is output when the input closes.

```ts
import JsonEncoderStream from "web-utility-streams/JsonEncoderStream";

// Response body: {"total":1000,"results":[{...},{...},...]}
return new Response(
  results
    .pipeThrough(
      new JsonEncoderStream({ path: ["results"], extraData: { total: 1000 } }),
    )
    .pipeThrough(new TextEncoderStream()),
  { headers: { "Content-Type": "application/json" } },
);

// Response body: {"alice":{...},"bob":{...},...}
const usersJson = users
  .pipeThrough(new MapStream((user: User) => [user.name, user]))
  .pipeThrough(new JsonEncoderStream({ mode: "object" }));
```

### JsonTokenDecoderStream

Split a stream of JSON text into tokens. There are three types of token: `PUNCTUATION` tokens, whose `value` is one of `{`, `}`, `[`, `]`, `:` or `,`; `KEY` tokens, for object keys, whose `value` is the decoded key; and `LITERAL` tokens, whose `value` is the decoded string, number, boolean or `null`. A string is a `KEY` if it is directly inside an object, immediately after the `{` or a `,`. Each token also has the `position` of its first character in the text. The text can be split into chunks at any point, and whitespace is skipped. The text must be a single JSON value (usually an object or array). Each token is checked to be valid, and to be in a valid place in the document (e.g. brackets match, and every key is followed by a `:`), so you can use this to build your own streaming JSON parser without checking the structure yourself. A problem (e.g. a bad escape sequence in a string, or a missing comma) errors the stream with a `SyntaxError` as soon as it's found, after emitting the tokens before it, and so does the input ending before the value is complete. A number, boolean or `null` is only emitted once the next character arrives (or the input ends), as it could continue in the next chunk.

```ts
import JsonTokenDecoderStream from "web-utility-streams/JsonTokenDecoderStream";

const tokens = readableStream.pipeThrough(new JsonTokenDecoderStream());
// For the text `{"a": [1, true]}`:
// { type: "PUNCTUATION", value: "{", position: 0 }
// { type: "KEY", value: "a", position: 1 }
// { type: "PUNCTUATION", value: ":", position: 4 }
// { type: "PUNCTUATION", value: "[", position: 6 }
// { type: "LITERAL", value: 1, position: 7 }
// { type: "PUNCTUATION", value: ",", position: 8 }
// { type: "LITERAL", value: true, position: 10 }
// { type: "PUNCTUATION", value: "]", position: 14 }
// { type: "PUNCTUATION", value: "}", position: 15 }
```

The `JsonToken` type is also exported from this module.

### MapStream

Transform each chunk using a sync or async function. Chunks are processed one at a time, and output in the same order as the input. If the function throws, the stream is errored.

```ts
import MapStream from "web-utility-streams/MapStream";

const users = userIds.pipeThrough(
  new MapStream(async (id: string) => await db.users.get(id)),
);
```

### NewlineDelimitedJsonDecoderStream

Decode a stream of text in the newline delimited JSON format. Text can be split across chunks in any way, any line ending is supported, and blank lines are ignored. By default, a line of invalid JSON errors the stream, but you can pass an `onError` option to handle invalid lines yourself, e.g. by skipping them or enqueuing a replacement value.

```ts
import NewlineDelimitedJsonDecoderStream from "web-utility-streams/NewlineDelimitedJsonDecoderStream";

const response = await fetch("https://example.com/results.ndjson");
const results = response.body!.pipeThrough(new TextDecoderStream()).pipeThrough(
  new NewlineDelimitedJsonDecoderStream({
    onError: (invalidLine, error, controller) => {
      console.warn(`Skipping invalid line: ${invalidLine}`);
    },
  }),
);
```

### NewlineDelimitedJsonEncoderStream

Encode each chunk as a line of JSON ([NDJSON](https://github.com/ndjson/ndjson-spec)). Lines end with `"\n"` by default, but you can pass `{ lineEnding: "\r\n" }` or `{ lineEnding: "\r" }` to the constructor. Values that can't be represented as JSON (e.g. `undefined`, `BigInt` values or circular structures) error the stream.

```ts
import NewlineDelimitedJsonEncoderStream from "web-utility-streams/NewlineDelimitedJsonEncoderStream";

return new Response(
  results
    .pipeThrough(new NewlineDelimitedJsonEncoderStream())
    .pipeThrough(new TextEncoderStream()),
  { headers: { "Content-Type": "application/x-ndjson" } },
);
```

### SplitLinesStream

Split a stream of text into lines. Lines may end with `\n`, `\r\n` or `\r`, and the line endings are not included in the output. Blank lines are preserved. A final line without a line ending is emitted when the input ends.

```ts
import SplitLinesStream from "web-utility-streams/SplitLinesStream";

const response = await fetch("https://example.com/log.txt");
const lines = response
  .body!.pipeThrough(new TextDecoderStream())
  .pipeThrough(new SplitLinesStream());

for await (const line of lines) {
  console.log(line);
}
```

### TimeoutStream

Pass chunks through unchanged, but error the stream if no chunk is written for `timeoutMs` milliseconds. This is useful for detecting a connection that has stalled, such as a streaming API response that stops sending data without closing. When the timeout is reached, the stream is errored with a `TimeoutStreamError` (which has `code: "TIMEOUT_STREAM_TIMEOUT"`), so in a pipeline the source is cancelled and the error is thrown from `pipeTo`.

```ts
import TimeoutStream, {
  TimeoutStreamError,
} from "web-utility-streams/TimeoutStream";

try {
  await response
    .body!.pipeThrough(new TimeoutStream({ timeoutMs: 30_000 }))
    .pipeTo(destination);
} catch (ex) {
  if (ex instanceof TimeoutStreamError) {
    // No data was received for 30 seconds
  }
}
```

The timeout starts when the stream is created, and is stopped when the input closes, or the stream is cancelled or errored.

Chunks are only written to a `TimeoutStream` when its output has room for them (it reads one chunk ahead of the output), so if the output is read slowly, for example because the destination of a pipeline is slow, the input is held back by backpressure, and that can cause a timeout too. Choose a timeout that allows for the slowest reader you expect, or put the `TimeoutStream` before any slow steps in the pipeline.

`timeoutMs` can also be a function, which is called to choose the first timeout (with `remainingMs` of `0` and no chunk), and then each time a chunk is written, with the time that was left of the previous timeout and the chunk. For example, when streaming audio to a player that reads it in real time, each chunk gives the input longer before the next one is needed:

```ts
await audioStream
  .pipeThrough(
    new TimeoutStream<AudioChunk>({
      // Allow 5 seconds for the first chunk. After that, add the length of
      // each chunk to the time that was left, as the player won't ask for
      // more until it has played it.
      timeoutMs: (remainingMs, chunk) =>
        chunk === undefined ? 5_000 : remainingMs + getAudioDuration(chunk),
    }),
  )
  .pipeTo(audioPlayer);
```

Pass `onTimeout` to choose what happens when the timeout is reached. It's called with the stream's `TransformStreamDefaultController`, so it can error the stream with your own error, end it cleanly with `controller.terminate()`, or enqueue a chunk. If it doesn't error or terminate the stream, the timeout starts again when the next chunk is written. If it throws, the stream is errored.

```ts
const stream = new TimeoutStream({
  timeoutMs: 10_000,
  onTimeout: (controller) => controller.error(new MyTimeoutError()),
});
```

### TransformPipelineStream

Combine a series of transform streams into a single transform stream. The function you pass is given an `input` with a `pipeThrough` method, and must return the final readable stream.

```ts
import TransformPipelineStream from "web-utility-streams/TransformPipelineStream";
import EventSourceDecoderStream, {
  type EventSourceEvent,
} from "web-utility-streams/EventSourceDecoderStream";

class BytesToEventsStream extends TransformPipelineStream<
  Uint8Array,
  EventSourceEvent
> {
  constructor() {
    super((input) =>
      input
        .pipeThrough(new TextDecoderStream())
        .pipeThrough(new EventSourceDecoderStream()),
    );
  }
}

const events = response.body!.pipeThrough(new BytesToEventsStream());
```

### TransformStreamWithCancel

`TransformStream`, with support for the transformer's `cancel` method in every runtime. `cancel` is called with the reason when the readable side is cancelled or the writable side is aborted (e.g. because another part of a pipeline errored), which makes it the place to clean up resources such as timers or connections. It was added to the Streams standard in 2023, but isn't supported everywhere yet.

In runtimes that support `cancel` (such as Node.js), `TransformStreamWithCancel` is just `TransformStream`. Elsewhere it's `TransformStreamWithCancelPolyfill`, which implements the standard's `TransformStream` with a `ReadableStream` and a `WritableStream`. It supports everything `TransformStream` does, including queuing strategies and backpressure, but `instanceof TransformStream` is `false`.

```ts
import TransformStreamWithCancel from "web-utility-streams/TransformStreamWithCancel";

const subscription = subscribe();
const stream = new TransformStreamWithCancel<Message, Message>({
  transform(message, controller) {
    if (subscription.isWanted(message)) controller.enqueue(message);
  },
  flush() {
    subscription.unsubscribe();
  },
  cancel(reason) {
    subscription.unsubscribe();
  },
});
```

You can extend it like `TransformStream`, e.g. `class MyStream extends TransformStreamWithCancel<In, Out>`. The module also exports `supportsTransformerCancel`, which is `true` if the native `TransformStream` supports `cancel`, the `TransformStreamWithCancelPolyfill` class (if you want to use it regardless), and the `TransformerWithCancel` type (TypeScript's own `Transformer` type doesn't include `cancel` yet).

## Readable streams

### ConcatStream

Join multiple readable streams into a single stream, emitting all the chunks from the first stream, then all the chunks from the second stream, and so on. The output closes once all the sources have closed, if any source errors the output errors and the other sources are cancelled, and cancelling the output cancels all the sources. All the sources are locked immediately, but each one is only read once the streams before it have closed.

```ts
import ConcatStream from "web-utility-streams/ConcatStream";

const allPages = new ConcatStream([page1Results, page2Results, page3Results]);
```

An optional [queuing strategy](https://developer.mozilla.org/en-US/docs/Web/API/ReadableStream/ReadableStream#queuingstrategy) can be passed as the second argument, to control how far ahead of the reader the sources are read. By default, one chunk is read ahead. With `{ highWaterMark: 0 }`, the sources are only read when the output is read.

```ts
const allPages = new ConcatStream([page1Results, page2Results], {
  highWaterMark: 0,
});
```

### DeferredStream

A readable stream that you can return immediately, when the stream with the actual content is only available later, from a `Promise<ReadableStream<T>>`. Once the promise resolves, the chunks of that stream are passed through. If the promise rejects, the stream errors with that error when read. Errors from the resolved stream are forwarded, and cancelling the `DeferredStream` cancels the resolved stream, even if the promise hasn't resolved yet. Nothing is read from the resolved stream until the `DeferredStream` is read.

```ts
import DeferredStream from "web-utility-streams/DeferredStream";

function getResults(query: string): ReadableStream<Result> {
  return new DeferredStream(
    fetch(`/api/results?q=${encodeURIComponent(query)}`).then((response) =>
      response
        .body!.pipeThrough(new TextDecoderStream())
        .pipeThrough(new NewlineDelimitedJsonDecoderStream()),
    ),
  );
}
```

### EmptyStream

A readable stream that is closed immediately, without emitting any chunks. Its chunk type is `never`, so it can be returned wherever a `ReadableStream<T>` is expected.

```ts
import EmptyStream from "web-utility-streams/EmptyStream";

function getEvents(user: User | null): ReadableStream<Event> {
  if (!user) return new EmptyStream();
  return getEventsForUser(user);
}
```

### LazyStream

A readable stream that only creates its underlying stream when it is first read, by calling a function that returns a `ReadableStream` or a `Promise<ReadableStream<T>>`. This is useful when creating the stream is expensive, or has side effects such as making a request, and the stream may never be read. Unlike [`DeferredStream`](#deferredstream), which takes a promise for work that has already started, nothing happens until the stream is read.

The function is called at most once. If it throws or its promise rejects, the stream errors with that error. Errors from the underlying stream are forwarded. Cancelling the `LazyStream` before it is read does not call the function, and cancelling it later cancels the underlying stream, even if the promise hasn't resolved yet. Nothing is read from the underlying stream until the `LazyStream` is read.

```ts
import LazyStream from "web-utility-streams/LazyStream";

const results = new LazyStream(async () => {
  const response = await fetch("/api/results");
  return response
    .body!.pipeThrough(new TextDecoderStream())
    .pipeThrough(new NewlineDelimitedJsonDecoderStream());
});
```

### UnionStream

Merge multiple readable streams into a single stream, emitting chunks in the order they become available. The output closes once all the sources have closed. If any source errors, the output errors and the other sources are cancelled. Cancelling the output cancels all the sources. Backpressure is respected, so the sources are only read from while the output is being read.

```ts
import UnionStream from "web-utility-streams/UnionStream";

const allEvents = new UnionStream([userEvents, systemEvents]);
```

An optional [queuing strategy](https://developer.mozilla.org/en-US/docs/Web/API/ReadableStream/ReadableStream#queuingstrategy) can be passed as the second argument, to control how far ahead of the reader the sources are read. By default, one chunk is read ahead. With `{ highWaterMark: 0 }`, the sources are only read when the output is read. Because the sources are read in parallel, the output can buffer one more chunk per source than the high water mark allows.

```ts
const allEvents = new UnionStream([userEvents, systemEvents], {
  highWaterMark: 0,
});
```

## Utilities

### consumers

`textFromStream`, `jsonFromStream` and `bytesFromStream` functions that read an entire stream and return its contents, matching the `text`, `json` and `bytes` functions in [`node:stream/consumers`](https://nodejs.org/api/webstreams.html#utility-consumers). They accept a `ReadableStream` or any async iterable, with chunks that are strings, `ArrayBuffer`s or typed arrays.

- `textFromStream(stream)` decodes the contents as UTF-8 text.
- `jsonFromStream(stream)` decodes the contents as UTF-8 text and parses it with `JSON.parse`.
- `bytesFromStream(stream)` joins the contents into a single `Uint8Array`. String chunks are UTF-8 encoded.

Each function rejects if the stream errors. If a chunk has an invalid type, the function rejects and cancels the stream.

```ts
import {
  bytesFromStream,
  jsonFromStream,
  textFromStream,
} from "web-utility-streams/consumers";

const config = await jsonFromStream(configStream);
const html = await textFromStream(
  renderStream.pipeThrough(new MapStream((chunk) => chunk.html)),
);
const image = await bytesFromStream(imageStream);
```

### readableStreamToGenerator

Convert a `ReadableStream` into an `AsyncGenerator`. Unlike the built-in async iterator on `ReadableStream`, calling `return()` or `throw()` on the generator (e.g. by breaking out of a `for await` loop) immediately cancels the stream, even while waiting for the next chunk. This is important for things like GraphQL subscriptions, to ensure resources are cleaned up promptly when a client disconnects. The generator also supports `await using`.

```ts
import readableStreamToGenerator from "web-utility-streams/readableStreamToGenerator";

const subscriptionResolver = {
  subscribe: () => readableStreamToGenerator(getEventStream()),
};

for await (const event of readableStreamToGenerator(stream)) {
  if (event.type === "done") break; // cancels the stream
}
```
