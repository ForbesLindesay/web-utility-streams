# web-utility-streams

Various utility streams that are very useful for working with web streams.

## Contents

**Transform streams**

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
- [`TransformPipelineStream`](#transformpipelinestream) - Combine several transform streams into one.

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

The whole document is validated, but only the elements being emitted are kept in memory, unless you pass `onExtraData` (see below). If the document isn't valid JSON, the stream errors when the problem is found. For speed, each element (and each object or array being discarded) is parsed in one go once its end is reached, so a problem inside one is found at its end, or after 1MB of it for larger values. If there's no array or object at `path`, the stream errors once the whole document has been parsed. Because elements are emitted as soon as they're parsed, some may be emitted before an error later in the document is found. If a streamed object repeats a key, a tuple is emitted for each occurrence.

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
