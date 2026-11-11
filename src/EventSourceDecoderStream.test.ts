import { test, expect } from "vitest";
import EventSourceDecoderStream from "./EventSourceDecoderStream.js";
import {
  delay,
  readableStream,
  readableStreamResults,
  testTransformRobustness,
} from "./internal/StreamTestUtils.js";

async function decode(text: string | string[]) {
  return await readableStreamResults(
    readableStream(Array.isArray(text) ? text : [text]).pipeThrough(
      new EventSourceDecoderStream(),
    ),
  );
}

test("EventSourceDecoderStream - decodes simple events", async () => {
  expect(await decode("data: hello\n\ndata: world\n\n")).toEqual([
    {
      ok: true,
      value: {
        type: "message",
        data: "hello",
        id: null,
        retryMilliseconds: null,
      },
    },
    {
      ok: true,
      value: {
        type: "message",
        data: "world",
        id: null,
        retryMilliseconds: null,
      },
    },
  ]);
});

test("EventSourceDecoderStream - joins multiple data lines and supports all fields", async () => {
  expect(
    await decode(
      "event: update\r\nid: 42\r\nretry: 1000\r\ndata: line 1\r\ndata:line 2\r\n\r\n",
    ),
  ).toEqual([
    {
      ok: true,
      value: {
        type: "update",
        data: "line 1\nline 2",
        id: "42",
        retryMilliseconds: 1000,
      },
    },
  ]);
});

test("EventSourceDecoderStream - ignores comments and unknown fields", async () => {
  expect(await decode(": this is a comment\n:\nfoo: bar\ndata: x\n\n")).toEqual(
    [
      {
        ok: true,
        value: {
          type: "message",
          data: "x",
          id: null,
          retryMilliseconds: null,
        },
      },
    ],
  );
});

test("EventSourceDecoderStream - only strips a single leading space from values", async () => {
  expect(await decode("data:  two spaces\ndata\n\n")).toEqual([
    {
      ok: true,
      value: {
        type: "message",
        data: " two spaces\n",
        id: null,
        retryMilliseconds: null,
      },
    },
  ]);
});

test("EventSourceDecoderStream - only matches exact field names, however the input is split", async () => {
  const text = [
    "dataX: ignored",
    "dat: ignored",
    "Data: ignored",
    " data: ignored",
    "data",
    "data:",
    "data:no space",
    "data:  two spaces",
    "data: a:b",
    "eventX: ignored",
    "event:custom",
    "idX: ignored",
    "id",
    "retryX: 1",
    "retry:42",
    ":data: comment",
    "",
    "",
  ].join("\n");
  const expected = {
    ok: true,
    value: {
      type: "custom",
      data: "\n\nno space\n two spaces\na:b",
      id: "",
      retryMilliseconds: 42,
    },
  };
  for (const size of [1, 2, 3, 5, text.length]) {
    const chunks: string[] = [];
    for (let i = 0; i < text.length; i += size) {
      chunks.push(text.slice(i, i + size));
    }
    // Enqueue all the chunks at once, rather than using `decode`, which waits
    // between chunks
    const input = new ReadableStream<string>({
      start(controller) {
        for (const chunk of chunks) controller.enqueue(chunk);
        controller.close();
      },
    });
    expect(
      await readableStreamResults(
        input.pipeThrough(new EventSourceDecoderStream()),
      ),
      `size ${size}`,
    ).toEqual([expected]);
  }
});

test("EventSourceDecoderStream - dispatches events with empty data", async () => {
  expect(await decode("data\n\ndata:\n\n")).toEqual([
    {
      ok: true,
      value: { type: "message", data: "", id: null, retryMilliseconds: null },
    },
    {
      ok: true,
      value: { type: "message", data: "", id: null, retryMilliseconds: null },
    },
  ]);
});

test("EventSourceDecoderStream - does not dispatch events without data", async () => {
  expect(await decode("event: foo\n\n\n\ndata: x\n\n")).toEqual([
    {
      ok: true,
      value: { type: "message", data: "x", id: null, retryMilliseconds: null },
    },
  ]);
});

test("EventSourceDecoderStream - an empty event type is treated as 'message'", async () => {
  expect(await decode("event:\ndata: x\n\n")).toEqual([
    {
      ok: true,
      value: { type: "message", data: "x", id: null, retryMilliseconds: null },
    },
  ]);
});

test("EventSourceDecoderStream - id and retryMilliseconds are attached to the next dispatched event", async () => {
  expect(await decode("id: 1\nretry: 50\n\ndata: a\n\ndata: b\n\n")).toEqual([
    {
      ok: true,
      value: { type: "message", data: "a", id: "1", retryMilliseconds: 50 },
    },
    {
      ok: true,
      value: { type: "message", data: "b", id: null, retryMilliseconds: null },
    },
  ]);
});

test("EventSourceDecoderStream - ignores invalid id and retry values", async () => {
  expect(
    await decode("id: a\u0000b\nretry: 10ms\nretry: -1\ndata: x\n\n"),
  ).toEqual([
    {
      ok: true,
      value: { type: "message", data: "x", id: null, retryMilliseconds: null },
    },
  ]);
});

test("EventSourceDecoderStream - discards an incomplete event at the end of the stream", async () => {
  expect(await decode("data: a\n\ndata: incomplete\n")).toEqual([
    {
      ok: true,
      value: { type: "message", data: "a", id: null, retryMilliseconds: null },
    },
  ]);
});

test("EventSourceDecoderStream - handles CRLF split across chunks", async () => {
  expect(await decode(["data: a\r", "\ndata: b\r", "\n\r", "\n"])).toEqual([
    {
      ok: true,
      value: {
        type: "message",
        data: "a\nb",
        id: null,
        retryMilliseconds: null,
      },
    },
  ]);
});

test("EventSourceDecoderStream - handles events split across chunks at any point", async () => {
  const text = "event: a\r\nid: 1\r\ndata: x\r\ndata: y\r\n\r\ndata: z\n\n";
  expect(await decode(text.split(""))).toEqual([
    {
      ok: true,
      value: { type: "a", data: "x\ny", id: "1", retryMilliseconds: null },
    },
    {
      ok: true,
      value: { type: "message", data: "z", id: null, retryMilliseconds: null },
    },
  ]);
});

test("EventSourceDecoderStream - supports \\r line endings", async () => {
  expect(await decode(["data: a\r", "\rdata: b\r\r"])).toEqual([
    {
      ok: true,
      value: { type: "message", data: "a", id: null, retryMilliseconds: null },
    },
    {
      ok: true,
      value: { type: "message", data: "b", id: null, retryMilliseconds: null },
    },
  ]);
});

testTransformRobustness({
  name: "EventSourceDecoderStream",
  create: () => new EventSourceDecoderStream(),
  validInput: ["data: a\n\nda", "ta: b\r", "\n\r\ndata: c\n\n"],
});

test("EventSourceDecoderStream - onId and onRetry are called for every valid id and retry field", async () => {
  const calls: unknown[] = [];
  const results = await readableStreamResults(
    readableStream([
      "id: 1\nretry: 100\n\n",
      "id: 2\nid: a\u0000b\nretry: 5s\ndata: x\n\n",
      "id:\nretry: 0\n",
    ]).pipeThrough(
      new EventSourceDecoderStream({
        onId: (id) => calls.push({ id }),
        onRetry: (retryMilliseconds) => calls.push({ retryMilliseconds }),
      }),
    ),
  );
  expect(calls).toEqual([
    { id: "1" },
    { retryMilliseconds: 100 },
    { id: "2" },
    // The id with a null character, and the non-numeric retry are ignored
    // The final fields are reported even though no event is dispatched
    { id: "" },
    { retryMilliseconds: 0 },
  ]);
  expect(results).toEqual([
    {
      ok: true,
      value: { type: "message", data: "x", id: "2", retryMilliseconds: 100 },
    },
  ]);
});

test("EventSourceDecoderStream - errors the stream if onId or onRetry throws", async () => {
  for (const options of [
    {
      onId: () => {
        throw new Error("onId failed");
      },
    },
    {
      onRetry: () => {
        throw new Error("onRetry failed");
      },
    },
  ]) {
    let cancelled = false;
    expect(
      await readableStreamResults(
        readableStream(["data: a\n\n", "id: 1\nretry: 1\n\n", "data: b\n\n"], {
          onCancel: () => (cancelled = true),
        }).pipeThrough(new EventSourceDecoderStream(options)),
      ),
    ).toEqual([
      {
        ok: true,
        value: {
          type: "message",
          data: "a",
          id: null,
          retryMilliseconds: null,
        },
      },
      { ok: false },
    ]);
    // Cancellation propagates back through the internal pipes asynchronously
    await delay(50);
    expect(cancelled).toBe(true);
  }
});
