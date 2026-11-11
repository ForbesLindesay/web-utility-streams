import { test, expect } from "vitest";
import EventSourceEncoderStream, {
  type EventSourceEncoderInput,
} from "./EventSourceEncoderStream.js";
import EventSourceDecoderStream from "./EventSourceDecoderStream.js";
import {
  readableStream,
  readableStreamResults,
  testTransformRobustness,
} from "./internal/StreamTestUtils.js";

async function encode(events: EventSourceEncoderInput[]) {
  return await readableStreamResults(
    readableStream(events).pipeThrough(new EventSourceEncoderStream()),
  );
}

test("EventSourceEncoderStream - encodes simple events", async () => {
  expect(await encode([{ data: "hello" }, { data: "world" }])).toEqual([
    { ok: true, value: "data: hello\n\n" },
    { ok: true, value: "data: world\n\n" },
  ]);
});

test("EventSourceEncoderStream - encodes all fields", async () => {
  expect(
    await encode([
      {
        type: "update",
        id: "42",
        retryMilliseconds: 1000,
        data: "line 1\nline 2\r\nline 3",
      },
    ]),
  ).toEqual([
    {
      ok: true,
      value:
        "event: update\nid: 42\nretry: 1000\ndata: line 1\ndata: line 2\ndata: line 3\n\n",
    },
  ]);
});

test("EventSourceEncoderStream - omits the default event type and null fields", async () => {
  expect(
    await encode([
      { type: "message", data: "x", id: null, retryMilliseconds: null },
    ]),
  ).toEqual([{ ok: true, value: "data: x\n\n" }]);
});

test("EventSourceEncoderStream - encodes empty data and empty ids", async () => {
  expect(await encode([{ data: "", id: "" }])).toEqual([
    { ok: true, value: "id: \ndata: \n\n" },
  ]);
});

test("EventSourceEncoderStream - errors the stream for events that cannot be encoded", async () => {
  const invalidEvents: any[] = [
    { data: 42 },
    { data: "x", type: "a\nb" },
    { data: "x", id: "a\rb" },
    { data: "x", id: "a\u0000b" },
    { data: "x", retryMilliseconds: -1 },
    { data: "x", retryMilliseconds: 1.5 },
    { data: "x", retryMilliseconds: NaN },
  ];
  for (const invalid of invalidEvents) {
    expect(await encode([{ data: "ok" }, invalid])).toEqual([
      { ok: true, value: "data: ok\n\n" },
      { ok: false },
    ]);
  }
});

test("EventSourceEncoderStream - round trips with EventSourceDecoderStream", async () => {
  const events = [
    { type: "message", data: "hello", id: null, retryMilliseconds: null },
    {
      type: "custom",
      data: " leading space\n\nblank line",
      id: "1",
      retryMilliseconds: 5,
    },
    { type: "message", data: "", id: "", retryMilliseconds: 0 },
    {
      type: " spaced",
      data: ": not a comment",
      id: " 2",
      retryMilliseconds: null,
    },
  ];
  expect(
    await readableStreamResults(
      readableStream(events)
        .pipeThrough(new EventSourceEncoderStream())
        .pipeThrough(new EventSourceDecoderStream()),
    ),
  ).toEqual(events.map((value) => ({ ok: true, value })));
});

testTransformRobustness({
  name: "EventSourceEncoderStream",
  create: () => new EventSourceEncoderStream(),
  validInput: [{ data: "a" }, { data: "b" }, { data: "c" }],
  invalidInput: { data: "x", id: "a\nb" },
});

test("EventSourceEncoderStream - supports custom line endings", async () => {
  const event = {
    type: "update",
    id: "1",
    retryMilliseconds: 10,
    data: "a\nb\r\nc\rd",
  };
  for (const lineEnding of ["\n", "\r\n", "\r"] as const) {
    const results = await readableStreamResults(
      readableStream([event]).pipeThrough(
        new EventSourceEncoderStream({ lineEnding }),
      ),
    );
    expect(results).toEqual([
      {
        ok: true,
        value: [
          "event: update",
          "id: 1",
          "retry: 10",
          "data: a",
          "data: b",
          "data: c",
          "data: d",
          "",
          "",
        ].join(lineEnding),
      },
    ]);
    expect(
      await readableStreamResults(
        readableStream([event])
          .pipeThrough(new EventSourceEncoderStream({ lineEnding }))
          .pipeThrough(new EventSourceDecoderStream()),
      ),
    ).toEqual([{ ok: true, value: { ...event, data: "a\nb\nc\nd" } }]);
  }
});
