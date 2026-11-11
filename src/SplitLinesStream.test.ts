import { test, expect } from "vitest";
import SplitLinesStream from "./SplitLinesStream.js";
import {
  readableStream,
  readableStreamResults,
  testTransformRobustness,
} from "./internal/StreamTestUtils.js";

async function split(chunks: string[]) {
  const results = await readableStreamResults(
    readableStream(chunks).pipeThrough(new SplitLinesStream()),
  );
  return results.map((r) => (r.ok ? r.value : "<error>"));
}

test("SplitLinesStream - splits on all line endings", async () => {
  expect(await split(["a\nb\r\nc\rd"])).toEqual(["a", "b", "c", "d"]);
});

test("SplitLinesStream - handles lines split across chunks", async () => {
  expect(await split(["he", "llo\nwor", "ld\n", "!"])).toEqual([
    "hello",
    "world",
    "!",
  ]);
});

test("SplitLinesStream - preserves blank lines", async () => {
  expect(await split(["a\n\nb\r\n\r\nc\r\rd\n"])).toEqual([
    "a",
    "",
    "b",
    "",
    "c",
    "",
    "d",
  ]);
});

test("SplitLinesStream - treats \\r\\n split across chunks as a single line break", async () => {
  expect(await split(["a\r", "\nb\r", "\n", "c"])).toEqual(["a", "b", "c"]);
  expect(await split(["a\r", "\n\nb"])).toEqual(["a", "", "b"]);
  expect(await split(["a\r", "", "\nb"])).toEqual(["a", "b"]);
});

test("SplitLinesStream - emits lines terminated by \\r without waiting for the next chunk", async () => {
  const stream = new SplitLinesStream();
  const writer = stream.writable.getWriter();
  const reader = stream.readable.getReader();
  void writer.write("a\r");
  expect(await reader.read()).toEqual({ done: false, value: "a" });
  void writer.write("\rb\n");
  expect(await reader.read()).toEqual({ done: false, value: "" });
  expect(await reader.read()).toEqual({ done: false, value: "b" });
  void writer.close();
  expect(await reader.read()).toEqual({ done: true, value: undefined });
});

test("SplitLinesStream - does not emit a trailing empty line", async () => {
  expect(await split(["a\n"])).toEqual(["a"]);
  expect(await split([""])).toEqual([]);
  expect(await split([])).toEqual([]);
});

test("SplitLinesStream - handles many small chunks", async () => {
  const text = "line 1\nline 2\r\nline 3\rline 4";
  expect(await split(text.split(""))).toEqual([
    "line 1",
    "line 2",
    "line 3",
    "line 4",
  ]);
});

testTransformRobustness({
  name: "SplitLinesStream",
  create: () => new SplitLinesStream(),
  validInput: ["a\nb", "\r", "\nc\n", "d"],
});
