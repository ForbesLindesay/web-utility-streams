import { test, expect } from "vitest";
import EmptyStream from "./EmptyStream.js";
import { readableStreamResults } from "./StreamTestUtils.js";

test("EmptyStream - closes immediately without emitting any chunks", async () => {
  expect(await readableStreamResults(new EmptyStream())).toEqual([]);
});

test("EmptyStream - can be piped to a destination", async () => {
  const written: unknown[] = [];
  await new EmptyStream().pipeTo(
    new WritableStream({ write: (chunk) => void written.push(chunk) }),
  );
  expect(written).toEqual([]);
});
