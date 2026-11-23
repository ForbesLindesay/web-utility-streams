import { expect, test } from "vitest";
import TableDecoderStream from "./TableDecoderStream.js";
import { testTransformRobustness } from "./internal/StreamTestUtils.js";
import {
  collect,
  streamFrom,
  testTableDecoder,
} from "./internal/TableDecoderTestCases.js";

testTableDecoder("TableDecoderStream", (rows, getObject, options) =>
  collect(
    streamFrom(rows).pipeThrough(new TableDecoderStream(getObject, options)),
  ),
);

test("TableDecoderStream does not modify the rows", async () => {
  const header = Object.freeze(["a", "b"]);
  const row = Object.freeze(["1"]);
  expect(
    await collect(
      streamFrom([header, row]).pipeThrough(
        new TableDecoderStream((r) => r.toObject()),
      ),
    ),
  ).toEqual({ values: [{ a: "1", b: "" }] });
});

testTransformRobustness({
  name: "TableDecoderStream",
  create: () => new TableDecoderStream((row) => row.toObject()),
  validInput: [
    ["a", "b"],
    ["1", "2"],
  ],
  longInput: [
    ["a", "b"],
    ["1", "2"],
    ["3", "4"],
    ["5", "6"],
    ["7", "8"],
  ],
  invalidInput: ["1", "2", "3"],
});
