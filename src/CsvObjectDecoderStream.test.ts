import { describe, expect, test } from "vitest";
import CsvDecoderStream from "./CsvDecoderStream.js";
import { CsvEncoder } from "./CsvEncoderStream.js";
import CsvObjectDecoderStream, {
  type CsvObjectDecoderStreamOptions,
  type CsvRow,
} from "./CsvObjectDecoderStream.js";
import TableDecoderStream from "./TableDecoderStream.js";
import { testTransformRobustness } from "./internal/StreamTestUtils.js";
import {
  collect,
  streamFrom,
  testTableDecoder,
} from "./internal/TableDecoderTestCases.js";

/**
 * Split `text` into chunks of `size` characters, so rows and fields are split
 * across chunks
 */
function chunks(text: string, size = 3): string[] {
  const result: string[] = [];
  for (let i = 0; i < text.length; i += size) {
    result.push(text.slice(i, i + size));
  }
  return result;
}

function decodeCsv<T>(
  text: string,
  getObject: (row: CsvRow) => T,
  options?: CsvObjectDecoderStreamOptions<T>,
) {
  return collect(
    streamFrom(chunks(text)).pipeThrough(
      new CsvObjectDecoderStream(getObject, options),
    ),
  );
}

/**
 * The same as `decodeCsv`, but by piping `CsvDecoderStream` into
 * `TableDecoderStream`
 */
function decodeCsvChained<T>(
  text: string,
  getObject: (row: CsvRow) => T,
  options?: CsvObjectDecoderStreamOptions<T>,
) {
  return collect(
    streamFrom(chunks(text))
      .pipeThrough(new CsvDecoderStream(options))
      .pipeThrough(new TableDecoderStream(getObject, options)),
  );
}

testTableDecoder("CsvObjectDecoderStream", (rows, getObject, options) => {
  const encoder = new CsvEncoder();
  const text = rows.map((row, i) => encoder.formatRow(row, i === 0)).join("");
  return decodeCsv(text, getObject, options);
});

describe("CsvObjectDecoderStream - CSV", () => {
  const toObject = (row: CsvRow) => row.toObject();

  test("decodes quoted headers and fields, split across chunks", async () => {
    expect(
      await decodeCsv('"a,1","b\nc"\r\n"x ""y""","z\nw"\r\n', toObject),
    ).toEqual({ values: [{ "a,1": 'x "y"', "b\nc": "z\nw" }] });
  });

  test("ignores a byte order mark before the headers", async () => {
    expect(await decodeCsv("﻿id,name\n1,Ada\n", toObject)).toEqual({
      values: [{ id: "1", name: "Ada" }],
    });
  });

  test("passes CSV options to the decoder", async () => {
    expect(
      await decodeCsv("a;b\n1;'2;3'\n\n4;5\n", toObject, {
        delimiter: ";",
        quote: "'",
        ignoreEmpty: true,
      }),
    ).toEqual({
      values: [
        { a: "1", b: "2;3" },
        { a: "4", b: "5" },
      ],
    });
  });

  test("errors the stream for invalid CSV", async () => {
    const result = await decodeCsv('a\n1\n"x" y\n2\n', toObject);
    expect(result.values).toEqual([{ a: "1" }]);
    expect(result.error).toBeInstanceOf(Error);
  });

  test.each([
    ["simple rows", "a,b\n1,2\n3,4\n"],
    ["ragged rows", "a,b,c\n1,2,3\n4\n\n5,6\n"],
    ["a long row", "a,b\n1,2\n3,4,5\n6,7\n"],
    ["a missing column", "a\n1\n"],
    ["duplicate headers", "a,a\n1,2\n"],
    ["no rows", "a,b\n"],
    ["no headers", ""],
  ])(
    "gives the same result as CsvDecoderStream piped into TableDecoderStream: %s",
    async (_, text) => {
      const getObject = (row: CsvRow) => [
        row.index,
        row.toObject(),
        row.getOptional("b"),
        row.get("a"),
      ];
      const combined = await decodeCsv(text, getObject);
      const chained = await decodeCsvChained(text, getObject);
      expect(combined.values).toEqual(chained.values);
      expect((combined.error as Error | undefined)?.message).toEqual(
        (chained.error as Error | undefined)?.message,
      );
    },
  );
});

testTransformRobustness({
  name: "CsvObjectDecoderStream",
  create: () => new CsvObjectDecoderStream((row) => row.toObject()),
  validInput: ["a,b\n", "1,", "2\n"],
  longInput: ["a,b\n", "1,2\n", "3,4\n", "5,6\n", "7,8\n"],
  invalidInput: "1,2,3\n",
});
