import { describe, expect, test } from "vitest";
import { writeToString } from "fast-csv";
import CsvEncoderStream, {
  CsvEncoder,
  type CsvEncoderOptions,
} from "./CsvEncoderStream.js";
import {
  readableStream,
  readableStreamResults,
  testTransformRobustness,
} from "./internal/StreamTestUtils.js";

async function encodeStream(
  rows: string[][],
  options?: CsvEncoderOptions,
): Promise<string> {
  const results = await readableStreamResults(
    readableStream(rows).pipeThrough(new CsvEncoderStream(options)),
  );
  return results.map((r) => (r.ok ? r.value : "<error>")).join("");
}

function encode(rows: string[][], options?: CsvEncoderOptions): string {
  const encoder = new CsvEncoder(options);
  return rows.map((row) => encoder.formatRow(row)).join("");
}

describe("CsvEncoderStream", () => {
  test("encodes rows", async () => {
    expect(
      await encodeStream([
        ["a", "b"],
        ["1", "2"],
      ]),
    ).toBe("a,b\n1,2");
  });

  test("quotes fields that need quoting", () => {
    expect(encode([["a,b", 'c"d', "e\nf", "g\rh", "", " i "]])).toBe(
      '"a,b","c""d","e\nf","g\rh",, i ',
    );
  });

  test("includeEndRowDelimiter", () => {
    expect(encode([["a"], ["b"]], { includeEndRowDelimiter: true })).toBe(
      "a\nb\n",
    );
  });

  test("rowDelimiter", () => {
    expect(encode([["a"], ["b"]], { rowDelimiter: "\r\n" })).toBe("a\r\nb");
  });

  test("delimiter", () => {
    expect(encode([["a;b", "c,d"]], { delimiter: ";" })).toBe('"a;b";c,d');
    // Characters that are special in regular expressions
    expect(encode([["a]b", "c\\d", "e"]], { delimiter: "]" })).toBe(
      '"a]b"]c\\d]e',
    );
  });

  test("quote", () => {
    expect(encode([["a'b", 'c"d']], { quote: "'" })).toBe("'a''b'\x2cc\"d");
  });

  test("escape", () => {
    expect(encode([['a"b', "c\\d", 'e\\"']], { escape: "\\" })).toBe(
      '"a\\"b",c\\d,"e\\\\\\""',
    );
    expect(encode([["a\\b"]], { escape: "\\", quoteMode: "ALWAYS" })).toBe(
      '"a\\\\b"',
    );
    // "$" is special in replacement strings
    expect(encode([['a"b']], { escape: "$" })).toBe('"a$"b"');
  });

  test("quoteMode", () => {
    const rows = [
      ["a", "", 'b"c'],
      ["d", "e,f"],
    ];
    expect(encode(rows, { quoteMode: "ALWAYS" })).toBe(
      '"a","","b""c"\n"d","e,f"',
    );
    // Only fields that need quotes are quoted by default
    expect(encode(rows, { quoteMode: "AUTO" })).toBe('a,,"b""c"\nd,"e,f"');
    expect(encode(rows)).toBe('a,,"b""c"\nd,"e,f"');
    expect(encode(rows, { quoteMode: "NEVER" })).toBe('a,,b"c\nd,e,f');
    expect(encode([["a,b", 'c"d', "e\nf"]], { quoteMode: "NEVER" })).toBe(
      'a,b,c"d,e\nf',
    );
  });

  test("formatRow", () => {
    const encoder = new CsvEncoder({ writeBOM: true });
    expect(encoder.formatRow(["a", "b"])).toBe("\ufeffa,b");
    expect(encoder.formatRow(["c", "d"])).toBe("\nc,d");
    const withEnd = new CsvEncoder({ includeEndRowDelimiter: true });
    expect(withEnd.formatRow(["a"])).toBe("a\n");
    expect(withEnd.formatRow(["b"])).toBe("b\n");
  });

  test("writeBOM", () => {
    expect(encode([["a"], ["b"]], { writeBOM: true })).toBe("﻿a\nb");
  });

  test("removes null characters", () => {
    expect(encode([["a\0b", "\0", "c\0,d"]])).toBe('ab,,"c,d"');
  });

  test("quotes a row with a single blank field, so it isn't a blank line", () => {
    expect(encode([["a"], [""]])).toBe('a\n""');
    expect(encode([[""]])).toBe('""');
    expect(encode([["\0"]])).toBe('""');
    expect(encode([[" \t "], ["b"]])).toBe('" \t "\nb');
    // Unlike a row with one empty field, an empty row is a blank line
    expect(encode([[], ["a"]])).toBe("\na");
    expect(encode([[""], [], ["a"]])).toBe('""\n\na');
    expect(encode([[], ["a"]], { includeEndRowDelimiter: true })).toBe("\na\n");
    expect(encode([["a"], []], { includeEndRowDelimiter: true })).toBe("a\n\n");
    expect(encode([[]], { writeBOM: true })).toBe("\ufeff");
    // Rows with more than one field don't need this
    expect(
      encode([
        ["", ""],
        [" ", " "],
      ]),
    ).toBe(",\n , ");
    // It can't be done if quoting is turned off
    expect(encode([["a"], [""]], { quoteMode: "NEVER" })).toBe("a\n");
  });

  test("validates options", () => {
    const invalidOptions: CsvEncoderOptions[] = [
      { delimiter: "" },
      { delimiter: "ab" },
      { delimiter: "\n" },
      { quote: "" },
      { quote: "''" },
      { quote: "\r" },
      { escape: "" },
      { escape: "\\\\" },
      { delimiter: '"' },
      { delimiter: "'", quote: "'" },
      { quoteMode: "always" as "ALWAYS" },
    ];
    for (const options of invalidOptions) {
      expect(() => new CsvEncoder(options)).toThrow(TypeError);
      expect(() => new CsvEncoderStream(options)).toThrow(TypeError);
    }
    // The delimiter can be the quote character if fields are never quoted
    expect(
      () => new CsvEncoder({ quoteMode: "NEVER", delimiter: '"' }),
    ).not.toThrow();
  });
});

describe("CsvEncoder - compared with fast-csv", () => {
  const alphabet = ["a", " ", ",", ";", "\t", '"', "'", "\r", "\n", "é"];
  let seed = 3;
  const rand = () => {
    seed = (seed * 1103515245 + 12345) % 2147483648;
    return seed / 2147483648;
  };
  const rows: string[][] = [];
  for (let r = 0; r < 2000; r++) {
    const row: string[] = [];
    const fieldCount = 1 + Math.floor(rand() * 5);
    for (let f = 0; f < fieldCount; f++) {
      let field = "";
      const length = Math.floor(rand() * 6);
      for (let i = 0; i < length; i++) {
        field += alphabet[Math.floor(rand() * alphabet.length)];
      }
      row.push(field);
    }
    rows.push(row);
  }

  test.each<[CsvEncoderOptions, Parameters<typeof writeToString>[1]]>([
    [{}, {}],
    [{ quoteMode: "ALWAYS" }, { quoteColumns: true }],
    [
      { delimiter: "\t", quote: "'", rowDelimiter: "\r\n" },
      { delimiter: "\t", quote: "'", rowDelimiter: "\r\n" },
    ],
    [
      { includeEndRowDelimiter: true, writeBOM: true },
      { includeEndRowDelimiter: true, writeBOM: true },
    ],
  ])("gives the same output as fast-csv with %j", async (ours, theirs) => {
    // Unlike fast-csv, we quote a row with a single blank field, so it isn't
    // written as a blank line
    const comparableRows = rows.filter(
      (row) => !(row.length === 1 && /^[ \t]*$/.test(row[0]!)),
    );
    expect(comparableRows.length).toBeLessThan(rows.length);
    expect(encode(comparableRows, ours)).toBe(
      await writeToString(comparableRows, theirs),
    );
  });
});

testTransformRobustness({
  name: "CsvEncoderStream",
  create: () => new CsvEncoderStream(),
  validInput: [
    ["a", "b"],
    ["c", "d"],
  ],
});
