import { describe, expect, test } from "vitest";
import { parseString, writeToString } from "fast-csv";
import CsvDecoderStream, {
  CsvDecoder,
  type CsvDecoderOptions,
} from "./CsvDecoderStream.js";
import { CsvEncoder, type CsvEncoderOptions } from "./CsvEncoderStream.js";
import {
  readableStream,
  readableStreamResults,
  testTransformRobustness,
} from "./internal/StreamTestUtils.js";

/**
 * Decode `chunks` with `CsvDecoderStream`
 */
async function decodeStream(
  chunks: string[],
  options?: CsvDecoderOptions,
): Promise<(string[] | "<error>")[]> {
  const results = await readableStreamResults(
    readableStream(chunks).pipeThrough(new CsvDecoderStream(options)),
  );
  return results.map((r) => (r.ok ? r.value : "<error>"));
}

const noop = () => {};

/**
 * The text the decoder is keeping until the next chunk. This is private, but
 * we check it to make sure the decoder doesn't keep (and re-parse) more text
 * than it needs to.
 */
function bufferedText(decoder: CsvDecoder): string {
  return (decoder as unknown as { _buffer: string })._buffer;
}

/**
 * Decode `chunks` with `CsvDecoder` directly, which is much quicker than
 * using a stream, so is used for tests that decode a lot of inputs.
 */
function decode(
  chunks: string | string[],
  options?: CsvDecoderOptions,
): string[][] {
  const rows: string[][] = [];
  const decoder = new CsvDecoder((row) => rows.push(row), options);
  for (const chunk of typeof chunks === "string" ? [chunks] : chunks) {
    decoder.parse(chunk, false);
  }
  decoder.parse("", true);
  expect(bufferedText(decoder)).toBe("");
  return rows;
}

/**
 * Decode, returning the error message instead of throwing
 */
function decodeResult(
  chunks: string | string[],
  options?: CsvDecoderOptions,
): string[][] | string {
  try {
    return decode(chunks, options);
  } catch (ex: any) {
    return ex.message;
  }
}

/**
 * Every way of splitting `text` into two or three chunks
 */
function* splits(text: string): Generator<string[]> {
  for (let i = 0; i <= text.length; i++) {
    for (let j = i; j <= text.length; j++) {
      yield [text.slice(0, i), text.slice(i, j), text.slice(j)];
    }
  }
}

// A deterministic random number generator, so failures can be reproduced
function random(seed: number): () => number {
  return () => {
    seed = (seed * 1103515245 + 12345) % 2147483648;
    return seed / 2147483648;
  };
}

describe("CsvDecoderStream", () => {
  test("decodes simple rows", async () => {
    expect(await decodeStream(["a,b,c\n1,2,3\n"])).toEqual([
      ["a", "b", "c"],
      ["1", "2", "3"],
    ]);
  });

  test("handles rows and fields split across chunks", async () => {
    expect(await decodeStream(["a,", "b", "b,c\n1", ",2,3", "\n"])).toEqual([
      ["a", "bb", "c"],
      ["1", "2", "3"],
    ]);
  });

  test("emits the final row without a trailing line break", async () => {
    expect(await decodeStream(["a,b\nc,d"])).toEqual([
      ["a", "b"],
      ["c", "d"],
    ]);
  });

  test("emits nothing for empty input", async () => {
    expect(await decodeStream([])).toEqual([]);
    expect(await decodeStream([""])).toEqual([]);
    expect(await decodeStream(["", ""])).toEqual([]);
  });

  test("errors the stream for invalid CSV", async () => {
    expect(await decodeStream(['a,b\n"c" d,e\nf,g\n'])).toEqual([
      ["a", "b"],
      "<error>",
    ]);
    expect(await decodeStream(['a,b\n"c'])).toEqual([["a", "b"], "<error>"]);
  });

  test("does not wait for the next chunk to emit a complete row", async () => {
    const stream = new CsvDecoderStream();
    const writer = stream.writable.getWriter();
    const reader = stream.readable.getReader();
    void writer.write("a,b\nc");
    expect(await reader.read()).toEqual({ done: false, value: ["a", "b"] });
    void writer.write(',"d"\r');
    expect(await reader.read()).toEqual({ done: false, value: ["c", "d"] });
    void writer.write("\ne\n");
    expect(await reader.read()).toEqual({ done: false, value: ["e"] });
    void writer.close();
    expect(await reader.read()).toEqual({ done: true, value: undefined });
  });
});

describe("CsvDecoder - fields", () => {
  test("keeps empty fields", () => {
    expect(decode("a,,b\n,\n,a\na,\n")).toEqual([
      ["a", "", "b"],
      ["", ""],
      ["", "a"],
      ["a", ""],
    ]);
    expect(decode("a,")).toEqual([["a", ""]]);
    expect(decode(",")).toEqual([["", ""]]);
  });

  test("keeps spaces in unquoted fields", () => {
    expect(decode(" a , b \n")).toEqual([[" a ", " b "]]);
  });

  test("decodes quoted fields", () => {
    expect(decode('"a","b,c","d\ne","f\r\ng",""\n')).toEqual([
      ["a", "b,c", "d\ne", "f\r\ng", ""],
    ]);
  });

  test("decodes escaped quotes", () => {
    expect(decode('"a""b","""","""""","""c"""\n')).toEqual([
      ['a"b', '"', '""', '"c"'],
    ]);
  });

  test("ignores spaces around quoted fields", () => {
    expect(decode('  "a"  , \t"b"\t\n "c" ', { delimiter: "," })).toEqual([
      ["a", "b"],
      ["c"],
    ]);
  });

  test("treats quotes in the middle of unquoted fields as normal characters", () => {
    expect(decode('a"b,c""\n')).toEqual([['a"b', 'c""']]);
  });

  test("strips a byte order mark at the start of the input", () => {
    expect(decode("﻿a,b\n")).toEqual([["a", "b"]]);
    expect(decode(["", "﻿a,b"])).toEqual([["a", "b"]]);
    // Only at the start
    expect(decode(["a\n", "﻿b"])).toEqual([["a"], ["﻿b"]]);
  });

  test("handles non-ASCII text", () => {
    expect(decode('東京,"Zürich ✨","😀,😀"\n')).toEqual([
      ["東京", "Zürich ✨", "😀,😀"],
    ]);
  });
});

describe("CsvDecoder - rows", () => {
  test("accepts any line ending by default", () => {
    expect(decode("a\nb\r\nc\rd")).toEqual([["a"], ["b"], ["c"], ["d"]]);
    expect(decode('"a"\n"b"\r\n"c"\r"d"')).toEqual([
      ["a"],
      ["b"],
      ["c"],
      ["d"],
    ]);
  });

  test("treats \\r\\n split across chunks as a single line break", () => {
    expect(decode(["a\r", "\nb"])).toEqual([["a"], ["b"]]);
    expect(decode(["a\r", "", "\nb"])).toEqual([["a"], ["b"]]);
    expect(decode(['"a"\r', "\nb"])).toEqual([["a"], ["b"]]);
    expect(decode(["a\r", "\n\nb"])).toEqual([["a"], [""], ["b"]]);
  });

  test("emits blank lines as a row with one empty field", () => {
    expect(decode("a\n\nb\r\n\r\n \n")).toEqual([
      ["a"],
      [""],
      ["b"],
      [""],
      [" "],
    ]);
  });

  test("does not emit a row for a final line break", () => {
    expect(decode("a\n")).toEqual([["a"]]);
    expect(decode("a\r\n")).toEqual([["a"]]);
    expect(decode("a\n\n")).toEqual([["a"], [""]]);
  });

  test("ignoreEmpty skips blank lines", () => {
    const options = { ignoreEmpty: true };
    expect(decode("\na\n\n \t\nb\r\n\r\n", options)).toEqual([["a"], ["b"]]);
    // A quoted empty field or empty fields separated by delimiters are not
    // blank lines
    expect(decode('""\n,\n', options)).toEqual([[""], ["", ""]]);
  });
});

describe("CsvDecoder - errors", () => {
  test("errors for text after a closing quote", () => {
    expect(decodeResult('a\n"b"c\n')).toBe(
      'CsvDecoder: Invalid CSV on row 2: expected "," or a line break after the closing quote, but got "c"',
    );
    expect(decodeResult('"b" "c"')).toMatch(/after the closing quote/);
  });

  test("errors for a missing closing quote", () => {
    expect(decodeResult('a\nb\n"c\nd')).toBe(
      "CsvDecoder: Invalid CSV on row 3: missing closing quote",
    );
    expect(decodeResult('"a""')).toMatch(/missing closing quote/);
    expect(decodeResult('"')).toMatch(/missing closing quote/);
  });

  test("validates options", () => {
    const invalidOptions: CsvDecoderOptions[] = [
      { delimiter: "" },
      { delimiter: "ab" },
      { delimiter: "\n" },
      { quote: "''" },
      { escape: "" },
      { delimiter: '"' },
      { quote: "," },
      { escape: ";" },
    ];
    for (const options of invalidOptions) {
      expect(() => new CsvDecoder(noop, options)).toThrow(TypeError);
      expect(() => new CsvDecoderStream(options)).toThrow(TypeError);
    }
  });
});

describe("CsvDecoder - guessing the delimiter", () => {
  test('uses the most common of ",", ";" and "\\t" in the first line', () => {
    expect(decode("a,b,c\n1,2,3\n")).toEqual([
      ["a", "b", "c"],
      ["1", "2", "3"],
    ]);
    expect(decode("a;b;c\n1;2,5;3\n")).toEqual([
      ["a", "b", "c"],
      ["1", "2,5", "3"],
    ]);
    expect(decode("a\tb,c\td\n1\t2,5\t3\n")).toEqual([
      ["a", "b,c", "d"],
      ["1", "2,5", "3"],
    ]);
  });

  test('prefers ",", then ";", if there\'s a tie or no delimiter', () => {
    expect(decode("a,b;c\td\n")).toEqual([["a", "b;c\td"]]);
    expect(decode("a;b\tc\n")).toEqual([["a", "b\tc"]]);
    expect(decode("abc\nd,e\n")).toEqual([["abc"], ["d", "e"]]);
  });

  test("only looks at the first line", () => {
    expect(decode("a;b\n1,2,3;4\n")).toEqual([
      ["a", "b"],
      ["1,2,3", "4"],
    ]);
    expect(decode("a;b\r1,2,3;4")).toEqual([
      ["a", "b"],
      ["1,2,3", "4"],
    ]);
  });

  test("ignores delimiters in quoted fields", () => {
    expect(decode('"a,b,c";d;e\n')).toEqual([["a,b,c", "d", "e"]]);
    expect(decode('"a"",b,c";d;e\n')).toEqual([['a",b,c', "d", "e"]]);
    expect(decode('"a\\",b,c";d;e\n', { escape: "\\" })).toEqual([
      ['a",b,c', "d", "e"],
    ]);
    // Including line breaks in quoted fields
    expect(decode('"a\n,,,";b;c\n')).toEqual([["a\n,,,", "b", "c"]]);
  });

  test("uses all of the input if there is only one line", () => {
    expect(decode(["a;b", ";c"])).toEqual([["a", "b", "c"]]);
    expect(decode("\ufeffa;b")).toEqual([["a", "b"]]);
  });

  test("requires a delimiter if the quote or escape could be guessed as one", () => {
    for (const options of [
      { quote: ";" },
      { quote: "," },
      { quote: "\t" },
      { escape: "," },
    ]) {
      expect(() => new CsvDecoder(noop, options)).toThrow(TypeError);
    }
    expect(decode(";a,b;,;c;\n", { quote: ";", delimiter: "," })).toEqual([
      ["a,b", "c"],
    ]);
  });

  test("treats quotes in the middle of unquoted fields as normal characters", () => {
    expect(decode('a"b;c;d\ne;f"g\n')).toEqual([
      ['a"b', "c", "d"],
      ["e", 'f"g'],
    ]);
    // But quotes after spaces at the start of a field start a quoted field
    expect(decode('a; ",";b\n')).toEqual([["a", ",", "b"]]);
    expect(decode('a\t ";;"\tb\n')).toEqual([["a", ";;", "b"]]);
    expect(decode('﻿",";b;c\n')).toEqual([[",", "b", "c"]]);
    // And doubled quotes stay in the quoted field
    expect(decode('""","",";b;c\n')).toEqual([['",",', "b", "c"]]);
  });

  test("emits rows at the end of a first line with a quote in an unquoted field", () => {
    const rows: string[][] = [];
    const decoder = new CsvDecoder((row) => rows.push(row));
    decoder.parse('5" pipe;10\n', false);
    expect(rows).toEqual([['5" pipe', "10"]]);
  });

  test("waits for the end of the first line before emitting rows", () => {
    const rows: string[][] = [];
    const decoder = new CsvDecoder((row) => rows.push(row));
    decoder.parse("a;", false);
    decoder.parse("b", false);
    expect(rows).toEqual([]);
    decoder.parse(";c\n1;2;3\n4", false);
    expect(rows).toEqual([
      ["a", "b", "c"],
      ["1", "2", "3"],
    ]);
    decoder.parse("", true);
    expect(rows).toEqual([["a", "b", "c"], ["1", "2", "3"], ["4"]]);
  });

  test("works with CsvDecoderStream", async () => {
    expect(await decodeStream(["a\tb", "\tc\n1\t", "2\t3\n"])).toEqual([
      ["a", "b", "c"],
      ["1", "2", "3"],
    ]);
  });

  test("is not used if a delimiter is passed", () => {
    expect(decode("a;b;c,d\n", { delimiter: "," })).toEqual([["a;b;c", "d"]]);
  });
});

describe("CsvDecoder - maxCharsToGuessDelimiter", () => {
  // The first line has more ";" in its first 11 characters, but more "," in
  // the whole line
  const chunks = ["a;b;c;d;", "e;f", ",g,h,i,j,k,l,m\n1;2"];

  test("guesses from the whole first line if it is short enough", () => {
    expect(decode(chunks)).toEqual([
      ["a;b;c;d;e;f", "g", "h", "i", "j", "k", "l", "m"],
      ["1;2"],
    ]);
  });

  test("guesses once it has read that many characters", () => {
    expect(decode(chunks, { maxCharsToGuessDelimiter: 10 })).toEqual([
      ["a", "b", "c", "d", "e", "f,g,h,i,j,k,l,m"],
      ["1", "2"],
    ]);
  });

  test("uses every character in the chunk that reaches the limit", () => {
    // The limit is reached part way through the 2nd chunk, but all of it
    // is used, so there are 3 "," and 2 ";"
    expect(
      decode(["a;b", ";c,d,e,f", ",g\n"], { maxCharsToGuessDelimiter: 4 }),
    ).toEqual([["a;b;c", "d", "e", "f", "g"]]);
  });

  test("emits rows once the delimiter has been guessed", () => {
    const rows: string[][] = [];
    const decoder = new CsvDecoder((row) => rows.push(row), {
      maxCharsToGuessDelimiter: 6,
    });
    decoder.parse("a;b;", false);
    expect(rows).toEqual([]);
    decoder.parse("c,d", false);
    // The delimiter has been guessed, but the first row isn't complete yet
    expect(rows).toEqual([]);
    decoder.parse("\n1;2\n", false);
    expect(rows).toEqual([
      ["a", "b", "c,d"],
      ["1", "2"],
    ]);
  });

  test("defaults to 10,000 characters", () => {
    const long = "c".repeat(9_996);
    // 10,000 characters, so the delimiter is guessed before the "," are seen
    expect(decode([`a;b${long}c`, ",d,e\n"])).toEqual([["a", `b${long}c,d,e`]]);
    // 9,999 characters, so the whole line is used
    expect(decode([`a;b${long}`, ",d,e\n"])).toEqual([
      [`a;b${long}`, "d", "e"],
    ]);
  });

  test("0 guesses from the first chunk, and Infinity from the whole first line", () => {
    expect(decode(chunks, { maxCharsToGuessDelimiter: 0 })).toEqual([
      ["a", "b", "c", "d", "e", "f,g,h,i,j,k,l,m"],
      ["1", "2"],
    ]);
    const line = `a;b;${"c".repeat(20_000)},d,e,f`;
    const lineChunks = line.match(/[^]{1,1000}/g)!;
    expect(
      decode([...lineChunks, "\n"], { maxCharsToGuessDelimiter: Infinity }),
    ).toEqual([line.split(",")]);
  });

  test("is ignored if a delimiter is passed", () => {
    expect(
      decode(chunks, { delimiter: ",", maxCharsToGuessDelimiter: 1 }),
    ).toEqual([["a;b;c;d;e;f", "g", "h", "i", "j", "k", "l", "m"], ["1;2"]]);
  });
});

describe("CsvDecoder - options", () => {
  test("delimiter", () => {
    expect(decode('a;"b;c";d\n', { delimiter: ";" })).toEqual([
      ["a", "b;c", "d"],
    ]);
    expect(decode('a\t"b\tc"\t d \n', { delimiter: "\t" })).toEqual([
      ["a", "b\tc", " d "],
    ]);
    // Tabs are not skipped around quoted fields when they're the delimiter
    expect(decode('\t"a"\t\n', { delimiter: "\t" })).toEqual([["", "a", ""]]);
  });

  test("quote", () => {
    expect(decode("'a,b','c''d'\n", { quote: "'" })).toEqual([["a,b", "c'd"]]);
    expect(decode('"a,b"\n', { quote: false })).toEqual([['"a', 'b"']]);
    expect(decode('"a"\n', { quote: true })).toEqual([["a"]]);
  });

  test("escape", () => {
    const options = { escape: "\\" };
    expect(decode('"a\\"b","c\\\\d","e\\f",g\\h\n', options)).toEqual([
      ['a"b', "c\\d", "e\\f", "g\\h"],
    ]);
    expect(decodeResult('"a""b"', options)).toMatch(/after the closing quote/);
    // The escape character is only treated specially in quoted fields
    expect(decode('a\\",b\n', options)).toEqual([['a\\"', "b"]]);
  });
});

describe("CsvDecoder - chunks", () => {
  const inputs: [string, CsvDecoderOptions?][] = [
    ['a,"b""c",\r\n"d\re"  ,\n\n f \r'],
    ['﻿ "a"\t,b\r\n\r"""",'],
    ['"a\\"b\\\\","c\\d",\\\n"\\"', { escape: "\\" }],
    ["a\r\nb\rc\r\r\n\nd\r"],
    ['a,"\r\n",\rb\r\n'],
    ["a\n\n \nb\r\n\t\r\n", { ignoreEmpty: true }],
    ['"a"" ,b', { quote: false }],
    ['"a" x'],
    ['a;"b,c,d";\te\r\n1;2;3'],
    ['"a\\",b,c";d\n', { escape: "\\" }],
    ['"a'],
  ];
  test.each(inputs)(
    "gives the same result however %j is split",
    (text, options) => {
      const expected = decodeResult(text, options);
      for (const chunks of splits(text)) {
        expect(decodeResult(chunks, options), JSON.stringify(chunks)).toEqual(
          expected,
        );
      }
      expect(decodeResult(text.split(""), options)).toEqual(expected);
    },
  );

  test("only keeps a few characters between chunks", () => {
    const rows: string[][] = [];
    const decoder = new CsvDecoder((row) => rows.push(row), {
      delimiter: ",",
    });
    decoder.parse('a,"b', false);
    expect(bufferedText(decoder)).toBe("");
    // The quote could be the start of an escaped quote
    decoder.parse('c""d"', false);
    expect(bufferedText(decoder)).toBe('"');
    decoder.parse(",e\n", false);
    expect(bufferedText(decoder)).toBe("");
    expect(rows).toEqual([["a", 'bc"d', "e"]]);
    decoder.parse(' "', false);
    expect(bufferedText(decoder)).toBe("");
    decoder.parse('f"', false);
    expect(bufferedText(decoder)).toBe('"');
    // The spaces could be followed by a quote
    decoder.parse(" ,  ", false);
    expect(bufferedText(decoder)).toBe("  ");
    decoder.parse("g", true);
    expect(bufferedText(decoder)).toBe("");
    expect(rows).toEqual([
      ["a", 'bc"d', "e"],
      ["f", "  g"],
    ]);
  });

  test("handles very large fields in linear time", () => {
    const field = "x".repeat(10_000_000);
    const quoted = `"${field.replaceAll("x", 'x""')}"`;
    const chunks: string[] = [];
    const text = `${field},${quoted}\n`;
    for (let i = 0; i < text.length; i += 1024) {
      chunks.push(text.slice(i, i + 1024));
    }
    const start = performance.now();
    const rows = decode(chunks);
    expect(performance.now() - start).toBeLessThan(5000);
    expect(rows.length).toBe(1);
    expect(rows[0]![0]).toBe(field);
    expect(rows[0]![1]).toBe(field.replaceAll("x", 'x"'));
  });
});

describe("CsvDecoder - round trips with CsvEncoder", () => {
  test("an empty row is a blank line, but a row with one empty field is kept", () => {
    const encoder = new CsvEncoder({ includeEndRowDelimiter: true });
    const text = [["a"], [], [""], ["b"]]
      .map((row, i) => encoder.formatRow(row, i === 0))
      .join("");
    expect(text).toBe('a\n\n""\nb\n');
    expect(decode(text, { delimiter: "," })).toEqual([
      ["a"],
      [""],
      [""],
      ["b"],
    ]);
    expect(decode(text, { delimiter: ",", ignoreEmpty: true })).toEqual([
      ["a"],
      [""],
      ["b"],
    ]);
  });

  // The decoder accepts any line break, so it's only given the options that
  // it shares with the encoder
  const encoderOptions: CsvEncoderOptions[] = [
    {},
    { quoteMode: "ALWAYS" },
    { delimiter: ";", quote: "'" },
    { delimiter: "\t", rowDelimiter: "\r\n", includeEndRowDelimiter: true },
    { escape: "\\" },
    { escape: "\\", quoteMode: "ALWAYS", writeBOM: true },
    { rowDelimiter: "\r" },
  ];
  const alphabet = [
    "a",
    "b",
    " ",
    ",",
    ";",
    "\t",
    '"',
    "'",
    "\\",
    "\r",
    "\n",
    "é",
    "😀",
  ];
  test.each(encoderOptions)("with %j", (options) => {
    const rand = random(42);
    for (let n = 0; n < 500; n++) {
      const rows: string[][] = [];
      const rowCount = 1 + Math.floor(rand() * 5);
      for (let r = 0; r < rowCount; r++) {
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
      const encoder = new CsvEncoder(options);
      const text = rows
        .map((row, i) => encoder.formatRow(row, i === 0))
        .join("");
      const split = Math.floor(rand() * text.length);
      const { delimiter = ",", quote, escape } = options;
      // Rows with a single blank field are quoted, so they're kept even if
      // blank lines are ignored
      for (const ignoreEmpty of [false, true]) {
        expect(
          decode([text.slice(0, split), text.slice(split)], {
            delimiter,
            quote,
            escape,
            ignoreEmpty,
          }),
          JSON.stringify(text),
        ).toEqual(rows);
      }
    }
  });
});

interface NodeStream {
  on(event: string, listener: (value: any) => void): NodeStream;
}

describe("CsvDecoder - compared with fast-csv", () => {
  function fastCsv(
    text: string,
    options: { delimiter?: string; quote?: string; escape?: string },
  ): Promise<string[][] | "<error>"> {
    return new Promise((resolve) => {
      const rows: string[][] = [];
      // fast-csv returns a Node.js stream, but this package doesn't use the
      // Node.js types, so we describe just the part we use
      const stream = parseString(text, options) as unknown as NodeStream;
      stream
        .on("data", (row: string[]) => rows.push(row))
        .on("error", () => resolve("<error>"))
        .on("end", () => resolve(rows));
    });
  }
  function ours(
    text: string,
    options: CsvDecoderOptions,
  ): string[][] | "<error>" {
    try {
      // fast-csv always uses "," by default, rather than guessing
      return decode(text, { delimiter: ",", ...options });
    } catch {
      return "<error>";
    }
  }
  // fast-csv emits blank lines as an empty array, rather than an array with
  // one empty field, and drops the spaces from fields that only contain
  // spaces at the start of a row. Neither of these seems intentional, so we
  // don't copy them, and ignore them when comparing.
  const normalise = (rows: string[][] | "<error>") =>
    rows === "<error>"
      ? rows
      : rows
          .map((row) => row.map((field) => (field.trim() ? field : "")))
          .filter(
            (row) =>
              !(row.length === 0 || (row.join("") === "" && row.length === 1)),
          );

  test("decodes CSV written by fast-csv", async () => {
    const rows: string[][] = [];
    for (let i = 0; i < 1000; i++) {
      rows.push([
        String(i),
        `User ${i}`,
        `Says "hello" in ${i % 7} languages\nand likes emoji ✨`,
        i % 3 ? "true" : "false",
        i % 5 ? "" : "a,b,c",
        i % 7 ? " padded " : "line\r\nbreak",
      ]);
    }
    for (const options of [
      {},
      { quoteColumns: true },
      { delimiter: "\t", rowDelimiter: "\r\n" },
      { quote: "'", delimiter: ";" },
    ]) {
      const text = await writeToString(rows, options);
      const decoderOptions = {
        ...("delimiter" in options ? { delimiter: options.delimiter } : {}),
        ...("quote" in options ? { quote: options.quote } : {}),
      };
      expect(decode(text, decoderOptions)).toEqual(rows);
      expect(decode(text.match(/[^]{1,100}/g)!, decoderOptions)).toEqual(rows);
      expect(await fastCsv(text, decoderOptions)).toEqual(rows);
    }
  });

  test.each([{}, { escape: "\\" }, { delimiter: ";", quote: "'" }])(
    "gives the same results as fast-csv for random input with %j",
    async (options) => {
      const alphabet = ["a", ",", ";", '"', "'", "\n", "\r", " ", "\\"];
      const rand = random(7);
      for (let n = 0; n < 5000; n++) {
        let text = "";
        const length = Math.floor(rand() * 16);
        for (let i = 0; i < length; i++) {
          text += alphabet[Math.floor(rand() * alphabet.length)];
        }
        expect(normalise(ours(text, options)), JSON.stringify(text)).toEqual(
          normalise(await fastCsv(text, options)),
        );
      }
    },
  );
});

testTransformRobustness({
  name: "CsvDecoderStream",
  create: () => new CsvDecoderStream(),
  validInput: ["a,b\n", '"c', 'd",e\n'],
  longInput: ["a,b\n", "c,d\n", "e,f\n", "g,h\n"],
  invalidInput: '"x" y\n',
});
