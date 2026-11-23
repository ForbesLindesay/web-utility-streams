import { describe, expect, test, vi } from "vitest";
import readableStreamToGenerator from "../readableStreamToGenerator.js";
import type {
  TableDecoderStreamOptions,
  TableRow,
} from "../TableDecoderStream.js";

export interface Decoded<T> {
  values: T[];
  error?: unknown;
}

/**
 * Decode a table (the first row is the headers) into objects. Implemented
 * once for `TableDecoderStream`, and once for `CsvObjectDecoderStream` (which
 * encodes the rows as CSV first).
 */
export type DecodeTable = <T>(
  rows: string[][],
  getObject: (row: TableRow) => T,
  options?: TableDecoderStreamOptions<T>,
) => Promise<Decoded<T>>;

/**
 * A stream of `chunks`, with no delay between them
 */
export function streamFrom<T>(chunks: Iterable<T>): ReadableStream<T> {
  const iterator = chunks[Symbol.iterator]();
  return new ReadableStream<T>({
    pull(controller) {
      const next = iterator.next();
      if (next.done) controller.close();
      else controller.enqueue(next.value);
    },
  });
}

/**
 * Read every value from a stream, and the error it ends with (if any)
 */
export async function collect<T>(
  stream: ReadableStream<T>,
): Promise<Decoded<T>> {
  const values: T[] = [];
  try {
    for await (const value of readableStreamToGenerator(stream)) {
      values.push(value);
    }
    return { values };
  } catch (error) {
    return { values, error };
  }
}

const toObject = (row: TableRow) => row.toObject();

const parseNumber = (value: string): number => {
  const n = Number(value);
  if (value === "" || Number.isNaN(n)) {
    throw new Error(`Not a number: ${JSON.stringify(value)}`);
  }
  return n;
};

/**
 * Tests for the behaviour shared by `TableDecoderStream` and
 * `CsvObjectDecoderStream`
 */
export function testTableDecoder(name: string, decode: DecodeTable): void {
  describe(`${name} - objects`, () => {
    test("decodes rows into objects", async () => {
      expect(
        await decode(
          [
            ["id", "name"],
            ["1", "Ada"],
            ["2", "Grace"],
          ],
          (row) => ({ id: Number(row.get("id")), name: row.get("name") }),
        ),
      ).toEqual({
        values: [
          { id: 1, name: "Ada" },
          { id: 2, name: "Grace" },
        ],
      });
    });

    test("columns are found by name, not position", async () => {
      expect(
        await decode(
          [
            ["b", "a"],
            ["2", "1"],
          ],
          (row) => [row.get("a"), row.get("b")],
        ),
      ).toEqual({ values: [["1", "2"]] });
    });

    test("index counts rows from 0, not counting the header row", async () => {
      expect(
        await decode([["a"], ["x"], ["y"], ["z"]], (row) => row.index),
      ).toEqual({ values: [0, 1, 2] });
    });

    test("emits nothing for empty input", async () => {
      const onHeaders = vi.fn();
      const getObject = vi.fn();
      expect(await decode([], getObject, { onHeaders })).toEqual({
        values: [],
      });
      expect(onHeaders).not.toHaveBeenCalled();
      expect(getObject).not.toHaveBeenCalled();
    });

    test("onHeaders is called with the header row, before any objects", async () => {
      const events: unknown[] = [];
      await decode(
        [
          ["a", "b"],
          ["1", "2"],
        ],
        (row) => events.push(["row", row.index]),
        { onHeaders: (headers) => events.push(["headers", [...headers]]) },
      );
      expect(events).toEqual([
        ["headers", ["a", "b"]],
        ["row", 0],
      ]);
    });

    test("onHeaders is called when there are no other rows", async () => {
      const onHeaders = vi.fn();
      expect(await decode([["a", "b"]], toObject, { onHeaders })).toEqual({
        values: [],
      });
      expect(onHeaders).toHaveBeenCalledTimes(1);
      expect([...onHeaders.mock.calls[0]![0]]).toEqual(["a", "b"]);
    });

    test("an error in onHeaders errors the stream", async () => {
      const result = await decode(
        [
          ["a", "b"],
          ["1", "2"],
        ],
        toObject,
        {
          onHeaders: () => {
            throw new Error("Bad headers");
          },
        },
      );
      expect(result.values).toEqual([]);
      expect(result.error).toMatchObject({ message: "Bad headers" });
    });
  });

  describe(`${name} - get and getOptional`, () => {
    test("getOptional returns undefined for a column that isn't in the headers", async () => {
      expect(
        await decode([["a"], ["1"]], (row) => [
          row.getOptional("a"),
          row.getOptional("b"),
        ]),
      ).toEqual({ values: [["1", undefined]] });
    });

    test("get errors the stream for a column that isn't in the headers", async () => {
      const result = await decode([["a"], ["1"], ["2"]], (row) => row.get("b"));
      expect(result.values).toEqual([]);
      expect(result.error).toMatchObject({
        message: "Error decoding row 0: Missing column: b",
        cause: { message: "Missing column: b" },
      });
    });

    test("cells missing from short rows are empty strings", async () => {
      expect(
        await decode(
          [["a", "b", "c"], ["1", "2", "3"], ["4", "5"], ["6"]],
          (row) => [row.get("a"), row.get("b"), row.getOptional("c")],
        ),
      ).toEqual({
        values: [
          ["1", "2", "3"],
          ["4", "5", ""],
          ["6", "", ""],
        ],
      });
    });

    test("a blank row has an empty string for every column", async () => {
      expect(
        await decode([["a", "b"], ["1", "2"], [], ["3", "4"]], toObject),
      ).toEqual({
        values: [
          { a: "1", b: "2" },
          { a: "", b: "" },
          { a: "3", b: "4" },
        ],
      });
    });

    test("rows with more cells than there are headers error the stream", async () => {
      const result = await decode(
        [
          ["a", "b"],
          ["1", "2"],
          ["3", "4", "5"],
          ["6", "7"],
        ],
        toObject,
      );
      expect(result.values).toEqual([{ a: "1", b: "2" }]);
      expect(result.error).toMatchObject({
        message: "Error decoding row 1: Expected 2 columns, got 3",
      });
    });

    test("duplicate headers are allowed if they're never read", async () => {
      expect(
        await decode(
          [
            ["a", "b", "a"],
            ["1", "2", "3"],
          ],
          (row) => row.get("b"),
        ),
      ).toEqual({ values: ["2"] });
    });

    test.each<[string, (row: TableRow) => unknown]>([
      ["get", (row: TableRow) => row.get("a")],
      ["getOptional", (row: TableRow) => row.getOptional("a")],
      ["parse", (row: TableRow) => row.parse("a", parseNumber)],
      ["parseOptional", (row: TableRow) => row.parseOptional("a", parseNumber)],
      ["toObject", (row: TableRow) => row.toObject()],
    ])("%s errors the stream for a duplicate header", async (_, getObject) => {
      const result = await decode(
        [
          ["a", "b", "a"],
          ["1", "2", "3"],
        ],
        getObject,
      );
      expect(result.values).toEqual([]);
      expect(result.error).toMatchObject({
        message: "Error decoding row 0: Duplicate column: a",
      });
    });

    test("errors thrown by getObject include the row index", async () => {
      const result = await decode([["a"], ["1"], ["2"]], (row) => {
        if (row.index === 1) throw new Error("Bad row");
        return row.get("a");
      });
      expect(result.values).toEqual(["1"]);
      expect(result.error).toMatchObject({
        message: "Error decoding row 1: Bad row",
        cause: { message: "Bad row" },
      });
    });

    test("non-Error values thrown by getObject are wrapped", async () => {
      const result = await decode([["a"], ["1"]], () => {
        throw "oops";
      });
      expect(result.error).toMatchObject({
        message: "Error decoding row 0: oops",
        cause: "oops",
      });
    });
  });

  describe(`${name} - toObject`, () => {
    test("returns the row keyed by column name", async () => {
      expect(
        await decode(
          [
            ["a", "b"],
            ["1", "2"],
          ],
          toObject,
        ),
      ).toEqual({ values: [{ a: "1", b: "2" }] });
    });

    test("a __proto__ header is an own property", async () => {
      const result = await decode(
        [
          ["__proto__", "b"],
          ["1", "2"],
        ],
        toObject,
      );
      const obj = result.values[0]!;
      expect(Object.getPrototypeOf(obj)).toBe(Object.prototype);
      expect(Object.keys(obj)).toEqual(["__proto__", "b"]);
      expect(Object.getOwnPropertyDescriptor(obj, "__proto__")?.value).toBe(
        "1",
      );
    });
  });

  describe(`${name} - parse and parseOptional`, () => {
    test("parse accepts a function or an object with a parse method", async () => {
      expect(
        await decode(
          [
            ["a", "b"],
            ["1", "2"],
          ],
          (row) => [
            row.parse("a", parseNumber),
            row.parse("b", { parse: parseNumber }),
          ],
        ),
      ).toEqual({ values: [[1, 2]] });
    });

    test("parse errors the stream for a column that isn't in the headers", async () => {
      const parser = vi.fn(parseNumber);
      const result = await decode([["a"], ["1"]], (row) =>
        row.parse("b", parser),
      );
      expect(result.error).toMatchObject({
        message: "Error decoding row 0: Missing column: b",
      });
      expect(parser).not.toHaveBeenCalled();
    });

    test("parseOptional returns undefined, without calling the parser, for a column that isn't in the headers", async () => {
      const parser = vi.fn(parseNumber);
      expect(
        await decode([["a"], ["1"]], (row) => [
          row.parseOptional("a", parser),
          row.parseOptional("b", parser),
        ]),
      ).toEqual({ values: [[1, undefined]] });
      expect(parser).toHaveBeenCalledTimes(1);
    });

    test("a parser that throws makes the cell invalid", async () => {
      const result = await decode([["a"], ["1"], ["x"], ["2"]], (row) =>
        row.parse("a", parseNumber),
      );
      expect(result.values).toEqual([1]);
      expect(result.error).toMatchObject({
        message: "Error decoding row 1: Invalid column: a",
        cause: {
          message: "Invalid column: a",
          cause: { message: 'Not a number: "x"' },
        },
      });
    });
  });

  describe(`${name} - handlers`, () => {
    const rows = [["a", "b"], ["1", "2"], ["3"], ["x", "4"], ["5", "6", "7"]];

    test("onMissingColumn is called instead of erroring, and the row is skipped", async () => {
      const onMissingColumn = vi.fn();
      const onInvalidRow = vi.fn();
      expect(
        await decode(
          [["a"], ["1"], ["2"]],
          (row) => [row.get("a"), row.get(row.index === 0 ? "b" : "a")],
          { onMissingColumn, onInvalidRow },
        ),
      ).toEqual({ values: [["2", "2"]] });
      expect(onMissingColumn).toHaveBeenCalledTimes(1);
      expect(onMissingColumn).toHaveBeenCalledWith("b", expect.anything());
      expect(onInvalidRow).not.toHaveBeenCalled();
    });

    test("onDuplicateColumn is called instead of erroring, and the row is skipped", async () => {
      const onDuplicateColumn = vi.fn();
      const onInvalidRow = vi.fn();
      expect(
        await decode(
          [
            ["a", "a", "b"],
            ["1", "2", "3"],
            ["4", "5", "6"],
          ],
          (row) => (row.index === 0 ? row.get("a") : row.get("b")),
          { onDuplicateColumn, onInvalidRow },
        ),
      ).toEqual({ values: ["6"] });
      expect(onDuplicateColumn).toHaveBeenCalledTimes(1);
      expect(onDuplicateColumn).toHaveBeenCalledWith("a", expect.anything());
      expect(onInvalidRow).not.toHaveBeenCalled();
    });

    test("onInvalidCell is called with the parser's error, and the row is skipped", async () => {
      const onInvalidCell = vi.fn();
      const onInvalidRow = vi.fn();
      expect(
        await decode(
          [
            ["a", "b"],
            ["1", "2"],
            ["x", "4"],
            ["5", "6"],
          ],
          (row) => [row.parse("a", parseNumber), row.parse("b", parseNumber)],
          { onInvalidCell, onInvalidRow },
        ),
      ).toEqual({
        values: [
          [1, 2],
          [5, 6],
        ],
      });
      expect(onInvalidCell).toHaveBeenCalledTimes(1);
      expect(onInvalidCell).toHaveBeenCalledWith(
        1,
        "a",
        expect.objectContaining({ message: 'Not a number: "x"' }),
        expect.anything(),
      );
      expect(onInvalidRow).not.toHaveBeenCalled();
    });

    test("onInvalidRow is called for every kind of error that doesn't have its own handler", async () => {
      const onInvalidRow = vi.fn();
      expect(
        await decode(
          rows,
          (row) => {
            if (row.index === 1) return row.get("missing");
            return [row.parse("a", parseNumber), row.get("b")];
          },
          { onInvalidRow },
        ),
      ).toEqual({ values: [[1, "2"]] });
      expect(
        onInvalidRow.mock.calls.map(([index, err]) => [index, err.message]),
      ).toEqual([
        [1, "Missing column: missing"],
        [2, "Invalid column: a"],
        [3, "Expected 2 columns, got 3"],
      ]);
    });

    test("onInvalidRow is called for errors thrown by getObject", async () => {
      const onInvalidRow = vi.fn();
      expect(
        await decode(
          [["a"], ["1"], ["2"]],
          (row) => {
            if (row.index === 0) throw "oops";
            return row.get("a");
          },
          { onInvalidRow },
        ),
      ).toEqual({ values: ["2"] });
      expect(onInvalidRow).toHaveBeenCalledWith(
        0,
        expect.objectContaining({ message: "oops" }),
        expect.anything(),
      );
    });

    test("a column error that getObject catches and replaces goes to onInvalidRow", async () => {
      const onMissingColumn = vi.fn();
      const onInvalidRow = vi.fn();
      await decode(
        [["a"], ["1"]],
        (row) => {
          try {
            return row.get("b");
          } catch {
            throw new Error("Replaced");
          }
        },
        { onMissingColumn, onInvalidRow },
      );
      expect(onMissingColumn).not.toHaveBeenCalled();
      expect(onInvalidRow).toHaveBeenCalledWith(
        0,
        expect.objectContaining({ message: "Replaced" }),
        expect.anything(),
      );
    });

    test("handlers can enqueue a replacement value", async () => {
      expect(
        await decode(
          [["a"], ["1"], ["x"]],
          (row) => row.parse("a", parseNumber),
          {
            onInvalidCell: (index, _column, _err, controller) =>
              controller.enqueue(-index),
          },
        ),
      ).toEqual({ values: [1, -1] });
    });

    test("handlers can error the stream", async () => {
      const result = await decode(
        [["a"], ["1"], ["2"], ["3"]],
        (row) => (row.index === 1 ? row.get("b") : row.get("a")),
        {
          onMissingColumn: (column, controller) =>
            controller.error(new Error(`Handler: ${column}`)),
        },
      );
      expect(result.values).toEqual(["1"]);
      expect(result.error).toMatchObject({ message: "Handler: b" });
    });

    test("an error thrown by a handler errors the stream", async () => {
      const result = await decode([["a"], ["1", "2"]], toObject, {
        onInvalidRow: () => {
          throw new Error("Handler threw");
        },
      });
      expect(result.error).toMatchObject({ message: "Handler threw" });
    });
  });
}
