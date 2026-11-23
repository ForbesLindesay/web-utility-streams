import { describe, expect, test, vi } from "vitest";
import CsvEncoderStream from "./CsvEncoderStream.js";
import CsvObjectDecoderStream from "./CsvObjectDecoderStream.js";
import CsvObjectEncoderStream, {
  type TableColumn,
  type CsvObjectEncoderStreamOptions,
} from "./CsvObjectEncoderStream.js";
import TableEncoderStream from "./TableEncoderStream.js";
import { testTransformRobustness } from "./internal/StreamTestUtils.js";
import { collect, streamFrom } from "./internal/TableDecoderTestCases.js";

interface User {
  id: number;
  name: string;
}

const columns = [
  ["id", (u) => String(u.id)],
  ["name", (u) => u.name],
] satisfies TableColumn<User>[];

const users: User[] = [
  { id: 1, name: "Ada" },
  { id: 2, name: 'Grace "Amazing", Hopper\n' },
];

async function encode<T>(
  objects: T[],
  cols: readonly TableColumn<T>[],
  options?: CsvObjectEncoderStreamOptions,
) {
  const { values, error } = await collect(
    streamFrom(objects).pipeThrough(new CsvObjectEncoderStream(cols, options)),
  );
  return { text: values.join(""), error };
}

/**
 * The same as `encode`, but by piping `TableEncoderStream` into
 * `CsvEncoderStream`
 */
async function encodeChained<T>(
  objects: T[],
  cols: readonly TableColumn<T>[],
  options?: CsvObjectEncoderStreamOptions,
) {
  const { values, error } = await collect(
    streamFrom(objects)
      .pipeThrough(new TableEncoderStream(cols, options))
      .pipeThrough(new CsvEncoderStream(options)),
  );
  return { text: values.join(""), error };
}

describe("CsvObjectEncoderStream", () => {
  test("encodes the headers, then a row for each object", async () => {
    expect(await encode(users, columns)).toEqual({
      text: 'id,name\n1,Ada\n2,"Grace ""Amazing"", Hopper\n"',
      error: undefined,
    });
  });

  test("emits the headers and each row as separate chunks", async () => {
    expect(
      await collect(
        streamFrom(users.slice(0, 1)).pipeThrough(
          new CsvObjectEncoderStream(columns, { includeEndRowDelimiter: true }),
        ),
      ),
    ).toEqual({ values: ["id,name\n", "1,Ada\n"] });
  });

  test("passes each object's index to the getters", async () => {
    const getValue = vi.fn((_: User, index: number) => String(index));
    expect((await encode(users, [["i", getValue]])).text).toBe("i\n0\n1");
    expect(getValue.mock.calls).toEqual([
      [users[0], 0],
      [users[1], 1],
    ]);
  });

  test("encodes just the headers for empty input", async () => {
    expect((await encode([], columns)).text).toBe("id,name");
  });

  test("skipHeaders", async () => {
    expect(
      (await encode(users.slice(0, 1), columns, { skipHeaders: true })).text,
    ).toBe("1,Ada");
    expect(
      (
        await encode(users.slice(0, 1), columns, {
          skipHeaders: true,
          writeBOM: true,
        })
      ).text,
    ).toBe("﻿1,Ada");
    expect((await encode([], columns, { skipHeaders: true })).text).toBe("");
  });

  test("passes CSV options to the encoder", async () => {
    expect(
      (
        await encode(users.slice(0, 1), columns, {
          delimiter: ";",
          quoteMode: "ALWAYS",
          rowDelimiter: "\r\n",
          includeEndRowDelimiter: true,
          writeBOM: true,
        })
      ).text,
    ).toBe('﻿"id";"name"\r\n"1";"Ada"\r\n');
  });

  test("invalid CSV options throw when the stream is created", () => {
    expect(
      () => new CsvObjectEncoderStream(columns, { delimiter: "ab" }),
    ).toThrow(TypeError);
  });

  test("an error in a getter errors the stream", async () => {
    const result = await encode(users, [
      [
        "name",
        (u) => {
          if (u.id === 2) throw new Error("Bad user");
          return u.name;
        },
      ],
    ]);
    expect(result.text).toBe("name\nAda");
    expect(result.error).toMatchObject({ message: "Bad user" });
  });

  test("round trips through CsvObjectDecoderStream", async () => {
    const { values } = await collect(
      streamFrom(users)
        .pipeThrough(new CsvObjectEncoderStream(columns))
        .pipeThrough(
          new CsvObjectDecoderStream((row) => ({
            id: Number(row.get("id")),
            name: row.get("name"),
          })),
        ),
    );
    expect(values).toEqual(users);
  });

  test.each<[string, CsvObjectEncoderStreamOptions | undefined]>([
    ["default options", undefined],
    ["includeEndRowDelimiter", { includeEndRowDelimiter: true }],
    ["writeBOM", { writeBOM: true }],
    ["skipHeaders", { skipHeaders: true }],
    ["skipHeaders and writeBOM", { skipHeaders: true, writeBOM: true }],
    [
      "skipHeaders and includeEndRowDelimiter",
      { skipHeaders: true, includeEndRowDelimiter: true },
    ],
    ["quoteMode NEVER", { quoteMode: "NEVER" }],
  ])(
    "gives the same output as TableEncoderStream piped into CsvEncoderStream: %s",
    async (_, options) => {
      for (const input of [[], users.slice(0, 1), users]) {
        expect(await encode(input, columns, options)).toEqual(
          await encodeChained(input, columns, options),
        );
      }
    },
  );
});

testTransformRobustness({
  name: "CsvObjectEncoderStream",
  create: () => new CsvObjectEncoderStream(columns),
  validInput: users,
  invalidInput: null,
});
