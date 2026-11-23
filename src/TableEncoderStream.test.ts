import { describe, expect, test, vi } from "vitest";
import TableEncoderStream, {
  type TableColumn,
  type TableEncoderStreamOptions,
} from "./TableEncoderStream.js";
import { testTransformRobustness } from "./internal/StreamTestUtils.js";
import { collect, streamFrom } from "./internal/TableDecoderTestCases.js";

interface User {
  id: number;
  name: string;
}

const columns: TableColumn<User>[] = [
  ["id", (u) => String(u.id)],
  ["name", (u) => u.name],
];

const users: User[] = [
  { id: 1, name: "Ada" },
  { id: 2, name: "Grace" },
];

function encode<T>(
  objects: T[],
  cols: readonly TableColumn<T>[],
  options?: TableEncoderStreamOptions,
) {
  return collect(
    streamFrom(objects).pipeThrough(new TableEncoderStream(cols, options)),
  );
}

describe("TableEncoderStream", () => {
  test("encodes the headers, then a row for each object", async () => {
    expect(await encode(users, columns)).toEqual({
      values: [
        ["id", "name"],
        ["1", "Ada"],
        ["2", "Grace"],
      ],
    });
  });

  test("uses the order of the columns array", async () => {
    expect(
      await encode(users, [
        ["2025", (u) => u.name],
        ["2024", (u) => String(u.id)],
      ]),
    ).toEqual({
      values: [
        ["2025", "2024"],
        ["Ada", "1"],
        ["Grace", "2"],
      ],
    });
  });

  test("allows duplicate headers", async () => {
    expect(
      await encode(
        [users[0]!],
        [
          ["x", (u) => u.name],
          ["x", (u) => String(u.id)],
        ],
      ),
    ).toEqual({
      values: [
        ["x", "x"],
        ["Ada", "1"],
      ],
    });
  });

  test("passes each object's index to the getters", async () => {
    const getValue = vi.fn((_: User, index: number) => String(index));
    expect(await encode(users, [["i", getValue]])).toEqual({
      values: [["i"], ["0"], ["1"]],
    });
    expect(getValue.mock.calls).toEqual([
      [users[0], 0],
      [users[1], 1],
    ]);
  });

  test("encodes just the headers for empty input", async () => {
    expect(await encode([], columns)).toEqual({ values: [["id", "name"]] });
  });

  test("skipHeaders", async () => {
    expect(await encode(users, columns, { skipHeaders: true })).toEqual({
      values: [
        ["1", "Ada"],
        ["2", "Grace"],
      ],
    });
    expect(await encode([], columns, { skipHeaders: true })).toEqual({
      values: [],
    });
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
    expect(result.values).toEqual([["name"], ["Ada"]]);
    expect(result.error).toMatchObject({ message: "Bad user" });
  });
});

testTransformRobustness({
  name: "TableEncoderStream",
  create: () => new TableEncoderStream(columns),
  validInput: users,
  invalidInput: null,
});
