import { test, expect } from "vitest";
import {
  delay,
  readableStream,
  readableStreamResults,
  testTransformRobustness,
} from "./StreamTestUtils.js";
import MapStream from "./MapStream.js";
import NewlineDelimitedJsonEncoderStream from "./NewlineDelimitedJsonEncoderStream.js";
import SplitLinesStream from "./SplitLinesStream.js";
import TransformPipelineStream from "./TransformPipelineStream.js";

test("TransformPipelineStream - Can combine a series of transform streams into one overall transform", async () => {
  expect(
    await readableStreamResults(
      readableStream([1, 2, 3, 4]).pipeThrough(
        new TransformPipelineStream((input) =>
          input
            .pipeThrough(
              new TransformStream({
                transform(chunk, controller) {
                  controller.enqueue(chunk.toString());
                },
              }),
            )
            .pipeThrough(
              new TransformStream({
                transform(chunk, controller) {
                  controller.enqueue(chunk.repeat(2));
                },
              }),
            )
            .pipeThrough(
              new TransformStream({
                transform(chunk, controller) {
                  controller.enqueue(parseInt(chunk, 10));
                },
              }),
            ),
        ),
      ) satisfies ReadableStream<number>,
    ),
  ).toEqual([
    { ok: true, value: 11 },
    { ok: true, value: 22 },
    { ok: true, value: 33 },
    { ok: true, value: 44 },
  ]);
});

testTransformRobustness({
  name: "TransformPipelineStream",
  create: () =>
    new TransformPipelineStream((input) =>
      input
        .pipeThrough(new NewlineDelimitedJsonEncoderStream())
        .pipeThrough(new SplitLinesStream())
        .pipeThrough(
          new MapStream(async (line: string) => {
            if (line === '"bad"') throw new Error("Bad value");
            await delay(5);
            return line;
          }),
        ),
    ),
  validInput: [1, 2, 3],
  invalidInput: "bad",
});
