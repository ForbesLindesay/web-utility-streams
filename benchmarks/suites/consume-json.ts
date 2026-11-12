import fs from "node:fs";
import * as nodeConsumers from "node:stream/consumers";
import { jsonFromStream } from "web-utility-streams/consumers";
import {
  consumeFile,
  type Counted,
  MAX_IN_MEMORY_BYTES,
  OURS,
  type Suite,
  type WithId,
} from "../shared.ts";

function checkJson(value: unknown): Counted {
  let count = 0;
  let idSum = 0;
  for (const record of (value as { results: WithId[] }).results) {
    count++;
    idSum += record.id;
  }
  return { count, idSum };
}

export default {
  format: "json",
  description:
    "Read a whole JSON file stream (as bytes) and parse it. The text must fit in a single string, so this only runs for sizes up to 512MB",
  cases: [
    {
      library: OURS,
      name: "jsonFromStream",
      ours: true,
      maxSize: MAX_IN_MEMORY_BYTES,
      run: ({ file }) => consumeFile(file, jsonFromStream, checkJson),
    },
    {
      library: "node:stream/consumers",
      name: "json",
      maxSize: MAX_IN_MEMORY_BYTES,
      run: ({ file }) => consumeFile(file, nodeConsumers.json, checkJson),
    },
    {
      library: "native",
      name: "new Response(stream).json()",
      maxSize: MAX_IN_MEMORY_BYTES,
      run: ({ file }) =>
        consumeFile(file, (stream) => new Response(stream).json(), checkJson),
    },
    {
      library: "baseline",
      name: 'JSON.parse(await fs.promises.readFile(file, "utf8")) (not streaming)',
      baseline: true,
      maxSize: MAX_IN_MEMORY_BYTES,
      run: async ({ file }) => {
        const value = JSON.parse(await fs.promises.readFile(file, "utf8"));
        return { finish: () => checkJson(value) };
      },
    },
  ],
} satisfies Suite;
