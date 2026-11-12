import fs from "node:fs";
import * as nodeConsumers from "node:stream/consumers";
import { bytesFromStream } from "web-utility-streams/consumers";
import { consumeFile, OURS, type Suite } from "../shared.ts";

const checkBytes = (bytes: Uint8Array) => ({ bytes: bytes.byteLength });

export default {
  format: "ndjson",
  description: "Read a whole file stream into a single `Uint8Array`",
  cases: [
    {
      library: OURS,
      name: "bytesFromStream",
      ours: true,
      run: ({ file }) => consumeFile(file, bytesFromStream, checkBytes),
    },
    {
      library: "node:stream/consumers",
      name: "bytes",
      run: ({ file }) => consumeFile(file, nodeConsumers.bytes, checkBytes),
    },
    {
      library: "native",
      name: "new Response(stream).bytes()",
      run: ({ file }) =>
        consumeFile(file, (stream) => new Response(stream).bytes(), checkBytes),
    },
    {
      library: "baseline",
      name: "fs.promises.readFile(file) (not streaming)",
      baseline: true,
      run: async ({ file }) => {
        const bytes = await fs.promises.readFile(file);
        return { finish: () => checkBytes(bytes) };
      },
    },
  ],
} satisfies Suite;
