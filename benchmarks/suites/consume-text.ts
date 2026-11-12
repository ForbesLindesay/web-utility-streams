import fs from "node:fs";
import * as nodeConsumers from "node:stream/consumers";
import { textFromStream } from "web-utility-streams/consumers";
import {
  consumeFile,
  MAX_IN_MEMORY_BYTES,
  OURS,
  type Suite,
} from "../shared.ts";

const checkText = (text: string) => ({ bytes: Buffer.byteLength(text) });

export default {
  format: "ndjson",
  description:
    "Read a whole file stream (as bytes) into a single string. The result must fit in a single string, so this only runs for sizes up to 512MB",
  cases: [
    {
      library: OURS,
      name: "textFromStream",
      ours: true,
      maxSize: MAX_IN_MEMORY_BYTES,
      run: ({ file }) => consumeFile(file, textFromStream, checkText),
    },
    {
      library: OURS,
      name: "textFromStream after TextDecoderStream (string chunks)",
      ours: true,
      maxSize: MAX_IN_MEMORY_BYTES,
      run: ({ file }) =>
        consumeFile(
          file,
          (stream) =>
            textFromStream(stream.pipeThrough(new TextDecoderStream())),
          checkText,
        ),
    },
    {
      library: "node:stream/consumers",
      name: "text",
      maxSize: MAX_IN_MEMORY_BYTES,
      run: ({ file }) => consumeFile(file, nodeConsumers.text, checkText),
    },
    {
      library: "native",
      name: "new Response(stream).text()",
      maxSize: MAX_IN_MEMORY_BYTES,
      run: ({ file }) =>
        consumeFile(file, (stream) => new Response(stream).text(), checkText),
    },
    {
      library: "baseline",
      name: 'fs.promises.readFile(file, "utf8") (not streaming)',
      baseline: true,
      maxSize: MAX_IN_MEMORY_BYTES,
      run: async ({ file }) => {
        const text = await fs.promises.readFile(file, "utf8");
        return { finish: () => checkText(text) };
      },
    },
  ],
} satisfies Suite;
