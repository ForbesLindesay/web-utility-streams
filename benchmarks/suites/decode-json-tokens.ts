import JsonTokenDecoderStream from "web-utility-streams/JsonTokenDecoderStream";
import StreamParserTokenizer from "@streamparser/json-whatwg/tokenizer.js";
import webChain from "stream-chain/web";
import { parser as streamJsonParser } from "stream-json";
import {
  asTransform,
  type Counted,
  OURS,
  readFileBaseline,
  type Suite,
  webFile,
} from "../shared.ts";

/**
 * Count the records in a stream of JSON tokens, and sum their ids, using
 * `isIdKey` and `getNumber` to read each library's tokens. Records are the
 * only objects with an "id" key, so the value after each "id" key is a
 * record's id.
 */
async function countTokens<T>(
  tokens: AsyncIterable<T>,
  isIdKey: (token: T) => boolean,
  getNumber: (token: T) => unknown,
): Promise<Counted> {
  let count = 0;
  let idSum = 0;
  let afterIdKey = false;
  for await (const token of tokens) {
    if (afterIdKey) {
      const id = getNumber(token);
      if (id !== undefined) {
        count++;
        idSum += id as number;
        afterIdKey = false;
      }
    } else if (isIdKey(token)) {
      afterIdKey = true;
    }
  }
  return { count, idSum };
}

export default {
  format: "json",
  description:
    "Split a JSON file in the shape of a paginated API response into tokens",
  cases: [
    {
      library: OURS,
      name: "JsonTokenDecoderStream",
      ours: true,
      run: async ({ file }) => {
        const { stream, close } = await webFile(file);
        try {
          return await countTokens(
            stream
              .pipeThrough(new TextDecoderStream())
              .pipeThrough(new JsonTokenDecoderStream()),
            (token) => token.type === "KEY" && token.value === "id",
            (token) => (token.type === "LITERAL" ? token.value : undefined),
          );
        } finally {
          await close();
        }
      },
    },
    {
      library: "@streamparser/json",
      name: "Tokenizer (web streams)",
      run: async ({ file }) => {
        const { stream, close } = await webFile(file);
        try {
          return await countTokens(
            stream
              .pipeThrough(new TextDecoderStream())
              .pipeThrough(new StreamParserTokenizer()),
            // Token types 9 and 10 are STRING and NUMBER
            (token) => token.token === 9 && token.value === "id",
            (token) => (token.token === 10 ? token.value : undefined),
          );
        } finally {
          await close();
        }
      },
    },
    {
      library: "stream-json",
      name: "parser({ streamValues: false }) (web streams)",
      run: async ({ file }) => {
        const { stream, close } = await webFile(file);
        try {
          return await countTokens(
            stream
              .pipeThrough(new TextDecoderStream())
              .pipeThrough(
                asTransform(
                  webChain([streamJsonParser({ streamValues: false })]),
                ),
              ),
            (token) => token.name === "keyValue" && token.value === "id",
            (token) =>
              token.name === "numberValue" ? Number(token.value) : undefined,
          );
        } finally {
          await close();
        }
      },
    },
    readFileBaseline,
  ],
} satisfies Suite;
