// Internal module, not exported from the package. Shared by
// `JsonTokenDecoderStream` and `JsonDecoderStream`, so that
// `JsonDecoderStream` can process tokens directly, without the overhead of
// passing each token through a stream.

/**
 * A token in a JSON document. `position` is the index of the token's first
 * character in the input text.
 *
 * A string is a `KEY` token if it is directly inside an object, immediately
 * after the `{` or a `,`. All other strings are `LITERAL` tokens.
 */
export type JsonToken =
  | {
      readonly type: "PUNCTUATION";
      readonly value: "{" | "}" | "[" | "]" | ":" | ",";
      readonly position: number;
    }
  | {
      readonly type: "KEY";
      readonly value: string;
      readonly position: number;
    }
  | {
      readonly type: "LITERAL";
      readonly value: string | number | boolean | null;
      readonly position: number;
    };

const NUMBER_CHAR = /[0-9eE+\-.]/;
const LITERAL_CHAR = /[a-zA-Z]/;

export interface JsonTokenizer {
  /**
   * Process the next chunk of text, calling `onToken` for each complete token
   */
  write(chunk: string, onToken: (token: JsonToken) => void): void;
  /**
   * Call once there is no more text, to emit any final token
   */
  end(onToken: (token: JsonToken) => void): void;
}

/**
 * Split JSON text, which can be split into chunks at any point, into tokens.
 * Throws a `SyntaxError`, with a message starting with `name`, for invalid
 * tokens.
 */
export function createJsonTokenizer(name: string): JsonTokenizer {
  // Strings, numbers and literals can be split across chunks, so we
  // accumulate the text of the current token until it is complete.
  let tokenKind: "string" | "number" | "literal" | null = null;
  let tokenText = "";
  let tokenPosition = 0;
  let isEscaped = false;
  // The number of characters in all previous chunks
  let offset = 0;
  // To tell keys apart from other strings, we track which brackets are open,
  // and whether the next token is in the position of an object key. We don't
  // check that the brackets match, as we don't check the document structure.
  const openBrackets: ("{" | "[")[] = [];
  let isKeyPosition = false;

  const unexpected = (description: string, position: number): never => {
    throw new SyntaxError(
      `${name}: Unexpected ${description} at position ${position}`,
    );
  };

  const endToken = (): JsonToken => {
    const text = tokenText;
    const kind = tokenKind;
    const position = tokenPosition;
    const isKey = isKeyPosition;
    tokenKind = null;
    tokenText = "";
    isKeyPosition = false;
    if (kind === "literal") {
      switch (text) {
        case "true":
          return { type: "LITERAL", value: true, position };
        case "false":
          return { type: "LITERAL", value: false, position };
        case "null":
          return { type: "LITERAL", value: null, position };
        default:
          return unexpected(`token ${text}`, position);
      }
    }
    // JSON.parse validates the string escapes or number format
    let value: string | number;
    try {
      value = JSON.parse(text);
    } catch {
      return unexpected(
        kind === "string" ? "string" : `number ${text}`,
        position,
      );
    }
    return kind === "string" && isKey
      ? { type: "KEY", value: value as string, position }
      : { type: "LITERAL", value, position };
  };

  return {
    write(chunk, onToken) {
      let i = 0;
      while (i < chunk.length) {
        if (tokenKind === "string") {
          let end = i;
          for (; end < chunk.length; end++) {
            if (isEscaped) {
              isEscaped = false;
            } else if (chunk[end] === "\\") {
              isEscaped = true;
            } else if (chunk[end] === '"') {
              break;
            }
          }
          if (end === chunk.length) {
            // The string continues in the next chunk
            tokenText += chunk.slice(i);
            i = end;
          } else {
            tokenText += chunk.slice(i, end + 1);
            i = end + 1;
            onToken(endToken());
          }
          continue;
        }
        if (tokenKind !== null) {
          const pattern = tokenKind === "number" ? NUMBER_CHAR : LITERAL_CHAR;
          let end = i;
          while (end < chunk.length && pattern.test(chunk[end]!)) end++;
          tokenText += chunk.slice(i, end);
          i = end;
          // If we reached the end of the chunk, the token may continue in
          // the next chunk. Otherwise, the next character ends the token.
          if (end < chunk.length) onToken(endToken());
          continue;
        }

        const char = chunk[i]!;
        const position = offset + i;
        if (char === " " || char === "\t" || char === "\n" || char === "\r") {
          i++;
        } else if (
          char === "{" ||
          char === "}" ||
          char === "[" ||
          char === "]" ||
          char === ":" ||
          char === ","
        ) {
          if (char === "{" || char === "[") {
            openBrackets.push(char);
          } else if (char === "}" || char === "]") {
            openBrackets.pop();
          }
          isKeyPosition =
            char === "{" || (char === "," && openBrackets.at(-1) === "{");
          onToken({ type: "PUNCTUATION", value: char, position });
          i++;
        } else {
          if (char === '"') {
            tokenKind = "string";
            // Consume the opening quote, so the string scanning above
            // doesn't treat it as the closing quote. Numbers and literals
            // are consumed by the scanning above.
            tokenText = char;
            i++;
          } else if (char === "-" || (char >= "0" && char <= "9")) {
            tokenKind = "number";
          } else if (LITERAL_CHAR.test(char)) {
            tokenKind = "literal";
          } else {
            unexpected(`character ${JSON.stringify(char)}`, position);
          }
          tokenPosition = position;
        }
      }
      offset += chunk.length;
    },
    end(onToken) {
      if (tokenKind === "string") {
        throw new SyntaxError(`${name}: Unexpected end of JSON input`);
      }
      if (tokenKind !== null) {
        // A number or literal at the very end of the input is complete
        onToken(endToken());
      }
    },
  };
}
