# Benchmark results

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## concat

Join 10 generated streams of `{ id }` objects, one after another.

### 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **ConcatStream with { highWaterMark: 0 }** | **9 ms (3.9M chunks/s)** | 93.1 MB |
| **web-utility-streams** | **ConcatStream** | **9 ms (3.7M chunks/s)** | 94.5 MB |
| native | ReadableStream.from(async generator) | 12 ms (2.8M chunks/s) | 94.1 MB |
| native | TransformStream, piping each source into it in turn | 14 ms (2.4M chunks/s) | 97.9 MB |
| baseline | Read each source stream in turn | 5 ms (7.4M chunks/s) | 92.3 MB |

### 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **ConcatStream with { highWaterMark: 0 }** | **126 ms (6.9M chunks/s)** | 97.8 MB |
| **web-utility-streams** | **ConcatStream** | **132 ms (6.6M chunks/s)** | 97.6 MB |
| native | TransformStream, piping each source into it in turn | 182 ms (4.8M chunks/s) | 102 MB |
| native | ReadableStream.from(async generator) | 204 ms (4.3M chunks/s) | 97.2 MB |
| baseline | Read each source stream in turn | 50 ms (17.2M chunks/s) | 92.0 MB |

### 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **ConcatStream** | **951 ms (7.1M chunks/s)** | 106 MB |
| **web-utility-streams** | **ConcatStream with { highWaterMark: 0 }** | **956 ms (7.1M chunks/s)** | 106 MB |
| native | TransformStream, piping each source into it in turn | 1.34 s (5.1M chunks/s) | 111 MB |
| native | ReadableStream.from(async generator) | 1.47 s (4.6M chunks/s) | 106 MB |
| baseline | Read each source stream in turn | 374 ms (18.1M chunks/s) | 96.5 MB |

## consume-bytes

Read a whole file stream into a single `Uint8Array`.

### 10MB (10.1 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **bytesFromStream** | **12 ms (848 MB/s)** | 115 MB |
| node:stream/consumers | bytes | 14 ms (699 MB/s) | 135 MB |
| native | new Response(stream).bytes() | 21 ms (490 MB/s) | 132 MB |
| baseline | fs.promises.readFile(file) (not streaming) | 2 ms (4377 MB/s) | 102 MB |

### 256MB (262 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **bytesFromStream** | **222 ms (1180 MB/s)** | 627 MB |
| native | new Response(stream).bytes() | 251 ms (1043 MB/s) | 897 MB |
| node:stream/consumers | bytes | 352 ms (744 MB/s) | 1.16 GB |
| baseline | fs.promises.readFile(file) (not streaming) | 31 ms (8584 MB/s) | 353 MB |

### 2GB (2.07 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | new Response(stream).bytes() | 1.75 s (1185 MB/s) | 6.37 GB |
| **web-utility-streams** | **bytesFromStream** | **1.90 s (1090 MB/s)** | 4.32 GB |
| node:stream/consumers | bytes | 2.48 s (837 MB/s) | 8.53 GB |
| baseline | fs.promises.readFile(file) (not streaming) | 227 ms (9136 MB/s) | 2.16 GB |

## consume-json

Read a whole JSON file stream (as bytes) and parse it. The text must fit in a single string, so this only runs for sizes up to 512MB.

### 10MB (10.1 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| node:stream/consumers | json | 37 ms (270 MB/s) | 168 MB |
| **web-utility-streams** | **jsonFromStream** | **38 ms (268 MB/s)** | 168 MB |
| native | new Response(stream).json() | 43 ms (237 MB/s) | 164 MB |
| baseline | JSON.parse(await fs.promises.readFile(file, "utf8")) (not streaming) | 31 ms (328 MB/s) | 172 MB |

### 256MB (263 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| node:stream/consumers | json | 815 ms (322 MB/s) | 1.81 GB |
| native | new Response(stream).json() | 827 ms (318 MB/s) | 1.80 GB |
| **web-utility-streams** | **jsonFromStream** | **860 ms (305 MB/s)** | 1.81 GB |
| baseline | JSON.parse(await fs.promises.readFile(file, "utf8")) (not streaming) | 740 ms (355 MB/s) | 1.28 GB |

### 2GB (2.08 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **jsonFromStream** | **n/a (too large for memory)** |  |
| node:stream/consumers | json | n/a (too large for memory) |  |
| native | new Response(stream).json() | n/a (too large for memory) |  |
| baseline | JSON.parse(await fs.promises.readFile(file, "utf8")) (not streaming) | n/a (too large for memory) |  |

## consume-text

Read a whole file stream (as bytes) into a single string. The result must fit in a single string, so this only runs for sizes up to 512MB.

### 10MB (10.1 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **textFromStream** | **16 ms (630 MB/s)** | 145 MB |
| node:stream/consumers | text | 16 ms (615 MB/s) | 145 MB |
| **web-utility-streams** | **textFromStream after TextDecoderStream (string chunks)** | **18 ms (546 MB/s)** | 149 MB |
| native | new Response(stream).text() | 22 ms (461 MB/s) | 142 MB |
| baseline | fs.promises.readFile(file, "utf8") (not streaming) | 9 ms (1093 MB/s) | 134 MB |

### 256MB (262 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | new Response(stream).text() | 274 ms (954 MB/s) | 1.15 GB |
| **web-utility-streams** | **textFromStream** | **293 ms (895 MB/s)** | 1.24 GB |
| node:stream/consumers | text | 293 ms (894 MB/s) | 1.27 GB |
| **web-utility-streams** | **textFromStream after TextDecoderStream (string chunks)** | **304 ms (861 MB/s)** | 1.27 GB |
| baseline | fs.promises.readFile(file, "utf8") (not streaming) | 204 ms (1282 MB/s) | 1.15 GB |

### 2GB (2.07 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **textFromStream** | **n/a (too large for memory)** |  |
| **web-utility-streams** | **textFromStream after TextDecoderStream (string chunks)** | **n/a (too large for memory)** |  |
| node:stream/consumers | text | n/a (too large for memory) |  |
| native | new Response(stream).text() | n/a (too large for memory) |  |
| baseline | fs.promises.readFile(file, "utf8") (not streaming) | n/a (too large for memory) |  |

## debug

Call a function with each chunk of a generated stream of `{ id }` objects, passing the chunks through unchanged.

### 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **DebugStream** | **13 ms (2.6M chunks/s)** | 104 MB |
| native | TransformStream | 13 ms (2.6M chunks/s) | 98.3 MB |
| baseline | Read the source stream only | 3 ms (9.8M chunks/s) | 92.8 MB |

### 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **DebugStream** | **175 ms (5.0M chunks/s)** | 103 MB |
| native | TransformStream | 180 ms (4.8M chunks/s) | 103 MB |
| baseline | Read the source stream only | 44 ms (19.7M chunks/s) | 92.2 MB |

### 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | TransformStream | 1.33 s (5.1M chunks/s) | 111 MB |
| **web-utility-streams** | **DebugStream** | **1.34 s (5.1M chunks/s)** | 112 MB |
| baseline | Read the source stream only | 318 ms (21.3M chunks/s) | 97.3 MB |

## decode-json

Decode the elements of the "results" array in `{"total":N,"page":1,"results":[...],"next":null}`.

### 10MB (10.1 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **JsonDecoderStream** | **52 ms (195 MB/s)** | 101 MB |
| JSONStream | JSONStream.parse("results.*") | 128 ms (79 MB/s) | 99.8 MB |
| @streamparser/json | JSONParser (web streams) | 188 ms (54 MB/s) | 110 MB |
| stream-json | chain([parser, pick, streamArray]) (web streams) | 471 ms (21 MB/s) | 143 MB |
| bfj | bfj.match | 1.14 s (9 MB/s) | 359 MB |
| baseline | Read the file through TextDecoderStream only | 15 ms (674 MB/s) | 106 MB |
| baseline | JSON.parse of the whole file (not streaming) | 36 ms (279 MB/s) | 203 MB |

### 256MB (263 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **JsonDecoderStream** | **1.12 s (235 MB/s)** | 109 MB |
| JSONStream | JSONStream.parse("results.*") | 3.04 s (86 MB/s) | 158 MB |
| @streamparser/json | JSONParser (web streams) | 3.72 s (71 MB/s) | 115 MB |
| stream-json | chain([parser, pick, streamArray]) (web streams) | 10.82 s (24 MB/s) | 227 MB |
| bfj | bfj.match | 29.15 s (9 MB/s) | 1.34 GB |
| baseline | Read the file through TextDecoderStream only | 209 ms (1257 MB/s) | 107 MB |
| baseline | JSON.parse of the whole file (not streaming) | 895 ms (294 MB/s) | 1.40 GB |

### 2GB (2.08 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **JsonDecoderStream** | **8.82 s (236 MB/s)** | 156 MB |
| JSONStream | JSONStream.parse("results.*") | 23.42 s (89 MB/s) | 213 MB |
| @streamparser/json | JSONParser (web streams) | 29.07 s (71 MB/s) | 157 MB |
| stream-json | chain([parser, pick, streamArray]) (web streams) | 1.4 min (24 MB/s) | 222 MB |
| bfj | bfj.match | n/a (too large for memory) |  |
| baseline | Read the file through TextDecoderStream only | 1.54 s (1349 MB/s) | 115 MB |
| baseline | JSON.parse of the whole file (not streaming) | n/a (too large for memory) |  |

## decode-json-tokens

Split a JSON file in the shape of a paginated API response into tokens.

### 10MB (10.1 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **JsonTokenDecoderStream** | **167 ms (61 MB/s)** | 101 MB |
| stream-json | parser({ streamValues: false }) (web streams) | 220 ms (46 MB/s) | 122 MB |
| @streamparser/json | Tokenizer (web streams) | 248 ms (41 MB/s) | 103 MB |
| baseline | Read the file through TextDecoderStream only | 15 ms (673 MB/s) | 104 MB |

### 256MB (263 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **JsonTokenDecoderStream** | **4.15 s (63 MB/s)** | 121 MB |
| stream-json | parser({ streamValues: false }) (web streams) | 4.86 s (54 MB/s) | 164 MB |
| @streamparser/json | Tokenizer (web streams) | 5.94 s (44 MB/s) | 125 MB |
| baseline | Read the file through TextDecoderStream only | 207 ms (1272 MB/s) | 106 MB |

### 2GB (2.08 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **JsonTokenDecoderStream** | **31.83 s (65 MB/s)** | 209 MB |
| stream-json | parser({ streamValues: false }) (web streams) | 37.29 s (56 MB/s) | 221 MB |
| @streamparser/json | Tokenizer (web streams) | 45.60 s (46 MB/s) | 158 MB |
| baseline | Read the file through TextDecoderStream only | 1.51 s (1380 MB/s) | 113 MB |

## decode-ndjson

Decode a file with one JSON record per line.

### 10MB (10.1 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| ndjson | ndjson.parse() | 37 ms (272 MB/s) | 99.1 MB |
| split2 | split2(JSON.parse) | 38 ms (265 MB/s) | 99.6 MB |
| node:readline | readline + JSON.parse | 39 ms (256 MB/s) | 98.8 MB |
| **web-utility-streams** | **NewlineDelimitedJsonDecoderStream** | **41 ms (248 MB/s)** | 109 MB |
| @streamparser/json | JSONParser with separator "\n" (web streams) | 165 ms (61 MB/s) | 135 MB |
| baseline | Read the file through TextDecoderStream only | 15 ms (657 MB/s) | 106 MB |
| baseline | split + JSON.parse of the whole file (not streaming) | 48 ms (211 MB/s) | 189 MB |

### 256MB (262 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| ndjson | ndjson.parse() | 777 ms (337 MB/s) | 125 MB |
| split2 | split2(JSON.parse) | 797 ms (329 MB/s) | 127 MB |
| **web-utility-streams** | **NewlineDelimitedJsonDecoderStream** | **825 ms (317 MB/s)** | 117 MB |
| node:readline | readline + JSON.parse | 889 ms (295 MB/s) | 124 MB |
| @streamparser/json | JSONParser with separator "\n" (web streams) | 3.46 s (76 MB/s) | 247 MB |
| baseline | Read the file through TextDecoderStream only | 202 ms (1299 MB/s) | 107 MB |
| baseline | split + JSON.parse of the whole file (not streaming) | 880 ms (298 MB/s) | 1.34 GB |

### 2GB (2.07 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| ndjson | ndjson.parse() | 5.98 s (346 MB/s) | 225 MB |
| split2 | split2(JSON.parse) | 6.19 s (335 MB/s) | 233 MB |
| **web-utility-streams** | **NewlineDelimitedJsonDecoderStream** | **6.34 s (326 MB/s)** | 158 MB |
| node:readline | readline + JSON.parse | 6.92 s (299 MB/s) | 229 MB |
| @streamparser/json | JSONParser with separator "\n" (web streams) | 27.14 s (76 MB/s) | 249 MB |
| baseline | Read the file through TextDecoderStream only | 1.50 s (1382 MB/s) | 114 MB |
| baseline | split + JSON.parse of the whole file (not streaming) | n/a (too large for memory) |  |

## decode-sse

Decode a `text/event-stream` file with one event per record, each with an id, a type, and the record's JSON as its data (split over multiple lines for every 10th event).

### 10MB (11.6 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **EventSourceDecoderStream** | **30 ms (393 MB/s)** | 101 MB |
| eventsource-parser | EventSourceParserStream | 31 ms (379 MB/s) | 102 MB |
| @microsoft/fetch-event-source | getLines + getMessages (callbacks, decodes bytes) | 62 ms (189 MB/s) | 95.3 MB |
| baseline | Read the file through TextDecoderStream only | 16 ms (731 MB/s) | 99.4 MB |

### 256MB (303 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| eventsource-parser | EventSourceParserStream | 523 ms (579 MB/s) | 109 MB |
| **web-utility-streams** | **EventSourceDecoderStream** | **534 ms (567 MB/s)** | 109 MB |
| @microsoft/fetch-event-source | getLines + getMessages (callbacks, decodes bytes) | 1.44 s (210 MB/s) | 98.4 MB |
| baseline | Read the file through TextDecoderStream only | 232 ms (1304 MB/s) | 103 MB |

### 2GB (2.40 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| eventsource-parser | EventSourceParserStream | 3.98 s (603 MB/s) | 158 MB |
| **web-utility-streams** | **EventSourceDecoderStream** | **4.16 s (576 MB/s)** | 125 MB |
| @microsoft/fetch-event-source | getLines + getMessages (callbacks, decodes bytes) | 11.23 s (213 MB/s) | 107 MB |
| baseline | Read the file through TextDecoderStream only | 1.73 s (1386 MB/s) | 112 MB |

## deferred

Read a generated stream of `{ id }` objects that is only available from a promise.

### 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **DeferredStream** | **8 ms (4.0M chunks/s)** | 93.4 MB |
| native | ReadableStream.from(async generator) | 10 ms (3.3M chunks/s) | 93.7 MB |
| native | TransformStream, piping the stream into it once it resolves | 12 ms (2.8M chunks/s) | 97.6 MB |
| baseline | Read the source stream only | 3 ms (9.8M chunks/s) | 92.6 MB |

### 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **DeferredStream** | **122 ms (7.1M chunks/s)** | 97.3 MB |
| native | TransformStream, piping the stream into it once it resolves | 179 ms (4.8M chunks/s) | 102 MB |
| native | ReadableStream.from(async generator) | 182 ms (4.8M chunks/s) | 97.6 MB |
| baseline | Read the source stream only | 44 ms (19.7M chunks/s) | 92.5 MB |

### 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **DeferredStream** | **895 ms (7.6M chunks/s)** | 106 MB |
| native | TransformStream, piping the stream into it once it resolves | 1.26 s (5.4M chunks/s) | 112 MB |
| native | ReadableStream.from(async generator) | 1.42 s (4.8M chunks/s) | 107 MB |
| baseline | Read the source stream only | 338 ms (20.0M chunks/s) | 97.0 MB |

## empty

Create and read one empty stream per chunk (the empty stream's index is counted as its id).

### 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | new ReadableStream({ start: (c) => c.close() }) | 8 ms (4.2M chunks/s) | 94.6 MB |
| **web-utility-streams** | **EmptyStream** | **8 ms (4.1M chunks/s)** | 94.2 MB |
| native | ReadableStream.from([]) | 22 ms (1.6M chunks/s) | 146 MB |

### 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | new ReadableStream({ start: (c) => c.close() }) | 120 ms (7.2M chunks/s) | 94.2 MB |
| **web-utility-streams** | **EmptyStream** | **124 ms (7.0M chunks/s)** | 94.1 MB |
| native | ReadableStream.from([]) | 198 ms (4.4M chunks/s) | 137 MB |

### 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | new ReadableStream({ start: (c) => c.close() }) | 881 ms (7.7M chunks/s) | 98.3 MB |
| **web-utility-streams** | **EmptyStream** | **918 ms (7.4M chunks/s)** | 98.7 MB |
| native | ReadableStream.from([]) | 1.44 s (4.7M chunks/s) | 137 MB |

## encode-json

Encode generated records as the elements of a JSON array, discarding the output.

### 10MB (33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| JSONStream | JSONStream.stringify() | 38 ms (263 MB/s) | 98.8 MB |
| **web-utility-streams** | **JsonEncoderStream** | **50 ms (199 MB/s)** | 101 MB |
| **web-utility-streams** | **JsonEncoderStream with path ["results"] and extraData** | **50 ms (198 MB/s)** | 102 MB |
| stream-json | chain([disassembler, stringer]) (Node streams) | 612 ms (16 MB/s) | 110 MB |
| baseline | Generate the records only | 21 ms | 98.2 MB |
| baseline | JSON.stringify of the whole array (not streaming) | 40 ms (248 MB/s) | 194 MB |

### 256MB (867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| JSONStream | JSONStream.stringify() | 935 ms (279 MB/s) | 164 MB |
| **web-utility-streams** | **JsonEncoderStream** | **1.14 s (227 MB/s)** | 172 MB |
| **web-utility-streams** | **JsonEncoderStream with path ["results"] and extraData** | **1.14 s (227 MB/s)** | 172 MB |
| stream-json | chain([disassembler, stringer]) (Node streams) | 15.25 s (17 MB/s) | 255 MB |
| baseline | Generate the records only | 502 ms | 163 MB |
| baseline | JSON.stringify of the whole array (not streaming) | 1.06 s (246 MB/s) | 2.33 GB |

### 2GB (6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| JSONStream | JSONStream.stringify() | 7.14 s (289 MB/s) | 233 MB |
| **web-utility-streams** | **JsonEncoderStream** | **8.85 s (232 MB/s)** | 242 MB |
| **web-utility-streams** | **JsonEncoderStream with path ["results"] and extraData** | **8.92 s (230 MB/s)** | 242 MB |
| stream-json | chain([disassembler, stringer]) (Node streams) | 2.0 min (17 MB/s) | 305 MB |
| baseline | Generate the records only | 3.80 s | 232 MB |
| baseline | JSON.stringify of the whole array (not streaming) | n/a (too large for memory) |  |

## encode-ndjson

Encode generated records as newline delimited JSON, discarding the output.

### 10MB (33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **NewlineDelimitedJsonEncoderStream** | **51 ms (196 MB/s)** | 103 MB |
| stream-json | jsonl/stringer (Node streams) | 56 ms (178 MB/s) | 100 MB |
| ndjson | ndjson.stringify() | 83 ms (121 MB/s) | 126 MB |
| baseline | Generate the records only | 21 ms | 98.4 MB |
| baseline | JSON.stringify + .join('\n') | 41 ms (245 MB/s) | 99.0 MB |

### 256MB (867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **NewlineDelimitedJsonEncoderStream** | **1.14 s (227 MB/s)** | 172 MB |
| stream-json | jsonl/stringer (Node streams) | 1.31 s (198 MB/s) | 174 MB |
| ndjson | ndjson.stringify() | 1.69 s (154 MB/s) | 427 MB |
| baseline | Generate the records only | 496 ms | 164 MB |
| baseline | JSON.stringify + .join('\n') | 1.00 s (259 MB/s) | 167 MB |

### 2GB (6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **NewlineDelimitedJsonEncoderStream** | **8.80 s (233 MB/s)** | 241 MB |
| stream-json | jsonl/stringer (Node streams) | 10.34 s (198 MB/s) | 254 MB |
| ndjson | ndjson.stringify() | 13.15 s (156 MB/s) | 1.81 GB |
| baseline | Generate the records only | 3.81 s | 232 MB |
| baseline | JSON.stringify + .join('\n') | 7.72 s (265 MB/s) | 236 MB |

## encode-sse

Encode generated events, each with an id, a type, and a record's JSON as its data (multi-line for every 10th event), discarding the output.

### 10MB (33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **EventSourceEncoderStream** | **57 ms (203 MB/s)** | 100 MB |
| eventsource-encoder | encode() in a TransformStream | 62 ms (184 MB/s) | 100 MB |
| baseline | Generate the events only | 41 ms | 97.4 MB |

### 256MB (867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **EventSourceEncoderStream** | **1.30 s (230 MB/s)** | 172 MB |
| eventsource-encoder | encode() in a TransformStream | 1.43 s (210 MB/s) | 175 MB |
| baseline | Generate the events only | 1.01 s | 166 MB |

### 2GB (6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **EventSourceEncoderStream** | **9.92 s (240 MB/s)** | 243 MB |
| eventsource-encoder | encode() in a TransformStream | 11.12 s (214 MB/s) | 246 MB |
| baseline | Generate the events only | 8.07 s | 235 MB |

## filter

Filter a generated stream of `{ id }` objects with a noise chunk after each one, removing the noise.

### 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **FilterStream** | **15 ms (2.2M chunks/s)** | 97.9 MB |
| native | TransformStream | 15 ms (2.2M chunks/s) | 99.8 MB |
| node:stream | Readable.prototype.filter (Node streams) | 48 ms (0.7M chunks/s) | 96.3 MB |
| baseline | Read the source stream (with noise) only | 5 ms (6.5M chunks/s) | 92.8 MB |

### 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | TransformStream | 241 ms (3.6M chunks/s) | 101 MB |
| **web-utility-streams** | **FilterStream** | **243 ms (3.6M chunks/s)** | 102 MB |
| node:stream | Readable.prototype.filter (Node streams) | 953 ms (0.9M chunks/s) | 100 MB |
| baseline | Read the source stream (with noise) only | 78 ms (11.1M chunks/s) | 92.7 MB |

### 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | TransformStream | 1.82 s (3.7M chunks/s) | 111 MB |
| **web-utility-streams** | **FilterStream** | **1.89 s (3.6M chunks/s)** | 111 MB |
| node:stream | Readable.prototype.filter (Node streams) | 6.95 s (1.0M chunks/s) | 127 MB |
| baseline | Read the source stream (with noise) only | 616 ms (11.0M chunks/s) | 97.2 MB |

## filter-map

Filter and map a generated stream of `{ id }` objects with a noise chunk after each one, removing the noise and copying the other chunks.

### 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **FilterMapStream (sync function)** | **15 ms (2.2M chunks/s)** | 98.5 MB |
| native | TransformStream | 16 ms (2.1M chunks/s) | 98.2 MB |
| **web-utility-streams** | **FilterMapStream (async function)** | **18 ms (1.9M chunks/s)** | 98.6 MB |
| **web-utility-streams** | **FilterStream + MapStream** | **25 ms (1.4M chunks/s)** | 99.7 MB |
| node:stream | Readable.prototype.filter + map (Node streams) | 66 ms (0.5M chunks/s) | 96.2 MB |
| baseline | Read the source stream (with noise) only | 5 ms (6.6M chunks/s) | 93.0 MB |

### 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | TransformStream | 242 ms (3.6M chunks/s) | 100 MB |
| **web-utility-streams** | **FilterMapStream (sync function)** | **242 ms (3.6M chunks/s)** | 103 MB |
| **web-utility-streams** | **FilterMapStream (async function)** | **289 ms (3.0M chunks/s)** | 103 MB |
| **web-utility-streams** | **FilterStream + MapStream** | **434 ms (2.0M chunks/s)** | 104 MB |
| node:stream | Readable.prototype.filter + map (Node streams) | 1.35 s (0.6M chunks/s) | 109 MB |
| baseline | Read the source stream (with noise) only | 79 ms (11.0M chunks/s) | 92.8 MB |

### 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | TransformStream | 1.80 s (3.8M chunks/s) | 111 MB |
| **web-utility-streams** | **FilterMapStream (sync function)** | **1.85 s (3.7M chunks/s)** | 111 MB |
| **web-utility-streams** | **FilterMapStream (async function)** | **2.15 s (3.2M chunks/s)** | 111 MB |
| **web-utility-streams** | **FilterStream + MapStream** | **3.33 s (2.0M chunks/s)** | 113 MB |
| node:stream | Readable.prototype.filter + map (Node streams) | 10.20 s (0.7M chunks/s) | 127 MB |
| baseline | Read the source stream (with noise) only | 587 ms (11.5M chunks/s) | 97.0 MB |

## lazy

Read a generated stream of `{ id }` objects that is only created when it is first read.

### 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **LazyStream (async function)** | **8 ms (4.1M chunks/s)** | 93.3 MB |
| **web-utility-streams** | **LazyStream** | **8 ms (4.0M chunks/s)** | 95.1 MB |
| native | ReadableStream.from(async generator) | 10 ms (3.3M chunks/s) | 93.8 MB |
| baseline | Read the source stream only | 4 ms (9.6M chunks/s) | 92.8 MB |

### 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **LazyStream (async function)** | **121 ms (7.2M chunks/s)** | 97.6 MB |
| **web-utility-streams** | **LazyStream** | **123 ms (7.1M chunks/s)** | 97.0 MB |
| native | ReadableStream.from(async generator) | 183 ms (4.7M chunks/s) | 98.0 MB |
| baseline | Read the source stream only | 46 ms (19.0M chunks/s) | 92.7 MB |

### 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **LazyStream** | **876 ms (7.7M chunks/s)** | 106 MB |
| **web-utility-streams** | **LazyStream (async function)** | **902 ms (7.5M chunks/s)** | 106 MB |
| native | ReadableStream.from(async generator) | 1.37 s (4.9M chunks/s) | 106 MB |
| baseline | Read the source stream only | 328 ms (20.6M chunks/s) | 97.0 MB |

## map

Map each chunk of a generated stream of `{ id }` objects to a copy of it.

### 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **MapStream (sync function)** | **13 ms (2.7M chunks/s)** | 98.2 MB |
| native | TransformStream | 13 ms (2.7M chunks/s) | 103 MB |
| **web-utility-streams** | **MapStream (async function)** | **13 ms (2.5M chunks/s)** | 98.1 MB |
| node:stream | Readable.prototype.map (Node streams) | 36 ms (0.9M chunks/s) | 95.5 MB |
| baseline | Read the source stream only | 3 ms (9.7M chunks/s) | 92.9 MB |
| baseline | Read the Node.js Readable source only | 19 ms (1.7M chunks/s) | 113 MB |

### 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | TransformStream | 170 ms (5.1M chunks/s) | 103 MB |
| **web-utility-streams** | **MapStream (sync function)** | **171 ms (5.1M chunks/s)** | 103 MB |
| **web-utility-streams** | **MapStream (async function)** | **202 ms (4.3M chunks/s)** | 102 MB |
| node:stream | Readable.prototype.map (Node streams) | 628 ms (1.4M chunks/s) | 99.8 MB |
| baseline | Read the source stream only | 46 ms (18.9M chunks/s) | 92.5 MB |
| baseline | Read the Node.js Readable source only | 194 ms (4.5M chunks/s) | 339 MB |

### 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | TransformStream | 1.29 s (5.2M chunks/s) | 112 MB |
| **web-utility-streams** | **MapStream (sync function)** | **1.35 s (5.0M chunks/s)** | 111 MB |
| **web-utility-streams** | **MapStream (async function)** | **1.60 s (4.2M chunks/s)** | 111 MB |
| node:stream | Readable.prototype.map (Node streams) | 4.72 s (1.4M chunks/s) | 109 MB |
| baseline | Read the source stream only | 325 ms (20.8M chunks/s) | 96.2 MB |
| baseline | Read the Node.js Readable source only | 1.33 s (5.1M chunks/s) | 1.62 GB |

## split-lines

Split a file with one JSON record per line into lines, without parsing them.

### 10MB (10.1 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| node:readline | readline.createInterface | 20 ms (493 MB/s) | 97.5 MB |
| **web-utility-streams** | **SplitLinesStream** | **21 ms (478 MB/s)** | 96.3 MB |
| split2 | split2() | 22 ms (449 MB/s) | 98.2 MB |
| baseline | Read the file through TextDecoderStream only | 15 ms (670 MB/s) | 99.6 MB |
| baseline | split of the whole file (not streaming) | 25 ms (411 MB/s) | 160 MB |

### 256MB (262 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **SplitLinesStream** | **345 ms (758 MB/s)** | 107 MB |
| split2 | split2() | 418 ms (626 MB/s) | 127 MB |
| node:readline | readline.createInterface | 430 ms (609 MB/s) | 123 MB |
| baseline | Read the file through TextDecoderStream only | 204 ms (1282 MB/s) | 105 MB |
| baseline | split of the whole file (not streaming) | 568 ms (461 MB/s) | 1.27 GB |

### 2GB (2.07 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **SplitLinesStream** | **2.57 s (807 MB/s)** | 124 MB |
| split2 | split2() | 3.19 s (650 MB/s) | 237 MB |
| node:readline | readline.createInterface | 3.31 s (626 MB/s) | 165 MB |
| baseline | Read the file through TextDecoderStream only | 1.49 s (1393 MB/s) | 113 MB |
| baseline | split of the whole file (not streaming) | n/a (too large for memory) |  |

## to-generator

Iterate over a generated stream of `{ id }` objects with `for await`.

### 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | ReadableStream's built-in async iterator | 4 ms (8.5M chunks/s) | 92.5 MB |
| **web-utility-streams** | **readableStreamToGenerator** | **6 ms (6.2M chunks/s)** | 92.9 MB |
| baseline | reader.read() in a loop (no async iterator) | 4 ms (8.5M chunks/s) | 92.6 MB |

### 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | ReadableStream's built-in async iterator | 45 ms (19.2M chunks/s) | 92.7 MB |
| **web-utility-streams** | **readableStreamToGenerator** | **61 ms (14.1M chunks/s)** | 92.9 MB |
| baseline | reader.read() in a loop (no async iterator) | 42 ms (20.5M chunks/s) | 92.4 MB |

### 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | ReadableStream's built-in async iterator | 323 ms (21.0M chunks/s) | 96.9 MB |
| **web-utility-streams** | **readableStreamToGenerator** | **412 ms (16.4M chunks/s)** | 97.2 MB |
| baseline | reader.read() in a loop (no async iterator) | 296 ms (22.9M chunks/s) | 97.0 MB |

## transform-pipeline

Pipe a generated stream of `{ id }` objects through two `MapStream`s, combined into one transform stream.

### 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **TransformPipelineStream** | **21 ms (1.6M chunks/s)** | 99.1 MB |
| baseline | pipeThrough each MapStream directly | 21 ms (1.6M chunks/s) | 99.1 MB |

### 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **TransformPipelineStream** | **357 ms (2.4M chunks/s)** | 104 MB |
| baseline | pipeThrough each MapStream directly | 346 ms (2.5M chunks/s) | 103 MB |

### 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **TransformPipelineStream** | **2.67 s (2.5M chunks/s)** | 112 MB |
| baseline | pipeThrough each MapStream directly | 2.87 s (2.4M chunks/s) | 112 MB |

## union

Merge 10 generated streams of `{ id }` objects, read in parallel.

### 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **UnionStream** | **7 ms (5.0M chunks/s)** | 91.7 MB |
| **web-utility-streams** | **UnionStream with { highWaterMark: 0 }** | **7 ms (4.6M chunks/s)** | 92.3 MB |
| native | ReadableStream.from(async generator racing reads) | 30 ms (1.1M chunks/s) | 92.7 MB |
| baseline | Read each source stream in turn | 4 ms (7.6M chunks/s) | 92.0 MB |

### 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **UnionStream** | **82 ms (10.6M chunks/s)** | 96.0 MB |
| **web-utility-streams** | **UnionStream with { highWaterMark: 0 }** | **85 ms (10.2M chunks/s)** | 96.4 MB |
| native | ReadableStream.from(async generator racing reads) | 679 ms (1.3M chunks/s) | 98.0 MB |
| baseline | Read each source stream in turn | 53 ms (16.5M chunks/s) | 92.0 MB |

### 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **UnionStream with { highWaterMark: 0 }** | **581 ms (11.7M chunks/s)** | 122 MB |
| **web-utility-streams** | **UnionStream** | **584 ms (11.6M chunks/s)** | 122 MB |
| native | ReadableStream.from(async generator racing reads) | 5.05 s (1.3M chunks/s) | 107 MB |
| baseline | Read each source stream in turn | 368 ms (18.4M chunks/s) | 96.4 MB |
