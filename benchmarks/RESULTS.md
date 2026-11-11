# Benchmark results

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-11
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## decode-json

Decode the elements of the "results" array in `{"total":N,"page":1,"results":[...],"next":null}`.

### 10MB (10.1 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **JsonDecoderStream** | **52 ms (193 MB/s)** | 77.6 MB |
| JSONStream | JSONStream.parse("results.*") | 122 ms (83 MB/s) | 75.8 MB |
| @streamparser/json | JSONParser (web streams) | 160 ms (63 MB/s) | 83.3 MB |
| stream-json | chain([parser, pick, streamArray]) (web streams) | 451 ms (22 MB/s) | 120 MB |
| bfj | bfj.match | 1.09 s (9 MB/s) | 281 MB |
| baseline | Read the file through TextDecoderStream only | 14 ms (712 MB/s) | 81.2 MB |
| baseline | JSON.parse of the whole file (not streaming) | 34 ms (301 MB/s) | 175 MB |

### 256MB (263 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **JsonDecoderStream** | **1.10 s (239 MB/s)** | 105 MB |
| JSONStream | JSONStream.parse("results.*") | 3.27 s (80 MB/s) | 134 MB |
| @streamparser/json | JSONParser (web streams) | 3.73 s (71 MB/s) | 91.0 MB |
| stream-json | chain([parser, pick, streamArray]) (web streams) | 11.08 s (24 MB/s) | 203 MB |
| bfj | bfj.match | 28.55 s (9 MB/s) | 1.42 GB |
| baseline | Read the file through TextDecoderStream only | 204 ms (1285 MB/s) | 83.8 MB |
| baseline | JSON.parse of the whole file (not streaming) | 1.08 s (244 MB/s) | 1.64 GB |

### 2GB (2.08 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **JsonDecoderStream** | **8.82 s (236 MB/s)** | 132 MB |
| JSONStream | JSONStream.parse("results.*") | 23.65 s (88 MB/s) | 193 MB |
| @streamparser/json | JSONParser (web streams) | 29.04 s (72 MB/s) | 132 MB |
| stream-json | chain([parser, pick, streamArray]) (web streams) | 1.4 min (24 MB/s) | 201 MB |
| bfj | bfj.match | n/a (too large for memory) |  |
| baseline | Read the file through TextDecoderStream only | 1.48 s (1400 MB/s) | 90.5 MB |
| baseline | JSON.parse of the whole file (not streaming) | n/a (too large for memory) |  |

## decode-ndjson

Decode a file with one JSON record per line.

### 10MB (10.1 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| ndjson | ndjson.parse() | 38 ms (268 MB/s) | 74.7 MB |
| split2 | split2(JSON.parse) | 38 ms (264 MB/s) | 82.3 MB |
| node:readline | readline + JSON.parse | 39 ms (256 MB/s) | 73.3 MB |
| **web-utility-streams** | **NewlineDelimitedJsonDecoderStream** | **40 ms (253 MB/s)** | 76.3 MB |
| @streamparser/json | JSONParser with separator "\n" (web streams) | 158 ms (64 MB/s) | 110 MB |
| baseline | Read the file through TextDecoderStream only | 14 ms (717 MB/s) | 81.2 MB |
| baseline | split + JSON.parse of the whole file (not streaming) | 40 ms (252 MB/s) | 151 MB |

### 256MB (262 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| split2 | split2(JSON.parse) | 814 ms (322 MB/s) | 104 MB |
| ndjson | ndjson.parse() | 819 ms (320 MB/s) | 104 MB |
| **web-utility-streams** | **NewlineDelimitedJsonDecoderStream** | **847 ms (309 MB/s)** | 83.7 MB |
| node:readline | readline + JSON.parse | 908 ms (288 MB/s) | 100 MB |
| @streamparser/json | JSONParser with separator "\n" (web streams) | 3.42 s (77 MB/s) | 216 MB |
| baseline | Read the file through TextDecoderStream only | 202 ms (1296 MB/s) | 83.6 MB |
| baseline | split + JSON.parse of the whole file (not streaming) | 941 ms (278 MB/s) | 1.23 GB |

### 2GB (2.07 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| ndjson | ndjson.parse() | 6.36 s (326 MB/s) | 207 MB |
| split2 | split2(JSON.parse) | 6.51 s (318 MB/s) | 223 MB |
| **web-utility-streams** | **NewlineDelimitedJsonDecoderStream** | **6.53 s (317 MB/s)** | 132 MB |
| node:readline | readline + JSON.parse | 7.05 s (294 MB/s) | 206 MB |
| @streamparser/json | JSONParser with separator "\n" (web streams) | 26.88 s (77 MB/s) | 215 MB |
| baseline | Read the file through TextDecoderStream only | 1.47 s (1405 MB/s) | 90.8 MB |
| baseline | split + JSON.parse of the whole file (not streaming) | n/a (too large for memory) |  |

## encode-json

Encode generated records as the elements of a JSON array, discarding the output.

### 10MB (33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| JSONStream | JSONStream.stringify() | 38 ms (267 MB/s) | 74.0 MB |
| **web-utility-streams** | **JsonEncoderStream** | **46 ms (218 MB/s)** | 77.3 MB |
| **web-utility-streams** | **JsonEncoderStream with path ["results"] and extraData** | **46 ms (218 MB/s)** | 77.2 MB |
| stream-json | chain([disassembler, stringer]) (Node streams) | 605 ms (16 MB/s) | 86.8 MB |
| baseline | Generate the records only | 17 ms | 73.1 MB |
| baseline | JSON.stringify of the whole array (not streaming) | 40 ms (248 MB/s) | 168 MB |

### 256MB (867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| JSONStream | JSONStream.stringify() | 945 ms (276 MB/s) | 140 MB |
| **web-utility-streams** | **JsonEncoderStream** | **1.06 s (244 MB/s)** | 141 MB |
| **web-utility-streams** | **JsonEncoderStream with path ["results"] and extraData** | **1.12 s (232 MB/s)** | 140 MB |
| stream-json | chain([disassembler, stringer]) (Node streams) | 15.07 s (17 MB/s) | 246 MB |
| baseline | Generate the records only | 423 ms | 103 MB |
| baseline | JSON.stringify of the whole array (not streaming) | 978 ms (265 MB/s) | 2.36 GB |

### 2GB (6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| JSONStream | JSONStream.stringify() | 7.20 s (287 MB/s) | 205 MB |
| **web-utility-streams** | **JsonEncoderStream** | **8.23 s (249 MB/s)** | 209 MB |
| **web-utility-streams** | **JsonEncoderStream with path ["results"] and extraData** | **8.32 s (246 MB/s)** | 209 MB |
| stream-json | chain([disassembler, stringer]) (Node streams) | 1.9 min (18 MB/s) | 298 MB |
| baseline | Generate the records only | 3.31 s | 205 MB |
| baseline | JSON.stringify of the whole array (not streaming) | n/a (too large for memory) |  |

## encode-ndjson

Encode generated records as newline delimited JSON, discarding the output.

### 10MB (33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **NewlineDelimitedJsonEncoderStream** | **45 ms (219 MB/s)** | 77.1 MB |
| stream-json | jsonl/stringer (Node streams) | 55 ms (183 MB/s) | 76.8 MB |
| ndjson | ndjson.stringify() | 82 ms (121 MB/s) | 111 MB |
| baseline | Generate the records only | 17 ms | 73.1 MB |
| baseline | JSON.stringify + .join('\n') | 37 ms (269 MB/s) | 72.7 MB |

### 256MB (867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **NewlineDelimitedJsonEncoderStream** | **1.06 s (244 MB/s)** | 140 MB |
| stream-json | jsonl/stringer (Node streams) | 1.31 s (198 MB/s) | 146 MB |
| ndjson | ndjson.stringify() | 1.66 s (156 MB/s) | 405 MB |
| baseline | Generate the records only | 425 ms | 102 MB |
| baseline | JSON.stringify + .join('\n') | 925 ms (280 MB/s) | 135 MB |

### 2GB (6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **NewlineDelimitedJsonEncoderStream** | **8.35 s (246 MB/s)** | 209 MB |
| stream-json | jsonl/stringer (Node streams) | 10.21 s (201 MB/s) | 222 MB |
| ndjson | ndjson.stringify() | 12.81 s (160 MB/s) | 1.73 GB |
| baseline | Generate the records only | 3.28 s | 205 MB |
| baseline | JSON.stringify + .join('\n') | 7.13 s (288 MB/s) | 203 MB |

## split-lines

Split a file with one JSON record per line into lines, without parsing them.

### 10MB (10.1 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| node:readline | readline.createInterface | 19 ms (527 MB/s) | 72.5 MB |
| **web-utility-streams** | **SplitLinesStream** | **20 ms (505 MB/s)** | 77.3 MB |
| split2 | split2() | 22 ms (449 MB/s) | 74.9 MB |
| baseline | Read the file through TextDecoderStream only | 14 ms (716 MB/s) | 81.2 MB |
| baseline | split of the whole file (not streaming) | 24 ms (418 MB/s) | 137 MB |

### 256MB (262 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **SplitLinesStream** | **339 ms (773 MB/s)** | 86.7 MB |
| split2 | split2() | 420 ms (624 MB/s) | 105 MB |
| node:readline | readline.createInterface | 429 ms (611 MB/s) | 102 MB |
| baseline | Read the file through TextDecoderStream only | 205 ms (1280 MB/s) | 85.1 MB |
| baseline | split of the whole file (not streaming) | 539 ms (485 MB/s) | 1.25 GB |

### 2GB (2.07 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **SplitLinesStream** | **2.50 s (829 MB/s)** | 103 MB |
| split2 | split2() | 3.19 s (648 MB/s) | 218 MB |
| node:readline | readline.createInterface | 3.30 s (628 MB/s) | 145 MB |
| baseline | Read the file through TextDecoderStream only | 1.48 s (1396 MB/s) | 90.7 MB |
| baseline | split of the whole file (not streaming) | n/a (too large for memory) |  |

## decode-sse

Decode a `text/event-stream` file with one event per record, each with an id, a type, and the record's JSON as its data (split over multiple lines for every 10th event).

### 10MB (11.6 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| eventsource-parser | EventSourceParserStream | 28 ms (420 MB/s) | 79.6 MB |
| **web-utility-streams** | **EventSourceDecoderStream** | **28 ms (409 MB/s)** | 80.8 MB |
| @microsoft/fetch-event-source | getLines + getMessages (callbacks, decodes bytes) | 59 ms (197 MB/s) | 76.3 MB |
| baseline | Read the file through TextDecoderStream only | 15 ms (765 MB/s) | 81.2 MB |

### 256MB (303 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| eventsource-parser | EventSourceParserStream | 513 ms (590 MB/s) | 105 MB |
| **web-utility-streams** | **EventSourceDecoderStream** | **528 ms (574 MB/s)** | 88.5 MB |
| @microsoft/fetch-event-source | getLines + getMessages (callbacks, decodes bytes) | 1.44 s (211 MB/s) | 76.7 MB |
| baseline | Read the file through TextDecoderStream only | 231 ms (1312 MB/s) | 91.7 MB |

### 2GB (2.40 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| eventsource-parser | EventSourceParserStream | 3.99 s (600 MB/s) | 138 MB |
| **web-utility-streams** | **EventSourceDecoderStream** | **4.09 s (586 MB/s)** | 137 MB |
| @microsoft/fetch-event-source | getLines + getMessages (callbacks, decodes bytes) | 11.24 s (213 MB/s) | 83.9 MB |
| baseline | Read the file through TextDecoderStream only | 1.72 s (1396 MB/s) | 90.5 MB |

## encode-sse

Encode generated events, each with an id, a type, and a record's JSON as its data (multi-line for every 10th event), discarding the output.

### 10MB (33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **EventSourceEncoderStream** | **52 ms (224 MB/s)** | 77.9 MB |
| eventsource-encoder | encode() in a TransformStream | 57 ms (203 MB/s) | 77.8 MB |
| baseline | Generate the events only | 37 ms | 73.2 MB |

### 256MB (867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **EventSourceEncoderStream** | **1.21 s (248 MB/s)** | 142 MB |
| eventsource-encoder | encode() in a TransformStream | 1.34 s (224 MB/s) | 145 MB |
| baseline | Generate the events only | 944 ms | 135 MB |

### 2GB (6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **EventSourceEncoderStream** | **9.38 s (253 MB/s)** | 213 MB |
| eventsource-encoder | encode() in a TransformStream | 10.40 s (228 MB/s) | 215 MB |
| baseline | Generate the events only | 7.25 s | 204 MB |
