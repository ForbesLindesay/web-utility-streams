# decode-json

Decode the elements of the "results" array in `{"total":N,"page":1,"results":[...],"next":null}`.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (10.1 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **JsonDecoderStream** | **52 ms (195 MB/s)** | 101 MB |
| JSONStream | JSONStream.parse("results.*") | 128 ms (79 MB/s) | 99.8 MB |
| @streamparser/json | JSONParser (web streams) | 188 ms (54 MB/s) | 110 MB |
| stream-json | chain([parser, pick, streamArray]) (web streams) | 471 ms (21 MB/s) | 143 MB |
| bfj | bfj.match | 1.14 s (9 MB/s) | 359 MB |
| baseline | Read the file through TextDecoderStream only | 15 ms (674 MB/s) | 106 MB |
| baseline | JSON.parse of the whole file (not streaming) | 36 ms (279 MB/s) | 203 MB |

## 256MB (263 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **JsonDecoderStream** | **1.12 s (235 MB/s)** | 109 MB |
| JSONStream | JSONStream.parse("results.*") | 3.04 s (86 MB/s) | 158 MB |
| @streamparser/json | JSONParser (web streams) | 3.72 s (71 MB/s) | 115 MB |
| stream-json | chain([parser, pick, streamArray]) (web streams) | 10.82 s (24 MB/s) | 227 MB |
| bfj | bfj.match | 29.15 s (9 MB/s) | 1.34 GB |
| baseline | Read the file through TextDecoderStream only | 209 ms (1257 MB/s) | 107 MB |
| baseline | JSON.parse of the whole file (not streaming) | 895 ms (294 MB/s) | 1.40 GB |

## 2GB (2.08 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **JsonDecoderStream** | **8.82 s (236 MB/s)** | 156 MB |
| JSONStream | JSONStream.parse("results.*") | 23.42 s (89 MB/s) | 213 MB |
| @streamparser/json | JSONParser (web streams) | 29.07 s (71 MB/s) | 157 MB |
| stream-json | chain([parser, pick, streamArray]) (web streams) | 1.4 min (24 MB/s) | 222 MB |
| bfj | bfj.match | n/a (too large for memory) |  |
| baseline | Read the file through TextDecoderStream only | 1.54 s (1349 MB/s) | 115 MB |
| baseline | JSON.parse of the whole file (not streaming) | n/a (too large for memory) |  |
