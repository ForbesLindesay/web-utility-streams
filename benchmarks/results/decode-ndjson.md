# decode-ndjson

Decode a file with one JSON record per line.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (10.1 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| ndjson | ndjson.parse() | 37 ms (272 MB/s) | 99.1 MB |
| split2 | split2(JSON.parse) | 38 ms (265 MB/s) | 99.6 MB |
| node:readline | readline + JSON.parse | 39 ms (256 MB/s) | 98.8 MB |
| **web-utility-streams** | **NewlineDelimitedJsonDecoderStream** | **41 ms (248 MB/s)** | 109 MB |
| @streamparser/json | JSONParser with separator "\n" (web streams) | 165 ms (61 MB/s) | 135 MB |
| baseline | Read the file through TextDecoderStream only | 15 ms (657 MB/s) | 106 MB |
| baseline | split + JSON.parse of the whole file (not streaming) | 48 ms (211 MB/s) | 189 MB |

## 256MB (262 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| ndjson | ndjson.parse() | 777 ms (337 MB/s) | 125 MB |
| split2 | split2(JSON.parse) | 797 ms (329 MB/s) | 127 MB |
| **web-utility-streams** | **NewlineDelimitedJsonDecoderStream** | **825 ms (317 MB/s)** | 117 MB |
| node:readline | readline + JSON.parse | 889 ms (295 MB/s) | 124 MB |
| @streamparser/json | JSONParser with separator "\n" (web streams) | 3.46 s (76 MB/s) | 247 MB |
| baseline | Read the file through TextDecoderStream only | 202 ms (1299 MB/s) | 107 MB |
| baseline | split + JSON.parse of the whole file (not streaming) | 880 ms (298 MB/s) | 1.34 GB |

## 2GB (2.07 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| ndjson | ndjson.parse() | 5.98 s (346 MB/s) | 225 MB |
| split2 | split2(JSON.parse) | 6.19 s (335 MB/s) | 233 MB |
| **web-utility-streams** | **NewlineDelimitedJsonDecoderStream** | **6.34 s (326 MB/s)** | 158 MB |
| node:readline | readline + JSON.parse | 6.92 s (299 MB/s) | 229 MB |
| @streamparser/json | JSONParser with separator "\n" (web streams) | 27.14 s (76 MB/s) | 249 MB |
| baseline | Read the file through TextDecoderStream only | 1.50 s (1382 MB/s) | 114 MB |
| baseline | split + JSON.parse of the whole file (not streaming) | n/a (too large for memory) |  |
