# encode-ndjson

Encode generated records as newline delimited JSON, discarding the output.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **NewlineDelimitedJsonEncoderStream** | **51 ms (196 MB/s)** | 103 MB |
| stream-json | jsonl/stringer (Node streams) | 56 ms (178 MB/s) | 100 MB |
| ndjson | ndjson.stringify() | 83 ms (121 MB/s) | 126 MB |
| baseline | Generate the records only | 21 ms | 98.4 MB |
| baseline | JSON.stringify + .join('\n') | 41 ms (245 MB/s) | 99.0 MB |

## 256MB (867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **NewlineDelimitedJsonEncoderStream** | **1.14 s (227 MB/s)** | 172 MB |
| stream-json | jsonl/stringer (Node streams) | 1.31 s (198 MB/s) | 174 MB |
| ndjson | ndjson.stringify() | 1.69 s (154 MB/s) | 427 MB |
| baseline | Generate the records only | 496 ms | 164 MB |
| baseline | JSON.stringify + .join('\n') | 1.00 s (259 MB/s) | 167 MB |

## 2GB (6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **NewlineDelimitedJsonEncoderStream** | **8.80 s (233 MB/s)** | 241 MB |
| stream-json | jsonl/stringer (Node streams) | 10.34 s (198 MB/s) | 254 MB |
| ndjson | ndjson.stringify() | 13.15 s (156 MB/s) | 1.81 GB |
| baseline | Generate the records only | 3.81 s | 232 MB |
| baseline | JSON.stringify + .join('\n') | 7.72 s (265 MB/s) | 236 MB |
