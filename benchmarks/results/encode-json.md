# encode-json

Encode generated records as the elements of a JSON array, discarding the output.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| JSONStream | JSONStream.stringify() | 38 ms (263 MB/s) | 98.8 MB |
| **web-utility-streams** | **JsonEncoderStream** | **50 ms (199 MB/s)** | 101 MB |
| **web-utility-streams** | **JsonEncoderStream with path ["results"] and extraData** | **50 ms (198 MB/s)** | 102 MB |
| stream-json | chain([disassembler, stringer]) (Node streams) | 612 ms (16 MB/s) | 110 MB |
| baseline | Generate the records only | 21 ms | 98.2 MB |
| baseline | JSON.stringify of the whole array (not streaming) | 40 ms (248 MB/s) | 194 MB |

## 256MB (867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| JSONStream | JSONStream.stringify() | 935 ms (279 MB/s) | 164 MB |
| **web-utility-streams** | **JsonEncoderStream** | **1.14 s (227 MB/s)** | 172 MB |
| **web-utility-streams** | **JsonEncoderStream with path ["results"] and extraData** | **1.14 s (227 MB/s)** | 172 MB |
| stream-json | chain([disassembler, stringer]) (Node streams) | 15.25 s (17 MB/s) | 255 MB |
| baseline | Generate the records only | 502 ms | 163 MB |
| baseline | JSON.stringify of the whole array (not streaming) | 1.06 s (246 MB/s) | 2.33 GB |

## 2GB (6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| JSONStream | JSONStream.stringify() | 7.14 s (289 MB/s) | 233 MB |
| **web-utility-streams** | **JsonEncoderStream** | **8.85 s (232 MB/s)** | 242 MB |
| **web-utility-streams** | **JsonEncoderStream with path ["results"] and extraData** | **8.92 s (230 MB/s)** | 242 MB |
| stream-json | chain([disassembler, stringer]) (Node streams) | 2.0 min (17 MB/s) | 305 MB |
| baseline | Generate the records only | 3.80 s | 232 MB |
| baseline | JSON.stringify of the whole array (not streaming) | n/a (too large for memory) |  |
