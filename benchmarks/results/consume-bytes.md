# consume-bytes

Read a whole file stream into a single `Uint8Array`.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (10.1 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **bytesFromStream** | **12 ms (848 MB/s)** | 115 MB |
| node:stream/consumers | bytes | 14 ms (699 MB/s) | 135 MB |
| native | new Response(stream).bytes() | 21 ms (490 MB/s) | 132 MB |
| baseline | fs.promises.readFile(file) (not streaming) | 2 ms (4377 MB/s) | 102 MB |

## 256MB (262 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **bytesFromStream** | **222 ms (1180 MB/s)** | 627 MB |
| native | new Response(stream).bytes() | 251 ms (1043 MB/s) | 897 MB |
| node:stream/consumers | bytes | 352 ms (744 MB/s) | 1.16 GB |
| baseline | fs.promises.readFile(file) (not streaming) | 31 ms (8584 MB/s) | 353 MB |

## 2GB (2.07 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | new Response(stream).bytes() | 1.75 s (1185 MB/s) | 6.37 GB |
| **web-utility-streams** | **bytesFromStream** | **1.90 s (1090 MB/s)** | 4.32 GB |
| node:stream/consumers | bytes | 2.48 s (837 MB/s) | 8.53 GB |
| baseline | fs.promises.readFile(file) (not streaming) | 227 ms (9136 MB/s) | 2.16 GB |
