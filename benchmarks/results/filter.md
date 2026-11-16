# filter

Filter a generated stream of `{ id }` objects with a noise chunk after each one, removing the noise.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **FilterStream** | **15 ms (2.2M chunks/s)** | 97.9 MB |
| native | TransformStream | 15 ms (2.2M chunks/s) | 99.8 MB |
| node:stream | Readable.prototype.filter (Node streams) | 48 ms (0.7M chunks/s) | 96.3 MB |
| baseline | Read the source stream (with noise) only | 5 ms (6.5M chunks/s) | 92.8 MB |

## 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | TransformStream | 241 ms (3.6M chunks/s) | 101 MB |
| **web-utility-streams** | **FilterStream** | **243 ms (3.6M chunks/s)** | 102 MB |
| node:stream | Readable.prototype.filter (Node streams) | 953 ms (0.9M chunks/s) | 100 MB |
| baseline | Read the source stream (with noise) only | 78 ms (11.1M chunks/s) | 92.7 MB |

## 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | TransformStream | 1.82 s (3.7M chunks/s) | 111 MB |
| **web-utility-streams** | **FilterStream** | **1.89 s (3.6M chunks/s)** | 111 MB |
| node:stream | Readable.prototype.filter (Node streams) | 6.95 s (1.0M chunks/s) | 127 MB |
| baseline | Read the source stream (with noise) only | 616 ms (11.0M chunks/s) | 97.2 MB |
