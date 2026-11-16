# to-generator

Iterate over a generated stream of `{ id }` objects with `for await`.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | ReadableStream's built-in async iterator | 4 ms (8.5M chunks/s) | 92.5 MB |
| **web-utility-streams** | **readableStreamToGenerator** | **6 ms (6.2M chunks/s)** | 92.9 MB |
| baseline | reader.read() in a loop (no async iterator) | 4 ms (8.5M chunks/s) | 92.6 MB |

## 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | ReadableStream's built-in async iterator | 45 ms (19.2M chunks/s) | 92.7 MB |
| **web-utility-streams** | **readableStreamToGenerator** | **61 ms (14.1M chunks/s)** | 92.9 MB |
| baseline | reader.read() in a loop (no async iterator) | 42 ms (20.5M chunks/s) | 92.4 MB |

## 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | ReadableStream's built-in async iterator | 323 ms (21.0M chunks/s) | 96.9 MB |
| **web-utility-streams** | **readableStreamToGenerator** | **412 ms (16.4M chunks/s)** | 97.2 MB |
| baseline | reader.read() in a loop (no async iterator) | 296 ms (22.9M chunks/s) | 97.0 MB |
