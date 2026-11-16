# deferred

Read a generated stream of `{ id }` objects that is only available from a promise.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **DeferredStream** | **8 ms (4.0M chunks/s)** | 93.4 MB |
| native | ReadableStream.from(async generator) | 10 ms (3.3M chunks/s) | 93.7 MB |
| native | TransformStream, piping the stream into it once it resolves | 12 ms (2.8M chunks/s) | 97.6 MB |
| baseline | Read the source stream only | 3 ms (9.8M chunks/s) | 92.6 MB |

## 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **DeferredStream** | **122 ms (7.1M chunks/s)** | 97.3 MB |
| native | TransformStream, piping the stream into it once it resolves | 179 ms (4.8M chunks/s) | 102 MB |
| native | ReadableStream.from(async generator) | 182 ms (4.8M chunks/s) | 97.6 MB |
| baseline | Read the source stream only | 44 ms (19.7M chunks/s) | 92.5 MB |

## 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **DeferredStream** | **895 ms (7.6M chunks/s)** | 106 MB |
| native | TransformStream, piping the stream into it once it resolves | 1.26 s (5.4M chunks/s) | 112 MB |
| native | ReadableStream.from(async generator) | 1.42 s (4.8M chunks/s) | 107 MB |
| baseline | Read the source stream only | 338 ms (20.0M chunks/s) | 97.0 MB |
