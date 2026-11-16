# concat

Join 10 generated streams of `{ id }` objects, one after another.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **ConcatStream with { highWaterMark: 0 }** | **9 ms (3.9M chunks/s)** | 93.1 MB |
| **web-utility-streams** | **ConcatStream** | **9 ms (3.7M chunks/s)** | 94.5 MB |
| native | ReadableStream.from(async generator) | 12 ms (2.8M chunks/s) | 94.1 MB |
| native | TransformStream, piping each source into it in turn | 14 ms (2.4M chunks/s) | 97.9 MB |
| baseline | Read each source stream in turn | 5 ms (7.4M chunks/s) | 92.3 MB |

## 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **ConcatStream with { highWaterMark: 0 }** | **126 ms (6.9M chunks/s)** | 97.8 MB |
| **web-utility-streams** | **ConcatStream** | **132 ms (6.6M chunks/s)** | 97.6 MB |
| native | TransformStream, piping each source into it in turn | 182 ms (4.8M chunks/s) | 102 MB |
| native | ReadableStream.from(async generator) | 204 ms (4.3M chunks/s) | 97.2 MB |
| baseline | Read each source stream in turn | 50 ms (17.2M chunks/s) | 92.0 MB |

## 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **ConcatStream** | **951 ms (7.1M chunks/s)** | 106 MB |
| **web-utility-streams** | **ConcatStream with { highWaterMark: 0 }** | **956 ms (7.1M chunks/s)** | 106 MB |
| native | TransformStream, piping each source into it in turn | 1.34 s (5.1M chunks/s) | 111 MB |
| native | ReadableStream.from(async generator) | 1.47 s (4.6M chunks/s) | 106 MB |
| baseline | Read each source stream in turn | 374 ms (18.1M chunks/s) | 96.5 MB |
