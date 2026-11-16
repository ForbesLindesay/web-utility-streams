# union

Merge 10 generated streams of `{ id }` objects, read in parallel.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **UnionStream** | **7 ms (5.0M chunks/s)** | 91.7 MB |
| **web-utility-streams** | **UnionStream with { highWaterMark: 0 }** | **7 ms (4.6M chunks/s)** | 92.3 MB |
| native | ReadableStream.from(async generator racing reads) | 30 ms (1.1M chunks/s) | 92.7 MB |
| baseline | Read each source stream in turn | 4 ms (7.6M chunks/s) | 92.0 MB |

## 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **UnionStream** | **82 ms (10.6M chunks/s)** | 96.0 MB |
| **web-utility-streams** | **UnionStream with { highWaterMark: 0 }** | **85 ms (10.2M chunks/s)** | 96.4 MB |
| native | ReadableStream.from(async generator racing reads) | 679 ms (1.3M chunks/s) | 98.0 MB |
| baseline | Read each source stream in turn | 53 ms (16.5M chunks/s) | 92.0 MB |

## 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **UnionStream with { highWaterMark: 0 }** | **581 ms (11.7M chunks/s)** | 122 MB |
| **web-utility-streams** | **UnionStream** | **584 ms (11.6M chunks/s)** | 122 MB |
| native | ReadableStream.from(async generator racing reads) | 5.05 s (1.3M chunks/s) | 107 MB |
| baseline | Read each source stream in turn | 368 ms (18.4M chunks/s) | 96.4 MB |
