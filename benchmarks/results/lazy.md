# lazy

Read a generated stream of `{ id }` objects that is only created when it is first read.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **LazyStream (async function)** | **8 ms (4.1M chunks/s)** | 93.3 MB |
| **web-utility-streams** | **LazyStream** | **8 ms (4.0M chunks/s)** | 95.1 MB |
| native | ReadableStream.from(async generator) | 10 ms (3.3M chunks/s) | 93.8 MB |
| baseline | Read the source stream only | 4 ms (9.6M chunks/s) | 92.8 MB |

## 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **LazyStream (async function)** | **121 ms (7.2M chunks/s)** | 97.6 MB |
| **web-utility-streams** | **LazyStream** | **123 ms (7.1M chunks/s)** | 97.0 MB |
| native | ReadableStream.from(async generator) | 183 ms (4.7M chunks/s) | 98.0 MB |
| baseline | Read the source stream only | 46 ms (19.0M chunks/s) | 92.7 MB |

## 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **LazyStream** | **876 ms (7.7M chunks/s)** | 106 MB |
| **web-utility-streams** | **LazyStream (async function)** | **902 ms (7.5M chunks/s)** | 106 MB |
| native | ReadableStream.from(async generator) | 1.37 s (4.9M chunks/s) | 106 MB |
| baseline | Read the source stream only | 328 ms (20.6M chunks/s) | 97.0 MB |
