# add-prefix

Add a prefix to a generated stream of `{ id }` objects. The short prefix is the first 10 chunks, and the long prefix is the first half of the chunks.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **AddPrefixStream (short array prefix)** | **10 ms (3.4M chunks/s)** | 96.3 MB |
| **web-utility-streams** | **AddPrefixStream (short prefix from a promise)** | **10 ms (3.3M chunks/s)** | 96.4 MB |
| native | ReadableStream.from(async generator), with the short prefix | 11 ms (3.0M chunks/s) | 94.0 MB |
| **web-utility-streams** | **AddPrefixStream (long async generator prefix)** | **12 ms (2.8M chunks/s)** | 94.9 MB |
| native | TransformStream, enqueuing the short prefix in start | 13 ms (2.5M chunks/s) | 98.8 MB |
| **web-utility-streams** | **AddPrefixStream (long array prefix)** | **14 ms (2.4M chunks/s)** | 103 MB |
| native | ReadableStream.from(async generator), with the long async generator prefix | 15 ms (2.3M chunks/s) | 98.2 MB |
| baseline | Read the source stream only | 4 ms (8.2M chunks/s) | 92.7 MB |

## 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **AddPrefixStream (short array prefix)** | **105 ms (8.3M chunks/s)** | 97.4 MB |
| **web-utility-streams** | **AddPrefixStream (short prefix from a promise)** | **108 ms (8.0M chunks/s)** | 96.2 MB |
| **web-utility-streams** | **AddPrefixStream (long array prefix)** | **131 ms (6.6M chunks/s)** | 147 MB |
| **web-utility-streams** | **AddPrefixStream (long async generator prefix)** | **159 ms (5.5M chunks/s)** | 99.1 MB |
| native | TransformStream, enqueuing the short prefix in start | 180 ms (4.8M chunks/s) | 99.9 MB |
| native | ReadableStream.from(async generator), with the short prefix | 183 ms (4.7M chunks/s) | 98.3 MB |
| native | ReadableStream.from(async generator), with the long async generator prefix | 198 ms (4.4M chunks/s) | 97.0 MB |
| baseline | Read the source stream only | 46 ms (18.9M chunks/s) | 92.3 MB |

## 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **AddPrefixStream (short array prefix)** | **751 ms (9.0M chunks/s)** | 101 MB |
| **web-utility-streams** | **AddPrefixStream (short prefix from a promise)** | **777 ms (8.7M chunks/s)** | 99.0 MB |
| **web-utility-streams** | **AddPrefixStream (long array prefix)** | **902 ms (7.5M chunks/s)** | 415 MB |
| **web-utility-streams** | **AddPrefixStream (long async generator prefix)** | **1.16 s (5.8M chunks/s)** | 99.1 MB |
| native | TransformStream, enqueuing the short prefix in start | 1.36 s (5.0M chunks/s) | 110 MB |
| native | ReadableStream.from(async generator), with the short prefix | 1.39 s (4.9M chunks/s) | 107 MB |
| native | ReadableStream.from(async generator), with the long async generator prefix | 1.49 s (4.5M chunks/s) | 106 MB |
| baseline | Read the source stream only | 328 ms (20.7M chunks/s) | 96.9 MB |
