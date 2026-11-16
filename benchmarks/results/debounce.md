# debounce

Debounce a generated stream of `{ id }` objects, with a period of 60s, so almost every chunk is dropped.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **DebounceStream (LEADING)** | **7 ms (5.1M chunks/s)** | 94.6 MB |
| **web-utility-streams** | **DebounceStream (BOTH)** | **8 ms (4.5M chunks/s)** | 95.3 MB |
| **web-utility-streams** | **DebounceStream (TRAILING)** | **8 ms (4.5M chunks/s)** | 96.1 MB |
| native | TransformStream with setTimeout (BOTH) | 8 ms (4.2M chunks/s) | 95.2 MB |
| baseline | Read the source stream only | 4 ms (8.3M chunks/s) | 92.8 MB |

## 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **DebounceStream (LEADING)** | **56 ms (15.6M chunks/s)** | 94.5 MB |
| **web-utility-streams** | **DebounceStream (BOTH)** | **57 ms (15.1M chunks/s)** | 95.0 MB |
| **web-utility-streams** | **DebounceStream (TRAILING)** | **58 ms (14.9M chunks/s)** | 95.2 MB |
| native | TransformStream with setTimeout (BOTH) | 74 ms (11.7M chunks/s) | 95.0 MB |
| baseline | Read the source stream only | 45 ms (19.4M chunks/s) | 92.6 MB |

## 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **DebounceStream (LEADING)** | **408 ms (16.6M chunks/s)** | 98.9 MB |
| **web-utility-streams** | **DebounceStream (BOTH)** | **408 ms (16.6M chunks/s)** | 99.5 MB |
| **web-utility-streams** | **DebounceStream (TRAILING)** | **408 ms (16.6M chunks/s)** | 99.4 MB |
| native | TransformStream with setTimeout (BOTH) | 535 ms (12.7M chunks/s) | 99.4 MB |
| baseline | Read the source stream only | 322 ms (21.1M chunks/s) | 97.0 MB |
