# empty

Create and read one empty stream per chunk (the empty stream's index is counted as its id).

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | new ReadableStream({ start: (c) => c.close() }) | 8 ms (4.2M chunks/s) | 94.6 MB |
| **web-utility-streams** | **EmptyStream** | **8 ms (4.1M chunks/s)** | 94.2 MB |
| native | ReadableStream.from([]) | 22 ms (1.6M chunks/s) | 146 MB |

## 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | new ReadableStream({ start: (c) => c.close() }) | 120 ms (7.2M chunks/s) | 94.2 MB |
| **web-utility-streams** | **EmptyStream** | **124 ms (7.0M chunks/s)** | 94.1 MB |
| native | ReadableStream.from([]) | 198 ms (4.4M chunks/s) | 137 MB |

## 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | new ReadableStream({ start: (c) => c.close() }) | 881 ms (7.7M chunks/s) | 98.3 MB |
| **web-utility-streams** | **EmptyStream** | **918 ms (7.4M chunks/s)** | 98.7 MB |
| native | ReadableStream.from([]) | 1.44 s (4.7M chunks/s) | 137 MB |
