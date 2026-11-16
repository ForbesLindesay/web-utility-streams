# debug

Call a function with each chunk of a generated stream of `{ id }` objects, passing the chunks through unchanged.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **DebugStream** | **13 ms (2.6M chunks/s)** | 104 MB |
| native | TransformStream | 13 ms (2.6M chunks/s) | 98.3 MB |
| baseline | Read the source stream only | 3 ms (9.8M chunks/s) | 92.8 MB |

## 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **DebugStream** | **175 ms (5.0M chunks/s)** | 103 MB |
| native | TransformStream | 180 ms (4.8M chunks/s) | 103 MB |
| baseline | Read the source stream only | 44 ms (19.7M chunks/s) | 92.2 MB |

## 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | TransformStream | 1.33 s (5.1M chunks/s) | 111 MB |
| **web-utility-streams** | **DebugStream** | **1.34 s (5.1M chunks/s)** | 112 MB |
| baseline | Read the source stream only | 318 ms (21.3M chunks/s) | 97.3 MB |
