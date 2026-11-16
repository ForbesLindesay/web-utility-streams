# timeout

Error a generated stream of `{ id }` objects if no chunk is received for 60s, which never happens, so every chunk is passed through unchanged.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-16
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **TimeoutStream (fixed timeout)** | **13 ms (2.6M chunks/s)** | 103 MB |
| **web-utility-streams** | **TimeoutStream (timeout from a function)** | **13 ms (2.5M chunks/s)** | 103 MB |
| native | TransformStream, restarting a setTimeout for each chunk | 19 ms (1.8M chunks/s) | 98.5 MB |
| baseline | Read the source stream only | 3 ms (9.8M chunks/s) | 92.5 MB |

## 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **TimeoutStream (fixed timeout)** | **164 ms (5.3M chunks/s)** | 103 MB |
| **web-utility-streams** | **TimeoutStream (timeout from a function)** | **166 ms (5.2M chunks/s)** | 103 MB |
| native | TransformStream, restarting a setTimeout for each chunk | 305 ms (2.8M chunks/s) | 102 MB |
| baseline | Read the source stream only | 44 ms (19.7M chunks/s) | 92.4 MB |

## 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **TimeoutStream (fixed timeout)** | **1.21 s (5.6M chunks/s)** | 112 MB |
| **web-utility-streams** | **TimeoutStream (timeout from a function)** | **1.27 s (5.3M chunks/s)** | 112 MB |
| native | TransformStream, restarting a setTimeout for each chunk | 2.34 s (2.9M chunks/s) | 111 MB |
| baseline | Read the source stream only | 319 ms (21.2M chunks/s) | 96.2 MB |
