# transform-with-cancel

Pass a generated stream of `{ id }` objects through a transform that enqueues each chunk unchanged, with `flush` and `cancel` methods. Node.js supports `cancel`, so `TransformStreamWithCancel` is the native `TransformStream`; the polyfill is what's used in runtimes that don't.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-16
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **TransformStreamWithCancelPolyfill (sync transform)** | **12 ms (2.9M chunks/s)** | 97.9 MB |
| **web-utility-streams** | **TransformStreamWithCancelPolyfill (async transform)** | **13 ms (2.5M chunks/s)** | 97.6 MB |
| native | TransformStream (async transform) | 15 ms (2.3M chunks/s) | 98.5 MB |
| native | TransformStream (sync transform) | 15 ms (2.2M chunks/s) | 104 MB |
| **web-utility-streams** | **TransformStreamWithCancel (sync transform)** | **16 ms (2.2M chunks/s)** | 104 MB |
| baseline | Read the source stream only | 4 ms (9.4M chunks/s) | 92.5 MB |

## 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **TransformStreamWithCancelPolyfill (sync transform)** | **155 ms (5.6M chunks/s)** | 102 MB |
| **web-utility-streams** | **TransformStreamWithCancel (sync transform)** | **179 ms (4.8M chunks/s)** | 103 MB |
| **web-utility-streams** | **TransformStreamWithCancelPolyfill (async transform)** | **183 ms (4.7M chunks/s)** | 102 MB |
| native | TransformStream (sync transform) | 187 ms (4.6M chunks/s) | 104 MB |
| native | TransformStream (async transform) | 194 ms (4.5M chunks/s) | 103 MB |
| baseline | Read the source stream only | 46 ms (19.0M chunks/s) | 92.0 MB |

## 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **TransformStreamWithCancelPolyfill (sync transform)** | **1.19 s (5.7M chunks/s)** | 111 MB |
| native | TransformStream (sync transform) | 1.33 s (5.1M chunks/s) | 114 MB |
| **web-utility-streams** | **TransformStreamWithCancel (sync transform)** | **1.33 s (5.1M chunks/s)** | 112 MB |
| **web-utility-streams** | **TransformStreamWithCancelPolyfill (async transform)** | **1.36 s (5.0M chunks/s)** | 111 MB |
| native | TransformStream (async transform) | 1.41 s (4.8M chunks/s) | 111 MB |
| baseline | Read the source stream only | 333 ms (20.4M chunks/s) | 96.7 MB |
