# map

Map each chunk of a generated stream of `{ id }` objects to a copy of it.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **MapStream (sync function)** | **13 ms (2.7M chunks/s)** | 98.2 MB |
| native | TransformStream | 13 ms (2.7M chunks/s) | 103 MB |
| **web-utility-streams** | **MapStream (async function)** | **13 ms (2.5M chunks/s)** | 98.1 MB |
| node:stream | Readable.prototype.map (Node streams) | 36 ms (0.9M chunks/s) | 95.5 MB |
| baseline | Read the source stream only | 3 ms (9.7M chunks/s) | 92.9 MB |
| baseline | Read the Node.js Readable source only | 19 ms (1.7M chunks/s) | 113 MB |

## 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | TransformStream | 170 ms (5.1M chunks/s) | 103 MB |
| **web-utility-streams** | **MapStream (sync function)** | **171 ms (5.1M chunks/s)** | 103 MB |
| **web-utility-streams** | **MapStream (async function)** | **202 ms (4.3M chunks/s)** | 102 MB |
| node:stream | Readable.prototype.map (Node streams) | 628 ms (1.4M chunks/s) | 99.8 MB |
| baseline | Read the source stream only | 46 ms (18.9M chunks/s) | 92.5 MB |
| baseline | Read the Node.js Readable source only | 194 ms (4.5M chunks/s) | 339 MB |

## 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | TransformStream | 1.29 s (5.2M chunks/s) | 112 MB |
| **web-utility-streams** | **MapStream (sync function)** | **1.35 s (5.0M chunks/s)** | 111 MB |
| **web-utility-streams** | **MapStream (async function)** | **1.60 s (4.2M chunks/s)** | 111 MB |
| node:stream | Readable.prototype.map (Node streams) | 4.72 s (1.4M chunks/s) | 109 MB |
| baseline | Read the source stream only | 325 ms (20.8M chunks/s) | 96.2 MB |
| baseline | Read the Node.js Readable source only | 1.33 s (5.1M chunks/s) | 1.62 GB |
