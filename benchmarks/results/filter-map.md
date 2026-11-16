# filter-map

Filter and map a generated stream of `{ id }` objects with a noise chunk after each one, removing the noise and copying the other chunks.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **FilterMapStream (sync function)** | **15 ms (2.2M chunks/s)** | 98.5 MB |
| native | TransformStream | 16 ms (2.1M chunks/s) | 98.2 MB |
| **web-utility-streams** | **FilterMapStream (async function)** | **18 ms (1.9M chunks/s)** | 98.6 MB |
| **web-utility-streams** | **FilterStream + MapStream** | **25 ms (1.4M chunks/s)** | 99.7 MB |
| node:stream | Readable.prototype.filter + map (Node streams) | 66 ms (0.5M chunks/s) | 96.2 MB |
| baseline | Read the source stream (with noise) only | 5 ms (6.6M chunks/s) | 93.0 MB |

## 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | TransformStream | 242 ms (3.6M chunks/s) | 100 MB |
| **web-utility-streams** | **FilterMapStream (sync function)** | **242 ms (3.6M chunks/s)** | 103 MB |
| **web-utility-streams** | **FilterMapStream (async function)** | **289 ms (3.0M chunks/s)** | 103 MB |
| **web-utility-streams** | **FilterStream + MapStream** | **434 ms (2.0M chunks/s)** | 104 MB |
| node:stream | Readable.prototype.filter + map (Node streams) | 1.35 s (0.6M chunks/s) | 109 MB |
| baseline | Read the source stream (with noise) only | 79 ms (11.0M chunks/s) | 92.8 MB |

## 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | TransformStream | 1.80 s (3.8M chunks/s) | 111 MB |
| **web-utility-streams** | **FilterMapStream (sync function)** | **1.85 s (3.7M chunks/s)** | 111 MB |
| **web-utility-streams** | **FilterMapStream (async function)** | **2.15 s (3.2M chunks/s)** | 111 MB |
| **web-utility-streams** | **FilterStream + MapStream** | **3.33 s (2.0M chunks/s)** | 113 MB |
| node:stream | Readable.prototype.filter + map (Node streams) | 10.20 s (0.7M chunks/s) | 127 MB |
| baseline | Read the source stream (with noise) only | 587 ms (11.5M chunks/s) | 97.0 MB |
