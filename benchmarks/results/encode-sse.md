# encode-sse

Encode generated events, each with an id, a type, and a record's JSON as its data (multi-line for every 10th event), discarding the output.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **EventSourceEncoderStream** | **57 ms (203 MB/s)** | 100 MB |
| eventsource-encoder | encode() in a TransformStream | 62 ms (184 MB/s) | 100 MB |
| baseline | Generate the events only | 41 ms | 97.4 MB |

## 256MB (867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **EventSourceEncoderStream** | **1.30 s (230 MB/s)** | 172 MB |
| eventsource-encoder | encode() in a TransformStream | 1.43 s (210 MB/s) | 175 MB |
| baseline | Generate the events only | 1.01 s | 166 MB |

## 2GB (6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **EventSourceEncoderStream** | **9.92 s (240 MB/s)** | 243 MB |
| eventsource-encoder | encode() in a TransformStream | 11.12 s (214 MB/s) | 246 MB |
| baseline | Generate the events only | 8.07 s | 235 MB |
