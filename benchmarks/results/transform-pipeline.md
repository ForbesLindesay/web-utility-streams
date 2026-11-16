# transform-pipeline

Pipe a generated stream of `{ id }` objects through two `MapStream`s, combined into one transform stream.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (33,879 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **TransformPipelineStream** | **21 ms (1.6M chunks/s)** | 99.1 MB |
| baseline | pipeThrough each MapStream directly | 21 ms (1.6M chunks/s) | 99.1 MB |

## 256MB (867,325 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **TransformPipelineStream** | **357 ms (2.4M chunks/s)** | 104 MB |
| baseline | pipeThrough each MapStream directly | 346 ms (2.5M chunks/s) | 103 MB |

## 2GB (6,775,981 chunks)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **TransformPipelineStream** | **2.67 s (2.5M chunks/s)** | 112 MB |
| baseline | pipeThrough each MapStream directly | 2.87 s (2.4M chunks/s) | 112 MB |
