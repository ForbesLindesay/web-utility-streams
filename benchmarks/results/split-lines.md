# split-lines

Split a file with one JSON record per line into lines, without parsing them.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (10.1 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| node:readline | readline.createInterface | 20 ms (493 MB/s) | 97.5 MB |
| **web-utility-streams** | **SplitLinesStream** | **21 ms (478 MB/s)** | 96.3 MB |
| split2 | split2() | 22 ms (449 MB/s) | 98.2 MB |
| baseline | Read the file through TextDecoderStream only | 15 ms (670 MB/s) | 99.6 MB |
| baseline | split of the whole file (not streaming) | 25 ms (411 MB/s) | 160 MB |

## 256MB (262 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **SplitLinesStream** | **345 ms (758 MB/s)** | 107 MB |
| split2 | split2() | 418 ms (626 MB/s) | 127 MB |
| node:readline | readline.createInterface | 430 ms (609 MB/s) | 123 MB |
| baseline | Read the file through TextDecoderStream only | 204 ms (1282 MB/s) | 105 MB |
| baseline | split of the whole file (not streaming) | 568 ms (461 MB/s) | 1.27 GB |

## 2GB (2.07 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **SplitLinesStream** | **2.57 s (807 MB/s)** | 124 MB |
| split2 | split2() | 3.19 s (650 MB/s) | 237 MB |
| node:readline | readline.createInterface | 3.31 s (626 MB/s) | 165 MB |
| baseline | Read the file through TextDecoderStream only | 1.49 s (1393 MB/s) | 113 MB |
| baseline | split of the whole file (not streaming) | n/a (too large for memory) |  |
