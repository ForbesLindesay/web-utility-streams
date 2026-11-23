# decode-csv

Decode a CSV file with one row of 12 fields per record (no header row), where one field is quoted and contains escaped quotes and a line break.

- Node.js v24.22.3 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-23
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (5.8 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **CsvDecoderStream** | **26 ms (223 MB/s)** | 99.8 MB |
| papaparse | Papa.parse(NODE_STREAM_INPUT) | 50 ms (116 MB/s) | 118 MB |
| csv-parse | parse() (Node streams) | 99 ms (58 MB/s) | 102 MB |
| csv-parse | csv-parse/stream (web streams) | 104 ms (56 MB/s) | 103 MB |
| fast-csv | parse() (Node streams) | 181 ms (32 MB/s) | 137 MB |
| baseline | Read the file through TextDecoderStream only | 11 ms (535 MB/s) | 98.1 MB |

## 256MB (151 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **CsvDecoderStream** | **460 ms (329 MB/s)** | 108 MB |
| papaparse | Papa.parse(NODE_STREAM_INPUT) | 898 ms (169 MB/s) | 161 MB |
| csv-parse | parse() (Node streams) | 2.13 s (71 MB/s) | 102 MB |
| csv-parse | csv-parse/stream (web streams) | 2.15 s (70 MB/s) | 102 MB |
| fast-csv | parse() (Node streams) | 4.28 s (35 MB/s) | 257 MB |
| baseline | Read the file through TextDecoderStream only | 132 ms (1143 MB/s) | 104 MB |

## 2GB (1.21 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **CsvDecoderStream** | **3.53 s (343 MB/s)** | 149 MB |
| papaparse | Papa.parse(NODE_STREAM_INPUT) | 6.78 s (178 MB/s) | 221 MB |
| csv-parse | csv-parse/stream (web streams) | 16.40 s (74 MB/s) | 107 MB |
| csv-parse | parse() (Node streams) | 17.30 s (70 MB/s) | 104 MB |
| fast-csv | parse() (Node streams) | 32.67 s (37 MB/s) | 272 MB |
| baseline | Read the file through TextDecoderStream only | 1.01 s (1199 MB/s) | 106 MB |
