# decode-csv

Decode a CSV file with one row of 12 fields per record (no header row), where one field is quoted and contains escaped quotes and a line break.

- Node.js v24.22.3 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-23
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (5.8 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **CsvDecoderStream** | **28 ms (208 MB/s)** | 101 MB |
| papaparse | Papa.parse(NODE_STREAM_INPUT) | 52 ms (110 MB/s) | 118 MB |
| csv-parse | parse() (Node streams) | 100 ms (57 MB/s) | 101 MB |
| csv-parse | csv-parse/stream (web streams) | 106 ms (54 MB/s) | 104 MB |
| fast-csv | parse() (Node streams) | 190 ms (30 MB/s) | 138 MB |
| baseline | Read the file through TextDecoderStream only | 12 ms (500 MB/s) | 98.2 MB |

## 256MB (151 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **CsvDecoderStream** | **455 ms (333 MB/s)** | 109 MB |
| papaparse | Papa.parse(NODE_STREAM_INPUT) | 910 ms (166 MB/s) | 159 MB |
| csv-parse | csv-parse/stream (web streams) | 2.14 s (71 MB/s) | 105 MB |
| csv-parse | parse() (Node streams) | 2.25 s (67 MB/s) | 103 MB |
| fast-csv | parse() (Node streams) | 4.21 s (36 MB/s) | 252 MB |
| baseline | Read the file through TextDecoderStream only | 135 ms (1124 MB/s) | 104 MB |

## 2GB (1.21 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **CsvDecoderStream** | **3.49 s (346 MB/s)** | 150 MB |
| papaparse | Papa.parse(NODE_STREAM_INPUT) | 6.97 s (173 MB/s) | 220 MB |
| csv-parse | csv-parse/stream (web streams) | 16.56 s (73 MB/s) | 107 MB |
| csv-parse | parse() (Node streams) | 17.32 s (70 MB/s) | 104 MB |
| fast-csv | parse() (Node streams) | 32.96 s (37 MB/s) | 290 MB |
| baseline | Read the file through TextDecoderStream only | 1.02 s (1188 MB/s) | 106 MB |
