# encode-csv

Encode generated records as CSV rows of 12 fields, discarding the output. Converting the records to arrays of strings is included in every case.

- Node.js v24.22.3 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-23
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **CsvEncoderStream** | **50 ms (114 MB/s)** | 106 MB |
| fast-csv | format() (Node streams) | 72 ms (79 MB/s) | 113 MB |
| csv-stringify | stringify() (Node streams) | 92 ms (61 MB/s) | 113 MB |
| baseline | Generate the rows only | 26 ms | 99.2 MB |

## 256MB (867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **CsvEncoderStream** | **1.09 s (137 MB/s)** | 194 MB |
| fast-csv | format() (Node streams) | 1.63 s (91 MB/s) | 201 MB |
| csv-stringify | stringify() (Node streams) | 2.09 s (71 MB/s) | 249 MB |
| baseline | Generate the rows only | 587 ms | 184 MB |

## 2GB (6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **CsvEncoderStream** | **8.37 s (142 MB/s)** | 307 MB |
| fast-csv | format() (Node streams) | 12.62 s (94 MB/s) | 300 MB |
| csv-stringify | stringify() (Node streams) | 16.69 s (71 MB/s) | 303 MB |
| baseline | Generate the rows only | 4.57 s | 295 MB |
