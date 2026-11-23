# encode-csv

Encode generated records as CSV rows of 12 fields, discarding the output. Converting the records to arrays of strings is included in every case.

- Node.js v24.22.3 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-23
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **CsvEncoderStream** | **51 ms (111 MB/s)** | 105 MB |
| fast-csv | format() (Node streams) | 78 ms (73 MB/s) | 113 MB |
| csv-stringify | stringify() (Node streams) | 95 ms (59 MB/s) | 104 MB |
| baseline | Generate the rows only | 27 ms | 99.4 MB |

## 256MB (867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **CsvEncoderStream** | **1.13 s (132 MB/s)** | 194 MB |
| fast-csv | format() (Node streams) | 1.67 s (89 MB/s) | 200 MB |
| csv-stringify | stringify() (Node streams) | 2.16 s (69 MB/s) | 246 MB |
| baseline | Generate the rows only | 603 ms | 185 MB |

## 2GB (6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **CsvEncoderStream** | **8.69 s (137 MB/s)** | 302 MB |
| fast-csv | format() (Node streams) | 12.94 s (92 MB/s) | 307 MB |
| csv-stringify | stringify() (Node streams) | 16.75 s (71 MB/s) | 302 MB |
| baseline | Generate the rows only | 4.70 s | 295 MB |
