# encode-csv-objects

Encode generated records as CSV with a header row, using a function per column to get each field, comparing CsvObjectEncoderStream with piping TableEncoderStream into CsvEncoderStream.

- Node.js v24.22.3 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-23
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **CsvObjectEncoderStream** | **52 ms (108 MB/s)** | 104 MB |
| **web-utility-streams** | **MapStream(recordRow) → CsvEncoderStream (no header row)** | **59 ms (96 MB/s)** | 106 MB |
| **web-utility-streams** | **TableEncoderStream → CsvEncoderStream** | **60 ms (94 MB/s)** | 107 MB |
| fast-csv | format({ headers }) (Node streams) | 82 ms (69 MB/s) | 115 MB |
| baseline | Generate the records only | 21 ms | 100 MB |

## 256MB (867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **CsvObjectEncoderStream** | **1.16 s (128 MB/s)** | 197 MB |
| **web-utility-streams** | **MapStream(recordRow) → CsvEncoderStream (no header row)** | **1.27 s (117 MB/s)** | 198 MB |
| **web-utility-streams** | **TableEncoderStream → CsvEncoderStream** | **1.32 s (112 MB/s)** | 197 MB |
| fast-csv | format({ headers }) (Node streams) | 1.64 s (91 MB/s) | 200 MB |
| baseline | Generate the records only | 484 ms | 165 MB |

## 2GB (6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **CsvObjectEncoderStream** | **8.76 s (136 MB/s)** | 303 MB |
| **web-utility-streams** | **MapStream(recordRow) → CsvEncoderStream (no header row)** | **9.81 s (121 MB/s)** | 311 MB |
| **web-utility-streams** | **TableEncoderStream → CsvEncoderStream** | **10.15 s (117 MB/s)** | 302 MB |
| fast-csv | format({ headers }) (Node streams) | 13.10 s (91 MB/s) | 301 MB |
| baseline | Generate the records only | 3.78 s | 234 MB |
