# encode-csv-objects

Encode generated records as CSV with a header row, using a function per column to get each field, comparing CsvObjectEncoderStream with piping TableEncoderStream into CsvEncoderStream.

- Node.js v24.22.3 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-23
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **CsvObjectEncoderStream** | **52 ms (109 MB/s)** | 105 MB |
| **web-utility-streams** | **MapStream(recordRow) → CsvEncoderStream (no header row)** | **58 ms (98 MB/s)** | 106 MB |
| **web-utility-streams** | **TableEncoderStream → CsvEncoderStream** | **60 ms (94 MB/s)** | 106 MB |
| fast-csv | format({ headers }) (Node streams) | 76 ms (75 MB/s) | 116 MB |
| baseline | Generate the records only | 21 ms | 100 MB |

## 256MB (867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **CsvObjectEncoderStream** | **1.14 s (130 MB/s)** | 197 MB |
| **web-utility-streams** | **MapStream(recordRow) → CsvEncoderStream (no header row)** | **1.28 s (116 MB/s)** | 201 MB |
| **web-utility-streams** | **TableEncoderStream → CsvEncoderStream** | **1.34 s (111 MB/s)** | 201 MB |
| fast-csv | format({ headers }) (Node streams) | 1.72 s (87 MB/s) | 205 MB |
| baseline | Generate the records only | 495 ms | 166 MB |
