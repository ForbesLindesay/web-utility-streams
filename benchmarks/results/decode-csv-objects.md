# decode-csv-objects

Decode a CSV file with a header row, and the same rows as decode-csv, into records, looking each field up by its column name, comparing CsvObjectDecoderStream with piping CsvDecoderStream into TableDecoderStream.

- Node.js v24.22.3 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-23
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (5.8 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **CsvObjectDecoderStream** | **36 ms (160 MB/s)** | 101 MB |
| **web-utility-streams** | **CsvDecoderStream → FilterMapStream (fields by position)** | **40 ms (145 MB/s)** | 107 MB |
| **web-utility-streams** | **CsvDecoderStream → TableDecoderStream** | **42 ms (138 MB/s)** | 109 MB |
| fast-csv | parse({ headers: true }) (Node streams) | 191 ms (30 MB/s) | 152 MB |
| baseline | Read the file through TextDecoderStream only | 10 ms (550 MB/s) | 94.0 MB |

## 256MB (151 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **CsvObjectDecoderStream** | **612 ms (247 MB/s)** | 109 MB |
| **web-utility-streams** | **CsvDecoderStream → FilterMapStream (fields by position)** | **668 ms (227 MB/s)** | 114 MB |
| **web-utility-streams** | **CsvDecoderStream → TableDecoderStream** | **736 ms (206 MB/s)** | 114 MB |
| fast-csv | parse({ headers: true }) (Node streams) | 4.33 s (35 MB/s) | 268 MB |
| baseline | Read the file through TextDecoderStream only | 136 ms (1115 MB/s) | 104 MB |

## 2GB (1.21 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **CsvObjectDecoderStream** | **4.80 s (252 MB/s)** | 152 MB |
| **web-utility-streams** | **CsvDecoderStream → FilterMapStream (fields by position)** | **5.09 s (237 MB/s)** | 154 MB |
| **web-utility-streams** | **CsvDecoderStream → TableDecoderStream** | **5.52 s (219 MB/s)** | 153 MB |
| fast-csv | parse({ headers: true }) (Node streams) | 33.83 s (36 MB/s) | 276 MB |
| baseline | Read the file through TextDecoderStream only | 982 ms (1230 MB/s) | 106 MB |
