# decode-csv-objects

Decode the decode-csv file (with a header row added) into records, looking each field up by its column name, comparing CsvObjectDecoderStream with piping CsvDecoderStream into TableDecoderStream.

- Node.js v24.22.3 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-23
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (5.8 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **CsvObjectDecoderStream** | **35 ms (165 MB/s)** | 100 MB |
| **web-utility-streams** | **CsvDecoderStream → FilterMapStream (fields by position)** | **37 ms (157 MB/s)** | 105 MB |
| **web-utility-streams** | **CsvDecoderStream → TableDecoderStream** | **40 ms (144 MB/s)** | 107 MB |
| fast-csv | parse({ headers: true }) (Node streams) | 201 ms (29 MB/s) | 236 MB |
| baseline | Read the file through TextDecoderStream only | 11 ms (546 MB/s) | 94.0 MB |

## 256MB (151 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **CsvObjectDecoderStream** | **657 ms (230 MB/s)** | 108 MB |
| **web-utility-streams** | **CsvDecoderStream → FilterMapStream (fields by position)** | **664 ms (228 MB/s)** | 113 MB |
| **web-utility-streams** | **CsvDecoderStream → TableDecoderStream** | **733 ms (206 MB/s)** | 113 MB |
| fast-csv | parse({ headers: true }) (Node streams) | 4.79 s (32 MB/s) | 262 MB |
| baseline | Read the file through TextDecoderStream only | 137 ms (1103 MB/s) | 104 MB |
