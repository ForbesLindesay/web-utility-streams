# decode-json-tokens

Split a JSON file in the shape of a paginated API response into tokens.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (10.1 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **JsonTokenDecoderStream** | **167 ms (61 MB/s)** | 101 MB |
| stream-json | parser({ streamValues: false }) (web streams) | 220 ms (46 MB/s) | 122 MB |
| @streamparser/json | Tokenizer (web streams) | 248 ms (41 MB/s) | 103 MB |
| baseline | Read the file through TextDecoderStream only | 15 ms (673 MB/s) | 104 MB |

## 256MB (263 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **JsonTokenDecoderStream** | **4.15 s (63 MB/s)** | 121 MB |
| stream-json | parser({ streamValues: false }) (web streams) | 4.86 s (54 MB/s) | 164 MB |
| @streamparser/json | Tokenizer (web streams) | 5.94 s (44 MB/s) | 125 MB |
| baseline | Read the file through TextDecoderStream only | 207 ms (1272 MB/s) | 106 MB |

## 2GB (2.08 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **JsonTokenDecoderStream** | **31.83 s (65 MB/s)** | 209 MB |
| stream-json | parser({ streamValues: false }) (web streams) | 37.29 s (56 MB/s) | 221 MB |
| @streamparser/json | Tokenizer (web streams) | 45.60 s (46 MB/s) | 158 MB |
| baseline | Read the file through TextDecoderStream only | 1.51 s (1380 MB/s) | 113 MB |
