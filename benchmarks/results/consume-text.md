# consume-text

Read a whole file stream (as bytes) into a single string. The result must fit in a single string, so this only runs for sizes up to 512MB.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (10.1 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **textFromStream** | **16 ms (630 MB/s)** | 145 MB |
| node:stream/consumers | text | 16 ms (615 MB/s) | 145 MB |
| **web-utility-streams** | **textFromStream after TextDecoderStream (string chunks)** | **18 ms (546 MB/s)** | 149 MB |
| native | new Response(stream).text() | 22 ms (461 MB/s) | 142 MB |
| baseline | fs.promises.readFile(file, "utf8") (not streaming) | 9 ms (1093 MB/s) | 134 MB |

## 256MB (262 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| native | new Response(stream).text() | 274 ms (954 MB/s) | 1.15 GB |
| **web-utility-streams** | **textFromStream** | **293 ms (895 MB/s)** | 1.24 GB |
| node:stream/consumers | text | 293 ms (894 MB/s) | 1.27 GB |
| **web-utility-streams** | **textFromStream after TextDecoderStream (string chunks)** | **304 ms (861 MB/s)** | 1.27 GB |
| baseline | fs.promises.readFile(file, "utf8") (not streaming) | 204 ms (1282 MB/s) | 1.15 GB |

## 2GB (2.07 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **textFromStream** | **n/a (too large for memory)** |  |
| **web-utility-streams** | **textFromStream after TextDecoderStream (string chunks)** | **n/a (too large for memory)** |  |
| node:stream/consumers | text | n/a (too large for memory) |  |
| native | new Response(stream).text() | n/a (too large for memory) |  |
| baseline | fs.promises.readFile(file, "utf8") (not streaming) | n/a (too large for memory) |  |
