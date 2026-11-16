# consume-json

Read a whole JSON file stream (as bytes) and parse it. The text must fit in a single string, so this only runs for sizes up to 512MB.

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (10.1 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| node:stream/consumers | json | 37 ms (270 MB/s) | 168 MB |
| **web-utility-streams** | **jsonFromStream** | **38 ms (268 MB/s)** | 168 MB |
| native | new Response(stream).json() | 43 ms (237 MB/s) | 164 MB |
| baseline | JSON.parse(await fs.promises.readFile(file, "utf8")) (not streaming) | 31 ms (328 MB/s) | 172 MB |

## 256MB (263 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| node:stream/consumers | json | 815 ms (322 MB/s) | 1.81 GB |
| native | new Response(stream).json() | 827 ms (318 MB/s) | 1.80 GB |
| **web-utility-streams** | **jsonFromStream** | **860 ms (305 MB/s)** | 1.81 GB |
| baseline | JSON.parse(await fs.promises.readFile(file, "utf8")) (not streaming) | 740 ms (355 MB/s) | 1.28 GB |

## 2GB (2.08 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **jsonFromStream** | **n/a (too large for memory)** |  |
| node:stream/consumers | json | n/a (too large for memory) |  |
| native | new Response(stream).json() | n/a (too large for memory) |  |
| baseline | JSON.parse(await fs.promises.readFile(file, "utf8")) (not streaming) | n/a (too large for memory) |  |
