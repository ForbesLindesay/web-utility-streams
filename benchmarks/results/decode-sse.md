# decode-sse

Decode a `text/event-stream` file with one event per record, each with an id, a type, and the record's JSON as its data (split over multiple lines for every 10th event).

- Node.js v24.22.2 on Darwin arm64, Apple M5 Pro, 64 GB RAM
- Date: 2026-11-12
- Times are the median of the runs, measured in a fresh process for each run. Throughput is input bytes (for decoders) or output characters (for encoders) per second. Peak memory is the peak resident set size of the process. Decoders that use web streams read the input with `FileHandle.readableWebStream()` (16KiB chunks), and decoders that use Node.js streams read it with `fs.createReadStream()` (64KiB chunks).

## 10MB (11.6 MB, 33,879 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| **web-utility-streams** | **EventSourceDecoderStream** | **30 ms (393 MB/s)** | 101 MB |
| eventsource-parser | EventSourceParserStream | 31 ms (379 MB/s) | 102 MB |
| @microsoft/fetch-event-source | getLines + getMessages (callbacks, decodes bytes) | 62 ms (189 MB/s) | 95.3 MB |
| baseline | Read the file through TextDecoderStream only | 16 ms (731 MB/s) | 99.4 MB |

## 256MB (303 MB, 867,325 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| eventsource-parser | EventSourceParserStream | 523 ms (579 MB/s) | 109 MB |
| **web-utility-streams** | **EventSourceDecoderStream** | **534 ms (567 MB/s)** | 109 MB |
| @microsoft/fetch-event-source | getLines + getMessages (callbacks, decodes bytes) | 1.44 s (210 MB/s) | 98.4 MB |
| baseline | Read the file through TextDecoderStream only | 232 ms (1304 MB/s) | 103 MB |

## 2GB (2.40 GB, 6,775,981 records)

| Library | Approach | Time | Peak memory |
| --- | --- | --- | --- |
| eventsource-parser | EventSourceParserStream | 3.98 s (603 MB/s) | 158 MB |
| **web-utility-streams** | **EventSourceDecoderStream** | **4.16 s (576 MB/s)** | 125 MB |
| @microsoft/fetch-event-source | getLines + getMessages (callbacks, decodes bytes) | 11.23 s (213 MB/s) | 107 MB |
| baseline | Read the file through TextDecoderStream only | 1.73 s (1386 MB/s) | 112 MB |
