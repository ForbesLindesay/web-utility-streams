# Benchmarks

Benchmarks comparing this package's JSON, newline delimited JSON, server-sent event and line splitting streams with popular libraries that do the same jobs. The latest results are in [RESULTS.md](RESULTS.md).

## Running the benchmarks

```bash
npm run bench
```

This builds the library, generates the input data (if it doesn't already exist) and runs every benchmark at 10MB, 256MB and 2GB. The results are written to [RESULTS.md](RESULTS.md) as each table is completed, as well as being printed. A full run takes roughly 15 minutes on a fast machine, and the generated data needs about 7GB of disk space, in `benchmarks/.data/` (which is ignored by git). You can choose what to run with these options:

```bash
npm run bench -- --sizes 10MB --suites decode-json --libraries web-utility-streams,stream-json
```

| Option        | Description                                                                    | Default                              |
| ------------- | ------------------------------------------------------------------------------ | ------------------------------------ |
| `--sizes`     | Input sizes. MB and GB are decimal (1MB = 1,000,000 bytes).                    | `10MB,256MB,2GB`                     |
| `--suites`    | Which suites to run.                                                           | all                                  |
| `--libraries` | Only run cases whose library name includes one of these. Baselines always run. | all                                  |
| `--runs`      | Runs per measurement. The median is reported.                                  | 5 for sizes up to 100MB, otherwise 1 |
| `--out`       | The Markdown file to write the results to.                                     | `benchmarks/RESULTS.md`              |

## What's measured

There are seven suites:

- **decode-json:** decode the elements of the `results` array in a file shaped like a paginated API response: `{"total":N,"page":1,"results":[...],"next":null}`, with one record per line.
- **decode-ndjson:** decode a file with one JSON record per line.
- **encode-json:** encode generated records as the elements of a JSON array.
- **encode-ndjson:** encode generated records as newline delimited JSON.
- **split-lines:** split the newline delimited JSON file into lines, without parsing them.
- **decode-sse:** decode a [`text/event-stream`](https://html.spec.whatwg.org/multipage/server-sent-events.html) (server-sent events) file, with one event per record.
- **encode-sse:** encode generated events in the `text/event-stream` format.

The records look like typical API data (numbers, booleans, `null`, nested objects and arrays, ISO dates, strings that need escaping and non-ASCII text), and are about 300 bytes each. Each server-sent event has an `id`, an `event` type, and a record's JSON as its `data`. Every 10th event's data is pretty-printed, so it's split over multiple `data` lines, and there's an occasional comment and `retry` field, as a real server might send. Everything is generated deterministically by [data.mjs](data.mjs).

### Reading the input

Each decoder reads the input file the way it would in a real program. Libraries that use web streams (including this one) read it with `FileHandle.readableWebStream()`, which produces 16KiB chunks. Libraries that use Node.js streams read it with `fs.createReadStream()`, which produces 64KiB chunks. The encoders read generated records (or events) one at a time.

### Libraries

| Library                                                                                      | Weekly downloads (Nov 2026) | Used for                                       |
| -------------------------------------------------------------------------------------------- | --------------------------- | ---------------------------------------------- |
| [stream-json](https://www.npmjs.com/package/stream-json)                                     | 15M                         | All four JSON suites                           |
| [JSONStream](https://www.npmjs.com/package/JSONStream)                                       | 14M                         | decode-json, encode-json                       |
| [bfj](https://www.npmjs.com/package/bfj)                                                     | 4.6M                        | decode-json                                    |
| [@streamparser/json](https://www.npmjs.com/package/@streamparser/json)                       | 3.8M                        | decode-json, decode-ndjson                     |
| [split2](https://www.npmjs.com/package/split2)                                               | 142M                        | decode-ndjson (with `JSON.parse`), split-lines |
| [ndjson](https://www.npmjs.com/package/ndjson)                                               | 2.5M                        | decode-ndjson, encode-ndjson                   |
| `node:readline`                                                                              | built in                    | decode-ndjson (with `JSON.parse`), split-lines |
| [eventsource-parser](https://www.npmjs.com/package/eventsource-parser)                       | 118M                        | decode-sse                                     |
| [@microsoft/fetch-event-source](https://www.npmjs.com/package/@microsoft/fetch-event-source) | 5.7M                        | decode-sse                                     |
| [eventsource-encoder](https://www.npmjs.com/package/eventsource-encoder)                     | 128K                        | encode-sse                                     |

Each library is used the way its documentation recommends. `stream-json`'s stages are combined into a single stream with [stream-chain](https://www.npmjs.com/package/stream-chain), as its documentation shows, which is much faster than piping them as separate streams. `stream-json` is measured with both Node.js streams and web streams. `@streamparser/json` is measured with web streams (using `@streamparser/json-whatwg`). `bfj` isn't included in the encoder benchmarks, because `bfj.streamify` can only encode data that's already in memory, not a stream of values. `@microsoft/fetch-event-source` doesn't export its parser, so we import its `getLines` and `getMessages` functions from its CommonJS build. They decode bytes directly, using callbacks rather than a stream. `eventsource-encoder` only provides an `encode` function, so it's run in a `TransformStream`.

There are also some baselines for context. These show the cost of just reading the file, or just generating the records, and the speed of `JSON.parse`/`JSON.stringify` on the whole input at once. The whole-input baselines aren't streaming, and are only run for inputs up to 512MB, as the whole input has to fit in a single string.

### Method

- Every measurement runs in a fresh Node.js process, so libraries can't affect each other's memory usage or JIT optimisations.
- Times include reading the input file (or generating the records), so they reflect the time a real program would take. The "read the file" and "generate the records" baselines show how much of that is fixed overhead.
- Every decoder run is checked: the number of records (or lines, or events) and the sum of their ids must be exactly right. For inputs up to 256MB, each encoder also does an extra, untimed run that keeps its output and parses it back, checking it in the same way. For server-sent events, this also checks that each event's data is the JSON of the record with the event's id. This is done separately so that keeping the output doesn't affect the timed runs' speed or memory usage.
- Throughput is input bytes per second for decoders, and output characters per second for encoders.
- Peak memory is the peak resident set size of the whole process, including Node.js itself (about 70MB).
