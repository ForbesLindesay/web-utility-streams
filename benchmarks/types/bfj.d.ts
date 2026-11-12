// bfj doesn't have type definitions, so these describe the part of its API
// that the benchmarks use
declare module "bfj" {
  const bfj: {
    /**
     * Stream the values in a JSON document for which `selector` returns true
     */
    match(
      stream: NodeJS.ReadableStream,
      selector: (key: unknown, value: unknown, depth: number) => boolean,
    ): NodeJS.ReadableStream & AsyncIterable<unknown>;
  };
  export default bfj;
}
