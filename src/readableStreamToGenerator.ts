const DONE_RESULT = { done: true as const, value: undefined };
/**
 * Convert a ReadableStream into an AsyncGenerator. This implementation
 * also ensures that calls to `return()` and `throw()` on the generator will immediately cancel the underlying
 * stream. This is important for GraphQL subscriptions to ensure that resources are cleaned up promptly
 * when a client disconnects. The default implementation of AsyncIterable returned by ReadableStream does not
 * do this correctly.
 */
export default function readableStreamToGenerator<TEvent>(
  stream: ReadableStream<TEvent>,
): AsyncGenerator<TEvent, void> {
  // We could use the built-in async iterator, but it doesn't call cancel on return/throw
  // if we are currently waiting on a read. This would cause memory leaks in GraphQL
  // subscriptions implementation.
  //
  // return stream[Symbol.asyncIterator]()

  const reader = stream.getReader();
  // Once the stream has ended (or been cancelled) we release the lock and all
  // further calls to `next()` return `{done: true}`, and calls to `return()`
  // are no-ops, matching the behaviour of a native generator.
  let finished = false;

  const finish = () => {
    if (finished) return;
    finished = true;
    reader.releaseLock();
  };
  const cancel = async (reason?: unknown) => {
    if (finished) return;
    finished = true;
    try {
      await reader.cancel(reason);
    } finally {
      reader.releaseLock();
    }
  };

  const onResult = (
    result: ReadableStreamReadResult<TEvent>,
  ): IteratorResult<TEvent, void> => {
    if (result.done) {
      finish();
      return DONE_RESULT;
    }
    return result;
  };
  const onError = (err: unknown): never => {
    finish();
    throw err;
  };

  return {
    // Not an `async` function, as the extra promise it creates for every
    // chunk has a noticeable cost
    next() {
      if (finished) return Promise.resolve(DONE_RESULT);
      return reader.read().then(onResult, onError);
    },
    async return() {
      await cancel();
      return DONE_RESULT;
    },
    async throw(err: any) {
      try {
        await cancel(err);
      } catch {
        // Prefer re-throwing the original error over any error from cancel
      }
      throw err;
    },
    [Symbol.asyncIterator]() {
      return this;
    },
    async [Symbol.asyncDispose]() {
      await cancel();
    },
  };
}
