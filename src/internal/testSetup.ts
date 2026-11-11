// Runs before every test file (see `setupFiles` in vitest.config.ts).
//
// None of our tests should cause an unhandled promise rejection, as that
// would crash a Node.js application. Vitest reports these for the run as a
// whole, but this check attributes them to the test that caused them.
import { afterEach, beforeEach, expect } from "vitest";
import { delay } from "./StreamTestUtils.js";

// This package doesn't depend on @types/node, so declare just what we need
declare const process: {
  on(event: "unhandledRejection", listener: (reason: unknown) => void): void;
  off(event: "unhandledRejection", listener: (reason: unknown) => void): void;
};

const unhandled: unknown[] = [];
const onUnhandled = (reason: unknown) => void unhandled.push(reason);

beforeEach(() => {
  unhandled.length = 0;
  process.on("unhandledRejection", onUnhandled);
});

afterEach(async () => {
  // Give any stray promises a chance to reject
  await delay(10);
  process.off("unhandledRejection", onUnhandled);
  expect(unhandled, "Unhandled promise rejections").toEqual([]);
});
