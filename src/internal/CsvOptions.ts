/**
 * Check that a CSV option (delimiter, quote or escape) is a single character
 * that isn't a line break.
 */
export function getCsvChar(
  className: string,
  name: string,
  value: string,
): string {
  if (typeof value !== "string" || value.length !== 1) {
    throw new TypeError(
      `${className}: ${name} must be a single character, got ${JSON.stringify(value)}`,
    );
  }
  if (value === "\r" || value === "\n") {
    throw new TypeError(`${className}: ${name} cannot be a line break`);
  }
  return value;
}
