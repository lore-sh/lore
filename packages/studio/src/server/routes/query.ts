export function parsePositiveInt(value: string | undefined): number | undefined {
  if (!value) {
    return undefined;
  }
  if (!/^\d+$/.test(value)) {
    return undefined;
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1) {
    return undefined;
  }
  return parsed;
}

export function singleValue(input: string | string[] | undefined): string | undefined {
  if (typeof input === "string") {
    return input;
  }
  if (Array.isArray(input)) {
    return typeof input[0] === "string" ? input[0] : undefined;
  }
  return undefined;
}
