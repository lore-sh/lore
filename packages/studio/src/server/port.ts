export const DEFAULT_STUDIO_PORT = 7055;
const MIN_PORT = 1;
const MAX_PORT = 65535;
const DECIMAL_INTEGER_PATTERN = /^[0-9]+$/;

function assertPortRange(port: number, original: string): number {
  if (!Number.isSafeInteger(port) || port < MIN_PORT || port > MAX_PORT) {
    throw new Error(`Invalid port: ${original}. Expected an integer between ${MIN_PORT} and ${MAX_PORT}.`);
  }
  return port;
}

export function parseStudioPort(value: string): number {
  if (!DECIMAL_INTEGER_PATTERN.test(value)) {
    throw new Error(`Invalid port: ${value}. Expected an integer between ${MIN_PORT} and ${MAX_PORT}.`);
  }
  return assertPortRange(Number(value), value);
}

export function parseStudioPortArg(value: string | undefined): number {
  if (!value) {
    throw new Error("studio requires a value for --port");
  }
  return parseStudioPort(value);
}

export function normalizeStudioPort(value: number | undefined): number {
  if (value === undefined) {
    return DEFAULT_STUDIO_PORT;
  }
  return assertPortRange(value, String(value));
}
