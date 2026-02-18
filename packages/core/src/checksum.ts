import { createHash } from "node:crypto";

function sortObject(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(sortObject);
  }
  if (value && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>).sort(([a], [b]) => a.localeCompare(b));
    return Object.fromEntries(entries.map(([key, nested]) => [key, sortObject(nested)]));
  }
  return value;
}

export function canonicalJson(value: unknown): string {
  return JSON.stringify(sortObject(value));
}

export function sha256Hex(value: unknown): string {
  const payload = typeof value === "string" ? value : canonicalJson(value);
  return createHash("sha256").update(payload).digest("hex");
}
