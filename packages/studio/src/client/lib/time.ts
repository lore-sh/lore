export function formatTimestamp(unixMs: number | null): string {
  if (unixMs === null) {
    return "n/a";
  }
  const date = new Date(unixMs);
  return Number.isNaN(date.getTime()) ? String(unixMs) : date.toISOString();
}
