export function formatTimestamp(unixMs: number | null): string {
  if (unixMs === null) {
    return "n/a";
  }
  const date = new Date(unixMs);
  if (Number.isNaN(date.getTime())) {
    return String(unixMs);
  }
  return date.toISOString();
}

export function formatRelativeTime(unixMs: number): string {
  const delta = Date.now() - unixMs;
  const abs = Math.abs(delta);
  const sign = delta >= 0 ? "ago" : "from now";

  if (abs < 60_000) {
    const seconds = Math.max(1, Math.round(abs / 1_000));
    return `${seconds}s ${sign}`;
  }
  if (abs < 3_600_000) {
    const minutes = Math.max(1, Math.round(abs / 60_000));
    return `${minutes}m ${sign}`;
  }
  if (abs < 86_400_000) {
    const hours = Math.max(1, Math.round(abs / 3_600_000));
    return `${hours}h ${sign}`;
  }
  const days = Math.max(1, Math.round(abs / 86_400_000));
  return `${days}d ${sign}`;
}

export function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes < 0) {
    return "n/a";
  }
  if (bytes < 1_024) {
    return `${bytes} B`;
  }
  if (bytes < 1_048_576) {
    return `${(bytes / 1_024).toFixed(1)} KB`;
  }
  if (bytes < 1_073_741_824) {
    return `${(bytes / 1_048_576).toFixed(1)} MB`;
  }
  return `${(bytes / 1_073_741_824).toFixed(1)} GB`;
}
