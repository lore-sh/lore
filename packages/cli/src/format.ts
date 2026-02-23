import type { Commit } from "@toss/core";

export function toJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

export function printTable(rows: Array<Record<string, unknown>>): string {
  if (rows.length === 0) {
    return "(no rows)";
  }

  const headers = Object.keys(rows[0] ?? {});
  const widths = headers.map((header) => Math.max(header.length, ...rows.map((row) => String(row[header] ?? "null").length)));

  const makeLine = (values: string[]): string =>
    `| ${values.map((value, index) => value.padEnd(widths[index] ?? value.length)).join(" | ")} |`;

  const divider = `+-${widths.map((width) => "-".repeat(width)).join("-+-")}-+`;
  const headerLine = makeLine(headers);
  const lines = rows.map((row) => makeLine(headers.map((header) => String(row[header] ?? "null"))));

  return [divider, headerLine, divider, ...lines, divider].join("\n");
}

export function formatTimestamp(unixMs: number): string {
  const date = new Date(unixMs);
  return Number.isNaN(date.getTime()) ? String(unixMs) : date.toISOString();
}

export function summarizeCommit(entry: Commit): Record<string, unknown> {
  return {
    commit_id: entry.commitId,
    seq: entry.seq,
    created_at: formatTimestamp(entry.createdAt),
    created_at_unix_ms: entry.createdAt,
    kind: entry.kind,
    message: entry.message,
    parent_ids: entry.parentIds,
    state_hash_after: entry.stateHashAfter,
    schema_hash_after: entry.schemaHashAfter,
    revertible: entry.revertible,
    revert_target_id: entry.revertTargetId,
  };
}
