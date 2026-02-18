import type { Database } from "bun:sqlite";
import { sha256Hex } from "./checksum";
import type { LogEntry, LogKind, Operation } from "./types";

interface AppendLogInput {
  kind: LogKind;
  message: string;
  operations: Operation[];
  schemaVersion: number;
  revertedTargetId?: string | null;
}

interface RawLogRow {
  rowid: number;
  id: string;
  timestamp: string;
  kind: LogKind;
  message: string;
  operations: string;
  schema_version: number;
  checksum: string;
  reverted_target_id: string | null;
}

export function generateCommitId(): string {
  return crypto.randomUUID();
}

function checksumPayload(entry: {
  id: string;
  timestamp: string;
  kind: LogKind;
  message: string;
  operations: Operation[];
  schemaVersion: number;
  revertedTargetId: string | null;
}): Record<string, unknown> {
  return {
    id: entry.id,
    timestamp: entry.timestamp,
    kind: entry.kind,
    message: entry.message,
    operations: entry.operations,
    schemaVersion: entry.schemaVersion,
    revertedTargetId: entry.revertedTargetId,
  };
}

export function appendLog(db: Database, input: AppendLogInput): LogEntry {
  const id = generateCommitId();
  const timestamp = new Date().toISOString();
  const revertedTargetId = input.revertedTargetId ?? null;
  const checksum = sha256Hex(
    checksumPayload({
      id,
      timestamp,
      kind: input.kind,
      message: input.message,
      operations: input.operations,
      schemaVersion: input.schemaVersion,
      revertedTargetId,
    }),
  );

  db.query(
    `
    INSERT INTO _toss_log(id, timestamp, kind, message, operations, schema_version, checksum, reverted_target_id)
    VALUES(?, ?, ?, ?, ?, ?, ?, ?)
    `,
  ).run(id, timestamp, input.kind, input.message, JSON.stringify(input.operations), input.schemaVersion, checksum, revertedTargetId);

  const row = db
    .query(
      `
      SELECT rowid, id, timestamp, kind, message, operations, schema_version, checksum, reverted_target_id
      FROM _toss_log WHERE id = ? LIMIT 1
      `,
    )
    .get(id) as RawLogRow;
  return decodeLogRow(row);
}

function decodeLogRow(row: RawLogRow): LogEntry {
  return {
    rowid: row.rowid,
    id: row.id,
    timestamp: row.timestamp,
    kind: row.kind,
    message: row.message,
    operations: JSON.parse(row.operations) as Operation[],
    schemaVersion: row.schema_version,
    checksum: row.checksum,
    revertedTargetId: row.reverted_target_id,
  };
}

export function listLogsAscending(db: Database): LogEntry[] {
  const rows = db
    .query(
      `
      SELECT rowid, id, timestamp, kind, message, operations, schema_version, checksum, reverted_target_id
      FROM _toss_log
      ORDER BY rowid ASC
      `,
    )
    .all() as RawLogRow[];
  return rows.map(decodeLogRow);
}

export function listLogsDescending(db: Database): LogEntry[] {
  const rows = db
    .query(
      `
      SELECT rowid, id, timestamp, kind, message, operations, schema_version, checksum, reverted_target_id
      FROM _toss_log
      ORDER BY rowid DESC
      `,
    )
    .all() as RawLogRow[];
  return rows.map(decodeLogRow);
}

export function collectRevertedTargets(logs: LogEntry[]): Set<string> {
  const reverted = new Set<string>();
  for (const entry of logs) {
    if (entry.kind === "revert" && entry.revertedTargetId) {
      reverted.add(entry.revertedTargetId);
    }
  }
  return reverted;
}

export function activeApplyLogs(logs: LogEntry[], extraRevertedTargets: string[] = []): LogEntry[] {
  const reverted = collectRevertedTargets(logs);
  for (const id of extraRevertedTargets) {
    reverted.add(id);
  }
  return logs.filter((entry) => entry.kind === "apply" && !reverted.has(entry.id));
}
