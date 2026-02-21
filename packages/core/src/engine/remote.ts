import { createClient, type Client, type InArgs, type ResultSet, type Row, type Transaction } from "@libsql/client";
import { drizzle } from "drizzle-orm/libsql";
import { migrate as migrateLibsql } from "drizzle-orm/libsql/migrator";
import { basename, resolve } from "node:path";
import {
  COMMIT_PARENT_TABLE,
  COMMIT_TABLE,
  EFFECT_ROW_TABLE,
  EFFECT_SCHEMA_TABLE,
  ENGINE_META_TABLE,
  MAIN_REF_NAME,
  OP_TABLE,
  PRESERVED_META_DEFAULTS,
  REF_TABLE,
  RESETTABLE_META_DEFAULTS,
} from "./db";
import { readAuthToken } from "../config";
import { TossError, isTossError } from "../errors";
import { canonicalJson, sha256Hex } from "./checksum";
import type { CommitReplayInput } from "./log";
import type { Operation, RemoteHead, SyncConfig } from "../types";

const ENGINE_MIGRATION_DIR = resolve(import.meta.dir, "../../migration");
const REMOTE_REQUIRED_READ_TABLES = [
  ENGINE_META_TABLE,
  COMMIT_TABLE,
  COMMIT_PARENT_TABLE,
  REF_TABLE,
  OP_TABLE,
  EFFECT_ROW_TABLE,
  EFFECT_SCHEMA_TABLE,
];

export type RemoteReadState = "initialized" | "empty";

interface SqlExecutor {
  execute(stmt: string | { sql: string; args?: InArgs }, args?: InArgs): Promise<ResultSet>;
}

export function parseRemoteDbName(remoteUrl: string): string | null {
  try {
    const parsed = new URL(remoteUrl);
    if (parsed.protocol === "file:") {
      const file = basename(parsed.pathname);
      if (!file) {
        return null;
      }
      return file.endsWith(".db") ? file.slice(0, -3) : file;
    }
    const host = parsed.hostname.trim();
    if (!host) {
      return null;
    }
    const [name] = host.split(".");
    return name?.trim().length ? name.trim() : null;
  } catch {
    return null;
  }
}

function parseRowValue(row: Row, key: string): unknown {
  const map = row as unknown as Record<string, unknown>;
  return map[key];
}

function parseString(value: unknown, label: string): string {
  if (typeof value !== "string") {
    throw new TossError("SYNC_DIVERGED", `Remote ${label} is invalid`);
  }
  return value;
}

function parseNullableString(value: unknown, label: string): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  return parseString(value, label);
}

function parseInteger(value: unknown, label: string): number {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new TossError("SYNC_DIVERGED", `Remote ${label} is not a finite number`);
    }
    return value;
  }
  if (typeof value === "bigint") {
    return Number(value);
  }
  if (typeof value === "string") {
    const parsed = Number.parseInt(value, 10);
    if (Number.isNaN(parsed)) {
      throw new TossError("SYNC_DIVERGED", `Remote ${label} is not an integer`);
    }
    return parsed;
  }
  throw new TossError("SYNC_DIVERGED", `Remote ${label} is invalid`);
}

function parseCommitKind(value: string): "apply" | "revert" {
  if (value === "apply" || value === "revert") {
    return value;
  }
  throw new TossError("SYNC_DIVERGED", `Remote commit kind is invalid: ${value}`);
}

function parseJson<T>(value: unknown, label: string): T {
  const text = parseString(value, label);
  try {
    return JSON.parse(text) as T;
  } catch {
    throw new TossError("SYNC_DIVERGED", `Remote ${label} JSON is invalid`);
  }
}

export function classifySyncBoundaryError(error: unknown): TossError {
  if (isTossError(error)) {
    return error;
  }
  if (error instanceof Error) {
    const message = error.message.toLowerCase();
    if (message.includes("unauthorized") || message.includes("401") || message.includes("auth")) {
      return new TossError("SYNC_AUTH_FAILED", error.message);
    }
    return new TossError("SYNC_REMOTE_UNREACHABLE", error.message);
  }
  return new TossError("SYNC_REMOTE_UNREACHABLE", String(error));
}

export function normalizeToken(token: string | null | undefined): string | undefined {
  if (!token) {
    return undefined;
  }
  const trimmed = token.trim();
  return trimmed.length === 0 ? undefined : trimmed;
}

export function authTokenForPlatform(config: SyncConfig, override?: string | null): string | undefined {
  if (override === null) {
    return undefined;
  }
  const fromOverride = normalizeToken(override);
  if (fromOverride) {
    return fromOverride;
  }
  return readAuthToken(config.platform);
}

export function openRemoteClient(config: SyncConfig, authTokenOverride?: string | null): Client {
  const authToken = authTokenForPlatform(config, authTokenOverride);
  return createClient({
    url: config.remoteUrl,
    ...(authToken ? { authToken } : {}),
  });
}

function rowsFrom(result: ResultSet): Row[] {
  return result.rows as Row[];
}

async function remoteTableExists(client: Client, tableName: string): Promise<boolean> {
  const result = await client.execute({
    sql: "SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
    args: [tableName],
  });
  return rowsFrom(result).length > 0;
}

export async function detectRemoteReadState(client: Client): Promise<RemoteReadState> {
  const existing = new Set<string>();
  for (const tableName of REMOTE_REQUIRED_READ_TABLES) {
    if (await remoteTableExists(client, tableName)) {
      existing.add(tableName);
    }
  }
  if (existing.size === 0) {
    return "empty";
  }
  const missing = REMOTE_REQUIRED_READ_TABLES.filter((tableName) => !existing.has(tableName));
  if (missing.length > 0) {
    throw new TossError(
      "CONFIG_ERROR",
      `Remote database has an incomplete toss schema (missing: ${missing.join(", ")}). Recreate remote schema with write access.`,
    );
  }
  return "initialized";
}

function metaInsertStatement(key: string, value: string): { sql: string; args: InArgs } {
  return {
    sql: "INSERT INTO _toss_engine_meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO NOTHING",
    args: [key, value],
  };
}

export async function ensureRemoteInitialized(client: Client): Promise<void> {
  const db = drizzle({ client });
  await migrateLibsql(db, { migrationsFolder: ENGINE_MIGRATION_DIR });

  const metaDefaults = [...RESETTABLE_META_DEFAULTS, ...PRESERVED_META_DEFAULTS];
  await client.batch(
    [
      ...metaDefaults.map(([key, value]) => metaInsertStatement(key, value)),
      {
        sql: "INSERT INTO _toss_ref(name, commit_id, updated_at) VALUES (?, NULL, ?) ON CONFLICT(name) DO NOTHING",
        args: [MAIN_REF_NAME, Date.now()],
      },
    ],
    "write",
  );
}

export async function fetchRemoteHead(executor: SqlExecutor): Promise<RemoteHead> {
  const result = await executor.execute({
    sql: `
      SELECT r.commit_id AS commit_id, c.seq AS seq
      FROM _toss_ref AS r
      LEFT JOIN _toss_commit AS c ON c.commit_id = r.commit_id
      WHERE r.name = ?
      LIMIT 1
    `,
    args: [MAIN_REF_NAME],
  });
  const row = rowsFrom(result)[0];
  if (!row) {
    return { commitId: null, seq: 0 };
  }
  const commitId = parseNullableString(parseRowValue(row, "commit_id"), "head.commit_id");
  if (!commitId) {
    return { commitId: null, seq: 0 };
  }
  return {
    commitId,
    seq: parseInteger(parseRowValue(row, "seq"), "head.seq"),
  };
}

export async function remoteHasCommit(client: Client, commitId: string): Promise<boolean> {
  const result = await client.execute({
    sql: "SELECT 1 AS ok FROM _toss_commit WHERE commit_id = ? LIMIT 1",
    args: [commitId],
  });
  return rowsFrom(result).length > 0;
}

export async function remoteCommitSeq(client: Client, commitId: string): Promise<number | null> {
  const result = await client.execute({
    sql: "SELECT seq AS seq FROM _toss_commit WHERE commit_id = ? LIMIT 1",
    args: [commitId],
  });
  const row = rowsFrom(result)[0];
  if (!row) {
    return null;
  }
  return parseInteger(parseRowValue(row, "seq"), "_toss_commit.seq");
}

async function insertReplayIntoRemote(tx: Transaction, replay: CommitReplayInput): Promise<void> {
  await tx.execute({
    sql: `
      INSERT INTO _toss_commit(
        commit_id, seq, kind, message, created_at, parent_count,
        schema_hash_before, schema_hash_after, state_hash_after, plan_hash,
        inverse_ready, reverted_target_id
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(commit_id) DO NOTHING
    `,
    args: [
      replay.commitId,
      replay.seq,
      replay.kind,
      replay.message,
      replay.createdAt,
      replay.parentIds.length,
      replay.schemaHashBefore,
      replay.schemaHashAfter,
      replay.stateHashAfter,
      replay.planHash,
      replay.inverseReady ? 1 : 0,
      replay.revertedTargetId,
    ],
  });

  for (let i = 0; i < replay.parentIds.length; i += 1) {
    await tx.execute({
      sql: `
        INSERT INTO _toss_commit_parent(commit_id, parent_commit_id, ord)
        VALUES (?, ?, ?)
        ON CONFLICT(commit_id, ord) DO NOTHING
      `,
      args: [replay.commitId, replay.parentIds[i]!, i],
    });
  }

  for (let i = 0; i < replay.operations.length; i += 1) {
    const operation = replay.operations[i]!;
    await tx.execute({
      sql: `
        INSERT INTO _toss_op(commit_id, op_index, op_type, op_json)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(commit_id, op_index) DO NOTHING
      `,
      args: [replay.commitId, i, operation.type, canonicalJson(operation)],
    });
  }

  for (let i = 0; i < replay.rowEffects.length; i += 1) {
    const effect = replay.rowEffects[i]!;
    const beforeRowJson = effect.beforeRow ? canonicalJson(effect.beforeRow) : null;
    const afterRowJson = effect.afterRow ? canonicalJson(effect.afterRow) : null;
    await tx.execute({
      sql: `
        INSERT INTO _toss_effect_row(
          commit_id, effect_index, table_name, pk_json, op_kind,
          before_row_json, after_row_json, before_hash, after_hash
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(commit_id, effect_index) DO NOTHING
      `,
      args: [
        replay.commitId,
        i,
        effect.tableName,
        canonicalJson(effect.pk),
        effect.opKind,
        beforeRowJson,
        afterRowJson,
        beforeRowJson ? sha256Hex(beforeRowJson) : null,
        afterRowJson ? sha256Hex(afterRowJson) : null,
      ],
    });
  }

  for (let i = 0; i < replay.schemaEffects.length; i += 1) {
    const effect = replay.schemaEffects[i]!;
    await tx.execute({
      sql: `
        INSERT INTO _toss_effect_schema(
          commit_id, effect_index, table_name, before_table_json, after_table_json
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(commit_id, effect_index) DO NOTHING
      `,
      args: [
        replay.commitId,
        i,
        effect.tableName,
        effect.beforeTable ? canonicalJson(effect.beforeTable) : null,
        effect.afterTable ? canonicalJson(effect.afterTable) : null,
      ],
    });
  }
}

export async function pushReplayCommitWithCas(
  client: Client,
  replay: CommitReplayInput,
  expectedRemoteHead: string | null,
): Promise<void> {
  const tx = await client.transaction("write");
  try {
    const currentHead = await fetchRemoteHead(tx);
    if (currentHead.commitId !== expectedRemoteHead) {
      throw new TossError(
        "SYNC_NON_FAST_FORWARD",
        `Remote HEAD changed during push. expected=${expectedRemoteHead ?? "null"} actual=${currentHead.commitId ?? "null"}`,
      );
    }

    await insertReplayIntoRemote(tx, replay);
    const update = await tx.execute({
      sql: `
        UPDATE _toss_ref
        SET commit_id = ?, updated_at = ?
        WHERE name = ? AND ((? IS NULL AND commit_id IS NULL) OR commit_id = ?)
      `,
      args: [replay.commitId, replay.createdAt, MAIN_REF_NAME, expectedRemoteHead, expectedRemoteHead],
    });
    if (update.rowsAffected !== 1) {
      throw new TossError(
        "SYNC_NON_FAST_FORWARD",
        `Remote HEAD changed during push CAS update. expected=${expectedRemoteHead ?? "null"}`,
      );
    }
    await tx.execute({
      sql: `
        INSERT INTO _toss_reflog(ref_name, old_commit_id, new_commit_id, reason, created_at)
        VALUES (?, ?, ?, ?, ?)
      `,
      args: [MAIN_REF_NAME, expectedRemoteHead, replay.commitId, replay.kind === "revert" ? "revert" : "apply", Date.now()],
    });
    await tx.commit();
  } catch (error) {
    try {
      await tx.rollback();
    } catch {
      // no-op
    }
    throw error;
  } finally {
    tx.close();
  }
}

async function fetchRemoteReplayInput(client: Client, commitId: string): Promise<CommitReplayInput> {
  const commitResult = await client.execute({
    sql: `
      SELECT
        commit_id, seq, kind, message, created_at, parent_count,
        schema_hash_before, schema_hash_after, state_hash_after, plan_hash,
        inverse_ready, reverted_target_id
      FROM _toss_commit
      WHERE commit_id = ?
      LIMIT 1
    `,
    args: [commitId],
  });
  const commitRow = rowsFrom(commitResult)[0];
  if (!commitRow) {
    throw new TossError("SYNC_DIVERGED", `Remote commit not found during pull: ${commitId}`);
  }

  const parentsResult = await client.execute({
    sql: `
      SELECT parent_commit_id
      FROM _toss_commit_parent
      WHERE commit_id = ?
      ORDER BY ord ASC
    `,
    args: [commitId],
  });
  const parentIds = rowsFrom(parentsResult).map((row) =>
    parseString(parseRowValue(row, "parent_commit_id"), "_toss_commit_parent.parent_commit_id")
  );

  const opsResult = await client.execute({
    sql: `
      SELECT op_json
      FROM _toss_op
      WHERE commit_id = ?
      ORDER BY op_index ASC
    `,
    args: [commitId],
  });
  const operations = rowsFrom(opsResult).map((row) => parseJson<Operation>(parseRowValue(row, "op_json"), "_toss_op.op_json"));

  const rowEffectsResult = await client.execute({
    sql: `
      SELECT table_name, pk_json, op_kind, before_row_json, after_row_json
      FROM _toss_effect_row
      WHERE commit_id = ?
      ORDER BY effect_index ASC
    `,
    args: [commitId],
  });
  const rowEffects = rowsFrom(rowEffectsResult).map((row) => ({
    tableName: parseString(parseRowValue(row, "table_name"), "_toss_effect_row.table_name"),
    pk: parseJson<Record<string, string>>(parseRowValue(row, "pk_json"), "_toss_effect_row.pk_json"),
    opKind: parseString(parseRowValue(row, "op_kind"), "_toss_effect_row.op_kind") as "insert" | "update" | "delete",
    beforeRow: parseRowValue(row, "before_row_json")
      ? parseJson<CommitReplayInput["rowEffects"][number]["beforeRow"]>(parseRowValue(row, "before_row_json"), "_toss_effect_row.before_row_json")
      : null,
    afterRow: parseRowValue(row, "after_row_json")
      ? parseJson<CommitReplayInput["rowEffects"][number]["afterRow"]>(parseRowValue(row, "after_row_json"), "_toss_effect_row.after_row_json")
      : null,
  }));

  const schemaEffectsResult = await client.execute({
    sql: `
      SELECT table_name, before_table_json, after_table_json
      FROM _toss_effect_schema
      WHERE commit_id = ?
      ORDER BY effect_index ASC
    `,
    args: [commitId],
  });
  const schemaEffects = rowsFrom(schemaEffectsResult).map((row) => ({
    tableName: parseString(parseRowValue(row, "table_name"), "_toss_effect_schema.table_name"),
    beforeTable: parseRowValue(row, "before_table_json")
      ? parseJson<CommitReplayInput["schemaEffects"][number]["beforeTable"]>(parseRowValue(row, "before_table_json"), "_toss_effect_schema.before_table_json")
      : null,
    afterTable: parseRowValue(row, "after_table_json")
      ? parseJson<CommitReplayInput["schemaEffects"][number]["afterTable"]>(parseRowValue(row, "after_table_json"), "_toss_effect_schema.after_table_json")
      : null,
  }));

  return {
    commitId: parseString(parseRowValue(commitRow, "commit_id"), "_toss_commit.commit_id"),
    seq: parseInteger(parseRowValue(commitRow, "seq"), "_toss_commit.seq"),
    kind: parseCommitKind(parseString(parseRowValue(commitRow, "kind"), "_toss_commit.kind")),
    message: parseString(parseRowValue(commitRow, "message"), "_toss_commit.message"),
    createdAt: parseInteger(parseRowValue(commitRow, "created_at"), "_toss_commit.created_at"),
    parentIds,
    schemaHashBefore: parseString(parseRowValue(commitRow, "schema_hash_before"), "_toss_commit.schema_hash_before"),
    schemaHashAfter: parseString(parseRowValue(commitRow, "schema_hash_after"), "_toss_commit.schema_hash_after"),
    stateHashAfter: parseString(parseRowValue(commitRow, "state_hash_after"), "_toss_commit.state_hash_after"),
    planHash: parseString(parseRowValue(commitRow, "plan_hash"), "_toss_commit.plan_hash"),
    inverseReady: parseInteger(parseRowValue(commitRow, "inverse_ready"), "_toss_commit.inverse_ready") === 1,
    revertedTargetId: parseNullableString(parseRowValue(commitRow, "reverted_target_id"), "_toss_commit.reverted_target_id"),
    operations,
    rowEffects,
    schemaEffects,
  };
}

export async function fetchRemoteReplayInputsAfterSeq(client: Client, fromSeqExclusive: number): Promise<CommitReplayInput[]> {
  const result = await client.execute({
    sql: `
      SELECT commit_id
      FROM _toss_commit
      WHERE seq > ?
      ORDER BY seq ASC
    `,
    args: [fromSeqExclusive],
  });
  const commitIds = rowsFrom(result).map((row) => parseString(parseRowValue(row, "commit_id"), "_toss_commit.commit_id"));
  const replayInputs: CommitReplayInput[] = [];
  for (const commitId of commitIds) {
    replayInputs.push(await fetchRemoteReplayInput(client, commitId));
  }
  return replayInputs;
}
