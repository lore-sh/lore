import { createClient, type Client, type InArgs, type ResultSet, type Row, type Transaction } from "@libsql/client";
import type { Database } from "bun:sqlite";
import { drizzle } from "drizzle-orm/libsql";
import { migrate as migrateLibsql } from "drizzle-orm/libsql/migrator";
import { existsSync } from "node:fs";
import { basename, resolve } from "node:path";
import {
  COMMIT_PARENT_TABLE,
  COMMIT_TABLE,
  EFFECT_ROW_TABLE,
  EFFECT_SCHEMA_TABLE,
  ENGINE_META_TABLE,
  LAST_PULLED_COMMIT_META_KEY,
  LAST_PUSHED_COMMIT_META_KEY,
  LAST_SYNC_ERROR_META_KEY,
  LAST_SYNC_STATE_META_KEY,
  MAIN_REF_NAME,
  OP_TABLE,
  PRESERVED_META_DEFAULTS,
  REF_TABLE,
  RESETTABLE_META_DEFAULTS,
  getMetaValue,
  resolveDbPath,
  runInTransactionWithDeferredForeignKeys,
  setMetaValue,
  withInitializedDatabase,
  withInitializedDatabaseAsync,
} from "./db";
import { clearAuthToken, parseRemotePlatform, readAuthToken, readRemoteConfig, writeAuthToken, writeRemoteConfig } from "./config";
import { getClientPath } from "./engine/client";
import { TossError, isTossError } from "./errors";
import { getHeadCommit, getHeadCommitId, getCommitById } from "./log";
import { initDatabase } from "./init";
import { findCommitSeq, getCommitReplayInput, loadCommitReplayInputs, replayCommitExactly } from "./replay";
import type { CommitReplayInput } from "./log";
import type {
  Operation,
  RemoteHead,
  SyncConfig,
  SyncConflict,
  SyncResult,
  SyncState,
  TossSyncStatus,
} from "./types";
import { canonicalJson, sha256Hex } from "./checksum";

const ENGINE_MIGRATION_DIR = resolve(import.meta.dir, "../migration");
const COMMIT_SIZE_WARNING_THRESHOLD_BYTES = 256 * 1024;
const REMOTE_REQUIRED_READ_TABLES = [
  ENGINE_META_TABLE,
  COMMIT_TABLE,
  COMMIT_PARENT_TABLE,
  REF_TABLE,
  OP_TABLE,
  EFFECT_ROW_TABLE,
  EFFECT_SCHEMA_TABLE,
];
type RemoteReadState = "initialized" | "empty";

interface SqlExecutor {
  execute(stmt: string | { sql: string; args?: InArgs }, args?: InArgs): Promise<ResultSet>;
}

function normalizeMetaString(value: string | null): string | null {
  if (value === null) {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length === 0 ? null : trimmed;
}

function parseRemoteDbName(remoteUrl: string): string | null {
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

function classifySyncBoundaryError(error: unknown): TossError {
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

function normalizeToken(token: string | null | undefined): string | undefined {
  if (!token) {
    return undefined;
  }
  const trimmed = token.trim();
  return trimmed.length === 0 ? undefined : trimmed;
}

function authTokenForPlatform(config: SyncConfig, override?: string | null): string | undefined {
  if (override === null) {
    return undefined;
  }
  const fromOverride = normalizeToken(override);
  if (fromOverride) {
    return fromOverride;
  }
  return readAuthToken(config.platform);
}

function openRemoteClient(config: SyncConfig, authTokenOverride?: string | null): Client {
  const authToken = authTokenForPlatform(config, authTokenOverride);
  return createClient({
    url: config.remoteUrl,
    ...(authToken ? { authToken } : {}),
  });
}

function readSyncConfig(): SyncConfig | null {
  const remote = readRemoteConfig();
  if (!remote) {
    return null;
  }
  return {
    platform: remote.platform,
    remoteUrl: remote.url,
    remoteDbName: parseRemoteDbName(remote.url),
  };
}

function writeSyncState(db: Database, state: SyncState, error: string | null): void {
  setMetaValue(db, LAST_SYNC_STATE_META_KEY, state);
  setMetaValue(db, LAST_SYNC_ERROR_META_KEY, error ?? "");
}

function writeLastPushedCommit(db: Database, commitId: string | null): void {
  setMetaValue(db, LAST_PUSHED_COMMIT_META_KEY, commitId ?? "");
}

function writeLastPulledCommit(db: Database, commitId: string | null): void {
  setMetaValue(db, LAST_PULLED_COMMIT_META_KEY, commitId ?? "");
}

function pendingCommitsFromHead(db: Database, lastPushedCommit: string | null): number {
  const head = getHeadCommit(db);
  if (!head) {
    return 0;
  }
  if (!lastPushedCommit) {
    return head.seq;
  }
  const pushedSeq = findCommitSeq(db, lastPushedCommit);
  if (!pushedSeq) {
    return head.seq;
  }
  return Math.max(head.seq - pushedSeq, 0);
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

async function detectRemoteReadState(client: Client): Promise<RemoteReadState> {
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

async function ensureRemoteInitialized(client: Client): Promise<void> {
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

async function fetchRemoteHead(executor: SqlExecutor): Promise<RemoteHead> {
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

async function remoteHasCommit(client: Client, commitId: string): Promise<boolean> {
  const result = await client.execute({
    sql: "SELECT 1 AS ok FROM _toss_commit WHERE commit_id = ? LIMIT 1",
    args: [commitId],
  });
  return rowsFrom(result).length > 0;
}

async function remoteCommitSeq(client: Client, commitId: string): Promise<number | null> {
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

async function pushReplayCommitWithCas(
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

function buildSyncResult(
  action: SyncResult["action"],
  state: SyncState,
  pushed: number,
  pulled: number,
  localHead: string | null,
  remoteHead: string | null,
  options: { conflict?: SyncConflict | undefined; error?: string | undefined } = {},
): SyncResult {
  return {
    action,
    state,
    pushed,
    pulled,
    localHead,
    remoteHead,
    conflict: options.conflict,
    error: options.error,
  };
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

async function fetchRemoteReplayInputsAfterSeq(client: Client, fromSeqExclusive: number): Promise<CommitReplayInput[]> {
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

function syncStateFromPending(pending: number): SyncState {
  return pending > 0 ? "pending" : "synced";
}

function statusStateForConfiguredDb(storedState: string | null, pendingCommits: number): SyncState {
  if (storedState === "conflict") {
    return "conflict";
  }
  if (pendingCommits > 0 || storedState === "pending") {
    return "pending";
  }
  return "synced";
}

async function runPush(action: SyncResult["action"]): Promise<SyncResult> {
  return await withInitializedDatabaseAsync(async ({ db }) => {
    const config = readSyncConfig();
    if (!config) {
      writeSyncState(db, "offline", "Remote is not configured");
      throw new TossError("CONFIG_ERROR", "Remote is not configured. Run `toss remote connect`.");
    }

    const client = openRemoteClient(config);
    try {
      await ensureRemoteInitialized(client);
      const localHead = getHeadCommitId(db);
      const remoteHeadBefore = await fetchRemoteHead(client);

      if (remoteHeadBefore.commitId && !getCommitById(db, remoteHeadBefore.commitId)) {
        const message = `Remote HEAD ${remoteHeadBefore.commitId} is unknown locally. Pull before push.`;
        writeSyncState(db, "conflict", message);
        throw new TossError("SYNC_NON_FAST_FORWARD", message);
      }

      const fromSeq = remoteHeadBefore.commitId ? (findCommitSeq(db, remoteHeadBefore.commitId) ?? 0) : 0;
      const replays = loadCommitReplayInputs(db, fromSeq);
      let expectedRemoteHead = remoteHeadBefore.commitId;
      let pushed = 0;
      for (const replay of replays) {
        await pushReplayCommitWithCas(client, replay, expectedRemoteHead);
        expectedRemoteHead = replay.commitId;
        pushed += 1;
      }

      const localHeadAfter = getHeadCommitId(db);
      const remoteHeadAfter = await fetchRemoteHead(client);
      writeLastPushedCommit(db, remoteHeadAfter.commitId);
      const pending = pendingCommitsFromHead(db, remoteHeadAfter.commitId);
      const state = syncStateFromPending(pending);
      writeSyncState(db, state, null);
      return buildSyncResult(action, state, pushed, 0, localHeadAfter, remoteHeadAfter.commitId);
    } catch (error) {
      const mapped = classifySyncBoundaryError(error);
      if (mapped.code === "SYNC_NON_FAST_FORWARD" || mapped.code === "SYNC_DIVERGED") {
        writeSyncState(db, "conflict", mapped.message);
      } else {
        writeSyncState(db, "pending", mapped.message);
      }
      throw mapped;
    } finally {
      client.close();
    }
  });
}

async function runPull(action: SyncResult["action"]): Promise<SyncResult> {
  return await withInitializedDatabaseAsync(async ({ db }) => {
    const config = readSyncConfig();
    if (!config) {
      writeSyncState(db, "offline", "Remote is not configured");
      throw new TossError("CONFIG_ERROR", "Remote is not configured. Run `toss remote connect`.");
    }

    const client = openRemoteClient(config);
    try {
      const localHead = getHeadCommitId(db);
      const remoteState = await detectRemoteReadState(client);
      if (remoteState === "empty") {
        writeLastPulledCommit(db, null);
        writeLastPushedCommit(db, null);
        const pending = pendingCommitsFromHead(db, null);
        const state = syncStateFromPending(pending);
        writeSyncState(db, state, null);
        return buildSyncResult(action, state, 0, 0, localHead, null);
      }
      const remoteHead = await fetchRemoteHead(client);
      let fromSeq = 0;

      if (localHead) {
        const remoteHasLocalHead = await remoteHasCommit(client, localHead);
        if (remoteHasLocalHead) {
          fromSeq = (await remoteCommitSeq(client, localHead)) ?? 0;
        } else if (remoteHead.commitId && getCommitById(db, remoteHead.commitId)) {
          const pending = pendingCommitsFromHead(db, normalizeMetaString(getMetaValue(db, LAST_PUSHED_COMMIT_META_KEY)));
          const state = syncStateFromPending(pending);
          writeSyncState(db, state, null);
          return buildSyncResult(action, state, 0, 0, localHead, remoteHead.commitId);
        } else if (remoteHead.commitId !== null) {
          const message = `Local HEAD ${localHead} is not present on remote, and remote HEAD ${remoteHead.commitId} is not present locally.`;
          writeSyncState(db, "conflict", message);
          throw new TossError("SYNC_DIVERGED", message);
        }
      }

      const replayInputs = await fetchRemoteReplayInputsAfterSeq(client, fromSeq);
      let pulled = 0;
      for (const replay of replayInputs) {
        if (getCommitById(db, replay.commitId)) {
          continue;
        }
        runInTransactionWithDeferredForeignKeys(db, () => {
          replayCommitExactly(db, replay, { errorCode: "SYNC_DIVERGED" });
        });
        pulled += 1;
      }

      const localHeadAfter = getHeadCommitId(db);
      const remoteHeadAfter = await fetchRemoteHead(client);
      writeLastPulledCommit(db, remoteHeadAfter.commitId);
      if (localHeadAfter === remoteHeadAfter.commitId) {
        writeLastPushedCommit(db, remoteHeadAfter.commitId);
      }
      const pending = pendingCommitsFromHead(db, normalizeMetaString(getMetaValue(db, LAST_PUSHED_COMMIT_META_KEY)));
      const state = syncStateFromPending(pending);
      writeSyncState(db, state, null);
      return buildSyncResult(action, state, 0, pulled, localHeadAfter, remoteHeadAfter.commitId);
    } catch (error) {
      const mapped = classifySyncBoundaryError(error);
      if (mapped.code === "SYNC_DIVERGED") {
        writeSyncState(db, "conflict", mapped.message);
      } else {
        writeSyncState(db, "pending", mapped.message);
      }
      throw mapped;
    } finally {
      client.close();
    }
  });
}

function syncConfigFromInputs(options: {
  platform: SyncConfig["platform"];
  url: string;
}): SyncConfig {
  const platform = parseRemotePlatform(options.platform);
  const trimmedUrl = options.url.trim();
  if (trimmedUrl.length === 0) {
    throw new TossError("CONFIG_ERROR", "Remote URL must not be empty");
  }
  return {
    platform,
    remoteUrl: trimmedUrl,
    remoteDbName: parseRemoteDbName(trimmedUrl),
  };
}

export async function connectRemote(options: {
  platform: SyncConfig["platform"];
  url: string;
  authToken?: string | null | undefined;
}): Promise<SyncConfig> {
  return await withInitializedDatabaseAsync(async ({ db }) => {
    const config = syncConfigFromInputs(options);
    const previousConfig = readSyncConfig();
    const previousIdentity = previousConfig ? `${previousConfig.platform}\u0000${previousConfig.remoteUrl}\u0000${previousConfig.remoteDbName ?? ""}` : null;
    const nextIdentity = `${config.platform}\u0000${config.remoteUrl}\u0000${config.remoteDbName ?? ""}`;
    const remoteChanged = previousIdentity !== nextIdentity;
    const client = openRemoteClient(config, options.authToken);
    try {
      await detectRemoteReadState(client);
      writeRemoteConfig({
        platform: config.platform,
        url: config.remoteUrl,
      });
      const token = normalizeToken(options.authToken);
      if (token) {
        writeAuthToken(config.platform, token);
      } else if (options.authToken === null) {
        clearAuthToken(config.platform);
      }
      if (remoteChanged) {
        writeLastPushedCommit(db, null);
        writeLastPulledCommit(db, null);
      }
      writeSyncState(db, "pending", null);
      return config;
    } catch (error) {
      throw classifySyncBoundaryError(error);
    } finally {
      client.close();
    }
  });
}

export function getSyncConfig(): SyncConfig | null {
  return withInitializedDatabase(() => readSyncConfig());
}

export async function pushToRemote(): Promise<SyncResult> {
  return await runPush("push");
}

export async function pullFromRemote(): Promise<SyncResult> {
  return await runPull("pull");
}

export async function syncWithRemote(options: { action?: SyncResult["action"] } = {}): Promise<SyncResult> {
  const action = options.action ?? "sync";
  const pullResult = await runPull(action);
  const pushResult = await runPush(action);
  return buildSyncResult(
    action,
    pushResult.state,
    pushResult.pushed,
    pullResult.pulled,
    pushResult.localHead,
    pushResult.remoteHead,
  );
}

export async function autoSyncAfterApply(): Promise<SyncResult | null> {
  return await withInitializedDatabaseAsync(async ({ db }) => {
    const config = readSyncConfig();
    if (!config) {
      return null;
    }
    try {
      return await syncWithRemote({ action: "auto_sync" });
    } catch (error) {
      const mapped = classifySyncBoundaryError(error);
      const localHead = getHeadCommitId(db);
      const isConflict = mapped.code === "SYNC_NON_FAST_FORWARD" || mapped.code === "SYNC_DIVERGED";
      const state: SyncState = isConflict ? "conflict" : "pending";
      writeSyncState(db, state, mapped.message);
      return buildSyncResult("auto_sync", state, 0, 0, localHead, null, {
        conflict: isConflict
          ? {
              kind: mapped.code === "SYNC_DIVERGED" ? "diverged" : "non_fast_forward",
              message: mapped.message,
              localHead,
              remoteHead: null,
            }
          : undefined,
        error: mapped.message,
      });
    }
  });
}

export async function getRemoteStatus(): Promise<{
  config: SyncConfig | null;
  localHead: string | null;
  remoteHead: RemoteHead | null;
  pendingCommits: number;
  hasAuthToken: boolean;
}> {
  return await withInitializedDatabaseAsync(async ({ db }) => {
    const config = readSyncConfig();
    const localHead = getHeadCommitId(db);
    const localPending = pendingCommitsFromHead(db, normalizeMetaString(getMetaValue(db, LAST_PUSHED_COMMIT_META_KEY)));
    if (!config) {
      return {
        config: null,
        localHead,
        remoteHead: null,
        pendingCommits: localPending,
        hasAuthToken: readAuthToken("turso") !== undefined,
      };
    }
    const client = openRemoteClient(config);
    try {
      const remoteState = await detectRemoteReadState(client);
      if (remoteState === "empty") {
        return {
          config,
          localHead,
          remoteHead: null,
          pendingCommits: pendingCommitsFromHead(db, null),
          hasAuthToken: authTokenForPlatform(config) !== undefined,
        };
      }
      return {
        config,
        localHead,
        remoteHead: await fetchRemoteHead(client),
        pendingCommits: localPending,
        hasAuthToken: authTokenForPlatform(config) !== undefined,
      };
    } catch (error) {
      throw classifySyncBoundaryError(error);
    } finally {
      client.close();
    }
  });
}

export async function cloneFromRemote(options: {
  platform: SyncConfig["platform"];
  url: string;
  forceNew?: boolean | undefined;
  authToken?: string | null | undefined;
}): Promise<{ dbPath: string; sync: SyncResult }> {
  const targetDbPath = getClientPath() ?? resolveDbPath();
  const forceNew = options.forceNew ?? false;
  if (!forceNew && existsSync(targetDbPath)) {
    throw new TossError("CONFIG_ERROR", `Clone target already exists: ${targetDbPath}. Use --force-new to replace it.`);
  }
  const initialized = await initDatabase({ dbPath: targetDbPath, forceNew, generateSkills: false });
  await connectRemote({
    platform: options.platform,
    url: options.url,
    authToken: options.authToken,
  });
  const sync = await runPull("clone");
  return { dbPath: initialized.dbPath, sync };
}

export function buildSyncStatus(db: Database): TossSyncStatus {
  const config = readSyncConfig();
  const lastPushedCommit = normalizeMetaString(getMetaValue(db, LAST_PUSHED_COMMIT_META_KEY));
  const lastPulledCommit = normalizeMetaString(getMetaValue(db, LAST_PULLED_COMMIT_META_KEY));
  const storedState = normalizeMetaString(getMetaValue(db, LAST_SYNC_STATE_META_KEY));
  const lastError = normalizeMetaString(getMetaValue(db, LAST_SYNC_ERROR_META_KEY));
  const pendingCommits = pendingCommitsFromHead(db, lastPushedCommit);
  const state = config ? statusStateForConfiguredDb(storedState, pendingCommits) : "offline";
  return {
    configured: config !== null,
    remotePlatform: config?.platform ?? null,
    remoteUrl: config?.remoteUrl ?? null,
    remoteDbName: config?.remoteDbName ?? null,
    state,
    lastPushedCommit,
    lastPulledCommit,
    pendingCommits,
    lastError,
  };
}

export function commitSizeWarning(commitId: string): string | null {
  return withInitializedDatabase(({ db }) => {
    const replay = getCommitReplayInput(db, commitId);
    const payloadSize = canonicalJson({
      operations: replay.operations,
      rowEffects: replay.rowEffects,
      schemaEffects: replay.schemaEffects,
    }).length;
    if (payloadSize < COMMIT_SIZE_WARNING_THRESHOLD_BYTES) {
      return null;
    }
    return `Commit payload is large (${payloadSize} bytes). Frequent update/delete operations can grow history quickly.`;
  });
}
