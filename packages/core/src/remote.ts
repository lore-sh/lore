import { createClient, type Client, type InArgs, type ResultSet, type Row, type Transaction } from "@libsql/client";
import { drizzle } from "drizzle-orm/libsql";
import { migrate as migrateLibsql } from "drizzle-orm/libsql/migrator";
import { basename, resolve } from "node:path";
import {
  COMMIT_PARENT_TABLE,
  COMMIT_TABLE,
  ROW_EFFECT_TABLE,
  SCHEMA_EFFECT_TABLE,
  META_TABLE,
  LAST_MATERIALIZED_AT_META_KEY,
  LAST_MATERIALIZED_COMMIT_META_KEY,
  LAST_MATERIALIZED_ERROR_META_KEY,
  MAIN_REF_NAME,
  OP_TABLE,
  PRESERVED_META_DEFAULTS,
  REF_TABLE,
  RESETTABLE_META_DEFAULTS,
  normalizeMetaString,
} from "./db";
import { readAuthToken } from "./config";
import { CodedError } from "./error";
import { canonicalJson, sha256Hex } from "./sql";
import { extractCheckConstraints, parseColumnDefinitionsFromCreateTable, rewriteCreateTableName } from "./sql";
import { schemaHashFromDescriptor } from "./effect";
import { normalizeSqlNullable, pragmaLiteral, quoteIdentifier } from "./sql";
import type { CommitReplayInput } from "./commit";
import type { EncodedCell, EncodedRow } from "./schema";
import type { Operation } from "./operation";
import type { RemoteHead, SyncConfig } from "./sync";

const ENGINE_MIGRATION_DIR = resolve(import.meta.dir, "../migration");
const REMOTE_REQUIRED_READ_TABLES = [
  META_TABLE,
  COMMIT_TABLE,
  COMMIT_PARENT_TABLE,
  REF_TABLE,
  OP_TABLE,
  ROW_EFFECT_TABLE,
  SCHEMA_EFFECT_TABLE,
];

const SQLITE_SEQUENCE_TABLE = "sqlite_sequence";

export type RemoteReadState = "initialized" | "empty";

export interface RemoteProjectionStatus {
  projectionHead: string | null;
  projectionLagCommits: number;
  projectionError: string | null;
}

interface SqlExecutor {
  execute(stmt: string | { sql: string; args?: InArgs }, args?: InArgs): Promise<ResultSet>;
}

type RowEffect = CommitReplayInput["rowEffects"][number];
type SchemaEffect = CommitReplayInput["schemaEffects"][number];

type ReplayDirection = "forward" | "inverse";

function parseRemoteRow(row: Row): Record<string, unknown> {
  if (!row || typeof row !== "object" || Array.isArray(row)) {
    throw new CodedError("SYNC_DIVERGED", "Remote row payload is invalid");
  }
  return row as Record<string, unknown>;
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
  return parseRemoteRow(row)[key];
}

function parseString(value: unknown, label: string): string {
  if (typeof value !== "string") {
    throw new CodedError("SYNC_DIVERGED", `Remote ${label} is invalid`);
  }
  return value;
}

function parseNullableString(value: unknown, label: string): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  return parseString(value, label);
}

function parseStringLike(value: unknown, label: string): string {
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "bigint" || typeof value === "boolean") {
    return String(value);
  }
  throw new CodedError("SYNC_DIVERGED", `Remote ${label} is invalid`);
}

function parseNullableStringLike(value: unknown, label: string): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  return parseStringLike(value, label);
}

function parseInteger(value: unknown, label: string): number {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new CodedError("SYNC_DIVERGED", `Remote ${label} is not a finite number`);
    }
    return value;
  }
  if (typeof value === "bigint") {
    return Number(value);
  }
  if (typeof value === "string") {
    const parsed = Number.parseInt(value, 10);
    if (Number.isNaN(parsed)) {
      throw new CodedError("SYNC_DIVERGED", `Remote ${label} is not an integer`);
    }
    return parsed;
  }
  throw new CodedError("SYNC_DIVERGED", `Remote ${label} is invalid`);
}

function parseCommitKind(value: string): "apply" | "revert" {
  if (value === "apply" || value === "revert") {
    return value;
  }
  throw new CodedError("SYNC_DIVERGED", `Remote commit kind is invalid: ${value}`);
}

function parseOpKind(value: string): "insert" | "update" | "delete" {
  if (value === "insert" || value === "update" || value === "delete") {
    return value;
  }
  throw new CodedError("SYNC_DIVERGED", `Remote row effect kind is invalid: ${value}`);
}

function parseJson<T>(value: unknown, label: string): T {
  const text = parseString(value, label);
  try {
    return JSON.parse(text) as T;
  } catch {
    throw new CodedError("SYNC_DIVERGED", `Remote ${label} JSON is invalid`);
  }
}

function parseSqlStorageClass(value: unknown, label: string): EncodedCell["storageClass"] {
  if (value === "null" || value === "integer" || value === "real" || value === "text" || value === "blob") {
    return value;
  }
  throw new CodedError("SYNC_DIVERGED", `Remote ${label} storage class is invalid`);
}

function rowHash(row: EncodedRow | null): string | null {
  if (!row) {
    return null;
  }
  return sha256Hex(row);
}

function isSystemSideEffectTable(tableName: string): boolean {
  return tableName === SQLITE_SEQUENCE_TABLE;
}

function projectionFailure(message: string): CodedError {
  return new CodedError("SYNC_DIVERGED", `Remote projection failed: ${message}`);
}

function projectionErrorMessage(error: unknown): string | null {
  if (CodedError.hasCode(error, "SYNC_DIVERGED") && error.message.startsWith("Remote projection failed:")) {
    return error.message;
  }
  return null;
}

function describeError(error: unknown): string {
  if (CodedError.is(error)) {
    return `${error.code}: ${error.message}`;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

function toProjectionError(error: unknown, context?: string): CodedError {
  if (CodedError.hasCode(error, "SYNC_DIVERGED") && error.message.startsWith("Remote projection failed:")) {
    return error;
  }
  const message = context ? `${context}: ${describeError(error)}` : describeError(error);
  return projectionFailure(message);
}

async function runProjectionStep<T>(run: () => Promise<T>, context: string): Promise<T> {
  try {
    return await run();
  } catch (error) {
    throw toProjectionError(error, context);
  }
}

export function classifySyncBoundaryError(error: unknown): CodedError {
  if (CodedError.is(error)) {
    return error;
  }
  if (error instanceof Error) {
    const message = error.message.toLowerCase();
    if (message.includes("unauthorized") || message.includes("401") || message.includes("auth")) {
      return new CodedError("SYNC_AUTH_FAILED", error.message, { cause: error });
    }
    return new CodedError("SYNC_UNREACHABLE", error.message, { cause: error });
  }
  return new CodedError("SYNC_UNREACHABLE", String(error));
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
  return createClient(authToken ? { url: config.remoteUrl, authToken } : { url: config.remoteUrl });
}

function rowsFrom(result: ResultSet): Row[] {
  return result.rows as Row[];
}

async function remoteTableExists(executor: SqlExecutor, tableName: string): Promise<boolean> {
  const result = await executor.execute({
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
    throw new CodedError(
      "CONFIG",
      `Remote database has an incomplete toss schema (missing: ${missing.join(", ")}). Recreate remote schema with write access.`,
    );
  }
  return "initialized";
}

function metaInsertStatement(key: string, value: string): { sql: string; args: InArgs } {
  return {
    sql: "INSERT INTO _toss_meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO NOTHING",
    args: [key, value],
  };
}

async function getRemoteMetaValue(executor: SqlExecutor, key: string): Promise<string | null> {
  const result = await executor.execute({
    sql: "SELECT value FROM _toss_meta WHERE key = ? LIMIT 1",
    args: [key],
  });
  const row = rowsFrom(result)[0];
  if (!row) {
    return null;
  }
  return parseString(parseRowValue(row, "value"), "_toss_meta.value");
}

async function setRemoteMetaValue(executor: SqlExecutor, key: string, value: string): Promise<void> {
  await executor.execute({
    sql: `
      INSERT INTO _toss_meta(key, value)
      VALUES (?, ?)
      ON CONFLICT(key) DO UPDATE SET value = excluded.value
    `,
    args: [key, value],
  });
}

async function writeMaterializedCheckpoint(executor: SqlExecutor, commitId: string | null): Promise<void> {
  await setRemoteMetaValue(executor, LAST_MATERIALIZED_COMMIT_META_KEY, commitId ?? "");
  await setRemoteMetaValue(executor, LAST_MATERIALIZED_AT_META_KEY, String(Date.now()));
  await setRemoteMetaValue(executor, LAST_MATERIALIZED_ERROR_META_KEY, "");
}

async function persistMaterializationErrorBestEffort(client: Client, error: unknown): Promise<void> {
  const message = projectionErrorMessage(error);
  if (!message) {
    return;
  }
  try {
    await setRemoteMetaValue(client, LAST_MATERIALIZED_ERROR_META_KEY, message.slice(0, 4000));
  } catch {
    // no-op
  }
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

export async function remoteHasCommit(executor: SqlExecutor, commitId: string): Promise<boolean> {
  const result = await executor.execute({
    sql: "SELECT 1 AS ok FROM _toss_commit WHERE commit_id = ? LIMIT 1",
    args: [commitId],
  });
  return rowsFrom(result).length > 0;
}

export async function remoteCommitSeq(executor: SqlExecutor, commitId: string): Promise<number | null> {
  const result = await executor.execute({
    sql: "SELECT seq AS seq FROM _toss_commit WHERE commit_id = ? LIMIT 1",
    args: [commitId],
  });
  const row = rowsFrom(result)[0];
  if (!row) {
    return null;
  }
  return parseInteger(parseRowValue(row, "seq"), "_toss_commit.seq");
}

async function listRemoteUserTables(executor: SqlExecutor): Promise<string[]> {
  const result = await executor.execute({
    sql: "SELECT name FROM sqlite_master WHERE type='table' AND name NOT GLOB '_toss_*' AND name NOT GLOB '__drizzle_*' AND name NOT GLOB 'sqlite_*' ORDER BY name",
  });
  return rowsFrom(result).map((row) => parseString(parseRowValue(row, "name"), "sqlite_master.name"));
}

function normalizeStateValue(value: unknown): string | number | boolean | null {
  if (value === null || typeof value === "string" || typeof value === "boolean") {
    return value;
  }
  if (typeof value === "bigint") {
    return Number(value);
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  if (value instanceof Uint8Array) {
    return Buffer.from(value).toString("base64");
  }
  if (value instanceof ArrayBuffer) {
    return Buffer.from(new Uint8Array(value)).toString("base64");
  }
  if (ArrayBuffer.isView(value)) {
    return Buffer.from(value.buffer, value.byteOffset, value.byteLength).toString("base64");
  }
  return JSON.stringify(value);
}

function normalizeStateRow(row: Record<string, unknown>): Record<string, string | number | boolean | null> {
  const out: Record<string, string | number | boolean | null> = {};
  for (const [key, value] of Object.entries(row)) {
    out[key] = normalizeStateValue(value);
  }
  return out;
}

async function remotePrimaryKeyColumns(executor: SqlExecutor, tableName: string): Promise<string[]> {
  const result = await executor.execute(`PRAGMA table_info(${pragmaLiteral(tableName)})`);
  const pkColumns = rowsFrom(result)
    .map((row) => ({
      name: parseString(parseRowValue(row, "name"), `PRAGMA table_info(${tableName}).name`),
      pk: parseInteger(parseRowValue(row, "pk"), `PRAGMA table_info(${tableName}).pk`),
    }))
    .filter((column) => column.pk > 0)
    .sort((a, b) => a.pk - b.pk)
    .map((column) => column.name);
  if (pkColumns.length === 0) {
    throw projectionFailure(`Table ${tableName} must define PRIMARY KEY for tracked operations`);
  }
  return pkColumns;
}

async function remoteStateHash(executor: SqlExecutor): Promise<string> {
  const tables = await listRemoteUserTables(executor);
  const state: Record<string, Array<Record<string, string | number | boolean | null>>> = {};
  for (const tableName of tables) {
    const pkColumns = await remotePrimaryKeyColumns(executor, tableName);
    const orderBy = pkColumns.map((column) => `${quoteIdentifier(column, { unsafe: true })} ASC`).join(", ");
    const result = await executor.execute(`SELECT * FROM ${quoteIdentifier(tableName, { unsafe: true })} ORDER BY ${orderBy}`);
    state[tableName] = rowsFrom(result).map((row) => normalizeStateRow(parseRemoteRow(row)));
  }
  return sha256Hex(state);
}

async function remoteSchemaHash(executor: SqlExecutor): Promise<string> {
  const tables = await listRemoteUserTables(executor);
  const tableListResult = await executor.execute("PRAGMA table_list");
  const tableOptions = new Map<string, { withoutRowid: boolean; strict: boolean }>();
  for (const row of rowsFrom(tableListResult)) {
    const schema = parseString(parseRowValue(row, "schema"), "PRAGMA table_list.schema");
    const type = parseString(parseRowValue(row, "type"), "PRAGMA table_list.type");
    if (schema !== "main" || type !== "table") {
      continue;
    }
    const name = parseString(parseRowValue(row, "name"), "PRAGMA table_list.name");
    const withoutRowid = parseInteger(parseRowValue(row, "wr"), "PRAGMA table_list.wr") === 1;
    const strict = parseInteger(parseRowValue(row, "strict"), "PRAGMA table_list.strict") === 1;
    tableOptions.set(name, { withoutRowid, strict });
  }

  const descriptors: Parameters<typeof schemaHashFromDescriptor>[0]["tables"] = [];
  for (const tableName of tables) {
    const tableDdlResult = await executor.execute({
      sql: "SELECT sql FROM sqlite_master WHERE type='table' AND name = ? COLLATE NOCASE LIMIT 1",
      args: [tableName],
    });
    const tableDdlRow = rowsFrom(tableDdlResult)[0];
    const tableDdl = tableDdlRow ? parseNullableStringLike(parseRowValue(tableDdlRow, "sql"), "sqlite_master.sql") : null;
    const columnDefinitions = parseColumnDefinitionsFromCreateTable(tableDdl);
    const checks = extractCheckConstraints(tableDdl);

    const columnsResult = await executor.execute(`PRAGMA table_xinfo(${pragmaLiteral(tableName)})`);
    const columns = rowsFrom(columnsResult)
      .map((row) => {
        const name = parseString(parseRowValue(row, "name"), `PRAGMA table_xinfo(${tableName}).name`);
        return {
          definitionSql: columnDefinitions.get(name.toLowerCase()) ?? null,
          cid: parseInteger(parseRowValue(row, "cid"), `PRAGMA table_xinfo(${tableName}).cid`),
          name,
          type: parseStringLike(parseRowValue(row, "type"), `PRAGMA table_xinfo(${tableName}).type`).trim().toUpperCase(),
          notnull: parseInteger(parseRowValue(row, "notnull"), `PRAGMA table_xinfo(${tableName}).notnull`),
          dfltValue: parseNullableStringLike(parseRowValue(row, "dflt_value"), `PRAGMA table_xinfo(${tableName}).dflt_value`),
          pk: parseInteger(parseRowValue(row, "pk"), `PRAGMA table_xinfo(${tableName}).pk`),
          hidden: parseInteger(parseRowValue(row, "hidden"), `PRAGMA table_xinfo(${tableName}).hidden`),
        };
      })
      .sort((a, b) => a.cid - b.cid);

    const foreignKeysResult = await executor.execute(`PRAGMA foreign_key_list(${pragmaLiteral(tableName)})`);
    const foreignKeysById = new Map<
      number,
      {
        id: number;
        refTable: string;
        onUpdate: string;
        onDelete: string;
        match: string;
        mappings: Array<{ seq: number; from: string; to: string | null }>;
      }
    >();
    for (const row of rowsFrom(foreignKeysResult)) {
      const id = parseInteger(parseRowValue(row, "id"), `PRAGMA foreign_key_list(${tableName}).id`);
      const seq = parseInteger(parseRowValue(row, "seq"), `PRAGMA foreign_key_list(${tableName}).seq`);
      const mapping = {
        seq,
        from: parseString(parseRowValue(row, "from"), `PRAGMA foreign_key_list(${tableName}).from`),
        to: parseNullableStringLike(parseRowValue(row, "to"), `PRAGMA foreign_key_list(${tableName}).to`),
      };
      const existing = foreignKeysById.get(id);
      if (!existing) {
        foreignKeysById.set(id, {
          id,
          refTable: parseString(parseRowValue(row, "table"), `PRAGMA foreign_key_list(${tableName}).table`),
          onUpdate: parseStringLike(parseRowValue(row, "on_update"), `PRAGMA foreign_key_list(${tableName}).on_update`),
          onDelete: parseStringLike(parseRowValue(row, "on_delete"), `PRAGMA foreign_key_list(${tableName}).on_delete`),
          match: parseStringLike(parseRowValue(row, "match"), `PRAGMA foreign_key_list(${tableName}).match`),
          mappings: [mapping],
        });
        continue;
      }
      existing.mappings.push(mapping);
    }
    const foreignKeys = Array.from(foreignKeysById.values())
      .map((fk) => ({ ...fk, mappings: fk.mappings.sort((a, b) => a.seq - b.seq) }))
      .sort((a, b) => a.id - b.id);

    const indexesResult = await executor.execute(`PRAGMA index_list(${pragmaLiteral(tableName)})`);
    const indexes: Array<{
      name: string;
      unique: boolean;
      origin: "c" | "u" | "pk";
      partial: boolean;
      sql: string | null;
      columns: Array<{
        seqno: number;
        cid: number;
        name: string | null;
        desc: number;
        coll: string | null;
        key: number;
      }>;
    }> = [];
    for (const row of rowsFrom(indexesResult)) {
      const indexName = parseString(parseRowValue(row, "name"), `PRAGMA index_list(${tableName}).name`);
      const indexSqlResult = await executor.execute({
        sql: "SELECT sql FROM sqlite_master WHERE type='index' AND name = ? LIMIT 1",
        args: [indexName],
      });
      const indexSqlRow = rowsFrom(indexSqlResult)[0];
      const indexSql = indexSqlRow ? parseNullableStringLike(parseRowValue(indexSqlRow, "sql"), "sqlite_master.sql") : null;
      const indexColumnsResult = await executor.execute(`PRAGMA index_xinfo(${pragmaLiteral(indexName)})`);
      const indexColumns = rowsFrom(indexColumnsResult)
        .map((entry) => ({
          seqno: parseInteger(parseRowValue(entry, "seqno"), `PRAGMA index_xinfo(${indexName}).seqno`),
          cid: parseInteger(parseRowValue(entry, "cid"), `PRAGMA index_xinfo(${indexName}).cid`),
          name: parseNullableStringLike(parseRowValue(entry, "name"), `PRAGMA index_xinfo(${indexName}).name`),
          desc: parseInteger(parseRowValue(entry, "desc"), `PRAGMA index_xinfo(${indexName}).desc`),
          coll: parseNullableStringLike(parseRowValue(entry, "coll"), `PRAGMA index_xinfo(${indexName}).coll`),
          key: parseInteger(parseRowValue(entry, "key"), `PRAGMA index_xinfo(${indexName}).key`),
        }))
        .sort((a, b) => a.seqno - b.seqno);
      const origin = parseString(parseRowValue(row, "origin"), `PRAGMA index_list(${tableName}).origin`);
      if (origin !== "c" && origin !== "u" && origin !== "pk") {
        throw projectionFailure(`Invalid index origin for ${tableName}.${indexName}: ${origin}`);
      }
      indexes.push({
        name: indexName,
        unique: parseInteger(parseRowValue(row, "unique"), `PRAGMA index_list(${tableName}).unique`) === 1,
        origin,
        partial: parseInteger(parseRowValue(row, "partial"), `PRAGMA index_list(${tableName}).partial`) === 1,
        sql: normalizeSqlNullable(indexSql),
        columns: indexColumns,
      });
    }
    indexes.sort((a, b) => a.name.localeCompare(b.name));

    const triggersResult = await executor.execute({
      sql: "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=? ORDER BY name ASC",
      args: [tableName],
    });
    const triggers = rowsFrom(triggersResult).map((row) => ({
      name: parseString(parseRowValue(row, "name"), `trigger(${tableName}).name`),
      sql: normalizeSqlNullable(parseNullableStringLike(parseRowValue(row, "sql"), `trigger(${tableName}).sql`)),
    }));

    descriptors.push({
      tableSql: normalizeSqlNullable(tableDdl),
      table: tableName,
      options: tableOptions.get(tableName) ?? { withoutRowid: false, strict: false },
      columns,
      foreignKeys,
      indexes,
      checks,
      triggers,
    });
  }

  return schemaHashFromDescriptor({ tables: descriptors });
}

interface CommitHashes {
  schemaHashAfter: string;
  stateHashAfter: string;
}

async function fetchCommitHashes(executor: SqlExecutor, commitId: string): Promise<CommitHashes | null> {
  const result = await executor.execute({
    sql: "SELECT schema_hash_after, state_hash_after FROM _toss_commit WHERE commit_id = ? LIMIT 1",
    args: [commitId],
  });
  const row = rowsFrom(result)[0];
  if (!row) {
    return null;
  }
  return {
    schemaHashAfter: parseString(parseRowValue(row, "schema_hash_after"), "_toss_commit.schema_hash_after"),
    stateHashAfter: parseString(parseRowValue(row, "state_hash_after"), "_toss_commit.state_hash_after"),
  };
}

async function verifyProjectionAtCommit(executor: SqlExecutor, commitId: string): Promise<void> {
  const expected = await fetchCommitHashes(executor, commitId);
  if (!expected) {
    throw projectionFailure(`checkpoint commit is missing from canonical history: ${commitId}`);
  }
  const actualSchemaHash = await remoteSchemaHash(executor);
  if (actualSchemaHash !== expected.schemaHashAfter) {
    throw projectionFailure(
      `schema_hash_after mismatch for projection ${commitId}: expected ${expected.schemaHashAfter}, got ${actualSchemaHash}`,
    );
  }
  const actualStateHash = await remoteStateHash(executor);
  if (actualStateHash !== expected.stateHashAfter) {
    throw projectionFailure(
      `state_hash_after mismatch for projection ${commitId}: expected ${expected.stateHashAfter}, got ${actualStateHash}`,
    );
  }
}

async function remoteTableColumns(executor: SqlExecutor, tableName: string): Promise<string[]> {
  const result = await executor.execute(`PRAGMA table_info(${quoteIdentifier(tableName, { unsafe: true })})`);
  const names = rowsFrom(result).map((row) =>
    parseString(parseRowValue(row, "name"), `PRAGMA table_info(${tableName}).name`),
  );
  if (names.length === 0) {
    throw projectionFailure(`Unable to inspect columns for table ${tableName}`);
  }
  return names;
}

function buildPkWhereClause(pk: Record<string, string>): string {
  const keys = Object.keys(pk).sort((a, b) => a.localeCompare(b));
  if (keys.length === 0) {
    throw projectionFailure("Primary key predicate must not be empty");
  }
  const clauses: string[] = [];
  for (const key of keys) {
    const literal = pk[key];
    if (!literal) {
      throw projectionFailure(`Primary key literal is missing for column ${key}`);
    }
    const quoted = quoteIdentifier(key, { unsafe: true });
    clauses.push(literal.toUpperCase() === "NULL" ? `${quoted} IS NULL` : `${quoted} = ${literal}`);
  }
  return clauses.join(" AND ");
}

function buildRowSelectSql(tableName: string, columns: string[], keyColumns: string[], whereClause: string | null): string {
  const quoteAliases = columns.map((_, i) => `__toss_quote_${i}`);
  const hexAliases = columns.map((_, i) => `__toss_hex_${i}`);
  const typeAliases = columns.map((_, i) => `__toss_type_${i}`);
  const parts: string[] = [];
  for (let i = 0; i < columns.length; i += 1) {
    const column = columns[i]!;
    const quotedColumn = quoteIdentifier(column, { unsafe: true });
    parts.push(`quote(${quotedColumn}) AS ${quoteIdentifier(quoteAliases[i]!, { unsafe: true })}`);
    parts.push(`hex(CAST(${quotedColumn} AS BLOB)) AS ${quoteIdentifier(hexAliases[i]!, { unsafe: true })}`);
    parts.push(`typeof(${quotedColumn}) AS ${quoteIdentifier(typeAliases[i]!, { unsafe: true })}`);
  }

  const orderBy = keyColumns.map((key) => `${quoteIdentifier(key, { unsafe: true })} ASC`).join(", ");
  const whereSql = whereClause ? ` WHERE ${whereClause}` : "";
  return `SELECT ${parts.join(", ")} FROM ${quoteIdentifier(tableName, { unsafe: true })}${whereSql} ORDER BY ${orderBy}`;
}

function encodeRowFromRemote(
  row: Record<string, unknown>,
  columns: string[],
  quoteAliases: string[],
  hexAliases: string[],
  typeAliases: string[],
): EncodedRow {
  const encoded: EncodedRow = {};
  for (let i = 0; i < columns.length; i += 1) {
    const column = columns[i]!;
    const quoteAlias = quoteAliases[i]!;
    const hexAlias = hexAliases[i]!;
    const typeAlias = typeAliases[i]!;
    const storageClass = parseSqlStorageClass(row[typeAlias], `${column}.storageClass`);

    let sqlLiteral: string;
    if (storageClass === "null") {
      sqlLiteral = "NULL";
    } else if (storageClass === "text") {
      const hex = parseString(row[hexAlias], `${column}.textHex`);
      sqlLiteral = `CAST(X'${hex}' AS TEXT)`;
    } else if (storageClass === "blob") {
      const hex = parseString(row[hexAlias], `${column}.blobHex`);
      sqlLiteral = `X'${hex}'`;
    } else {
      sqlLiteral = parseString(row[quoteAlias], `${column}.quoted`);
    }

    encoded[column] = { storageClass, sqlLiteral };
  }
  return encoded;
}

async function fetchObservedRowByPk(executor: SqlExecutor, tableName: string, pk: Record<string, string>): Promise<EncodedRow | null> {
  if (!(await remoteTableExists(executor, tableName))) {
    if (isSystemSideEffectTable(tableName)) {
      return null;
    }
    throw projectionFailure(`Table does not exist while applying row effects: ${tableName}`);
  }

  const columns = await remoteTableColumns(executor, tableName);
  const keyColumns = Object.keys(pk).sort((a, b) => a.localeCompare(b));
  const quoteAliases = columns.map((_, i) => `__toss_quote_${i}`);
  const hexAliases = columns.map((_, i) => `__toss_hex_${i}`);
  const typeAliases = columns.map((_, i) => `__toss_type_${i}`);
  const whereClause = buildPkWhereClause(pk);
  const sql = `${buildRowSelectSql(tableName, columns, keyColumns, whereClause)} LIMIT 1`;
  const result = await executor.execute(sql);
  const row = rowsFrom(result)[0];
  if (!row) {
    return null;
  }
  return encodeRowFromRemote(parseRemoteRow(row), columns, quoteAliases, hexAliases, typeAliases);
}

async function insertEncodedRow(executor: SqlExecutor, tableName: string, row: EncodedRow): Promise<void> {
  const columns = Object.keys(row).sort((a, b) => a.localeCompare(b));
  if (columns.length === 0) {
    throw projectionFailure(`Cannot insert empty encoded row for table ${tableName}`);
  }
  const columnSql = columns.map((column) => quoteIdentifier(column, { unsafe: true })).join(", ");
  const valueSql = columns
    .map((column) => {
      const cell = row[column];
      if (!cell) {
        throw projectionFailure(`Missing encoded cell for ${tableName}.${column}`);
      }
      return cell.sqlLiteral;
    })
    .join(", ");
  await executor.execute(`INSERT INTO ${quoteIdentifier(tableName, { unsafe: true })} (${columnSql}) VALUES (${valueSql})`);
}

async function updateEncodedRow(
  executor: SqlExecutor,
  tableName: string,
  pk: Record<string, string>,
  row: EncodedRow,
): Promise<void> {
  const columns = Object.keys(row).sort((a, b) => a.localeCompare(b));
  if (columns.length === 0) {
    throw projectionFailure(`Cannot update empty encoded row for table ${tableName}`);
  }
  const setSql = columns
    .map((column) => {
      const cell = row[column];
      if (!cell) {
        throw projectionFailure(`Missing encoded cell for ${tableName}.${column}`);
      }
      return `${quoteIdentifier(column, { unsafe: true })} = ${cell.sqlLiteral}`;
    })
    .join(", ");
  await executor.execute(
    `UPDATE ${quoteIdentifier(tableName, { unsafe: true })} SET ${setSql} WHERE ${buildPkWhereClause(pk)}`,
  );
}

async function deleteByPk(executor: SqlExecutor, tableName: string, pk: Record<string, string>): Promise<void> {
  await executor.execute(`DELETE FROM ${quoteIdentifier(tableName, { unsafe: true })} WHERE ${buildPkWhereClause(pk)}`);
}

async function referencedTables(executor: SqlExecutor, tableName: string): Promise<string[]> {
  if (!(await remoteTableExists(executor, tableName))) {
    return [];
  }
  const result = await executor.execute(`PRAGMA foreign_key_list(${quoteIdentifier(tableName, { unsafe: true })})`);
  const refs = new Set<string>();
  for (const row of rowsFrom(result)) {
    refs.add(parseString(parseRowValue(row, "table"), `PRAGMA foreign_key_list(${tableName}).table`));
  }
  return Array.from(refs).sort((a, b) => a.localeCompare(b));
}

async function missingReferencedTables(executor: SqlExecutor, tableName: string): Promise<string[]> {
  const refs = await referencedTables(executor, tableName);
  const missing: string[] = [];
  for (const ref of refs) {
    if (!(await remoteTableExists(executor, ref))) {
      missing.push(ref);
    }
  }
  return missing;
}

function effectRowMode(
  effect: RowEffect,
  direction: ReplayDirection,
): { expectedCurrent: EncodedRow | null; target: EncodedRow | null; opLabel: string } {
  if (direction === "forward") {
    if (effect.opKind === "insert") {
      return { expectedCurrent: null, target: effect.afterRow, opLabel: "insert" };
    }
    if (effect.opKind === "update") {
      return { expectedCurrent: effect.beforeRow, target: effect.afterRow, opLabel: "update" };
    }
    return { expectedCurrent: effect.beforeRow, target: null, opLabel: "delete" };
  }

  if (effect.opKind === "insert") {
    return { expectedCurrent: effect.afterRow, target: null, opLabel: "inverse-delete" };
  }
  if (effect.opKind === "update") {
    return { expectedCurrent: effect.afterRow, target: effect.beforeRow, opLabel: "inverse-update" };
  }
  return { expectedCurrent: null, target: effect.beforeRow, opLabel: "inverse-insert" };
}

interface DroppedTrigger {
  name: string;
  sql: string;
}

async function dropTriggersForTables(executor: SqlExecutor, effects: RowEffect[]): Promise<DroppedTrigger[]> {
  const touched = Array.from(new Set(effects.map((effect) => effect.tableName))).sort((a, b) => a.localeCompare(b));
  const dropped: DroppedTrigger[] = [];
  for (const tableName of touched) {
    const result = await executor.execute({
      sql: "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=? AND sql IS NOT NULL ORDER BY name ASC",
      args: [tableName],
    });
    for (const row of rowsFrom(result)) {
      const name = parseString(parseRowValue(row, "name"), "trigger.name");
      const sql = parseString(parseRowValue(row, "sql"), "trigger.sql");
      await executor.execute(`DROP TRIGGER IF EXISTS ${quoteIdentifier(name, { unsafe: true })}`);
      dropped.push({ name, sql });
    }
  }
  return dropped;
}

async function restoreDroppedTriggers(executor: SqlExecutor, dropped: DroppedTrigger[]): Promise<void> {
  for (const trigger of dropped) {
    await executor.execute(trigger.sql);
  }
}

async function applySystemRowEffectReconciled(
  executor: SqlExecutor,
  tableName: string,
  pk: Record<string, string>,
  target: EncodedRow | null,
): Promise<void> {
  const exists = await remoteTableExists(executor, tableName);
  if (!target) {
    if (!exists) {
      return;
    }
    await deleteByPk(executor, tableName, pk);
    return;
  }
  if (!exists) {
    throw projectionFailure(`System table does not exist for reconciled effect: ${tableName}`);
  }
  const current = await fetchObservedRowByPk(executor, tableName, pk);
  if (!current) {
    await insertEncodedRow(executor, tableName, target);
    return;
  }
  await updateEncodedRow(executor, tableName, pk, target);
}

async function applyRowEffectsWithOptions(
  executor: SqlExecutor,
  effects: RowEffect[],
  direction: ReplayDirection,
  options: {
    disableTableTriggers: boolean;
    includeSystemEffects?: boolean;
    includeUserEffects?: boolean;
    systemPolicy?: "strict" | "reconcile";
  },
): Promise<void> {
  const includeSystemEffects = options.includeSystemEffects ?? true;
  const includeUserEffects = options.includeUserEffects ?? true;
  const systemPolicy = options.systemPolicy ?? "strict";
  const filtered = effects.filter((effect) =>
    isSystemSideEffectTable(effect.tableName) ? includeSystemEffects : includeUserEffects,
  );

  const droppedTriggers = options.disableTableTriggers ? await dropTriggersForTables(executor, filtered) : null;
  try {
    const ordered = direction === "forward" ? filtered : filtered.toReversed();
    for (const effect of ordered) {
      const { expectedCurrent, target, opLabel } = effectRowMode(effect, direction);
      const isSystem = isSystemSideEffectTable(effect.tableName);
      if (isSystem && systemPolicy === "reconcile") {
        await applySystemRowEffectReconciled(executor, effect.tableName, effect.pk, target);
        continue;
      }

      const current = await fetchObservedRowByPk(executor, effect.tableName, effect.pk);
      if (rowHash(current) !== rowHash(expectedCurrent)) {
        throw projectionFailure(
          `Observed row mismatch during ${opLabel} on ${effect.tableName} (pk=${canonicalJson(effect.pk)})`,
        );
      }

      if (!target) {
        await deleteByPk(executor, effect.tableName, effect.pk);
        continue;
      }
      if (!current) {
        await insertEncodedRow(executor, effect.tableName, target);
        continue;
      }
      await updateEncodedRow(executor, effect.tableName, effect.pk, target);
    }
  } finally {
    if (droppedTriggers) {
      await restoreDroppedTriggers(executor, droppedTriggers);
    }
  }
}

function orderSchemaEffectsForReplay(effects: SchemaEffect[], direction: ReplayDirection): SchemaEffect[] {
  if (effects.length <= 1) {
    return effects;
  }

  const compareTableNames = (left: string, right: string): number => {
    const leftSystem = isSystemSideEffectTable(left);
    const rightSystem = isSystemSideEffectTable(right);
    if (leftSystem !== rightSystem) {
      return leftSystem ? -1 : 1;
    }
    return left.localeCompare(right);
  };

  const dependencyOrder = (
    tables: string[],
    tableRefs: Map<string, string[]>,
    mode: "parent-first" | "child-first",
  ): string[] => {
    const tableSet = new Set(tables);
    const outgoing = new Map<string, string[]>();
    for (const table of tables) {
      const refs = (tableRefs.get(table) ?? []).filter((ref) => tableSet.has(ref));
      outgoing.set(table, refs.sort(compareTableNames));
    }

    const temporary = new Set<string>();
    const permanent = new Set<string>();
    const parentFirst: string[] = [];

    const visit = (table: string): void => {
      if (permanent.has(table) || temporary.has(table)) {
        return;
      }
      temporary.add(table);
      for (const ref of outgoing.get(table) ?? []) {
        visit(ref);
      }
      temporary.delete(table);
      permanent.add(table);
      parentFirst.push(table);
    };

    for (const table of [...tables].sort(compareTableNames)) {
      visit(table);
    }

    return mode === "parent-first" ? parentFirst : [...parentFirst].reverse();
  };

  const byTable = new Map<string, SchemaEffect>();
  for (const effect of effects) {
    byTable.set(effect.tableName, effect);
  }

  const restoreRefs = new Map<string, string[]>();
  const restoreTables: string[] = [];
  const dropRefs = new Map<string, string[]>();
  const dropTables: string[] = [];

  for (const effect of effects) {
    const target = direction === "forward" ? effect.afterTable : effect.beforeTable;
    if (target) {
      restoreTables.push(effect.tableName);
      restoreRefs.set(effect.tableName, target.references);
      continue;
    }
    const current = direction === "forward" ? effect.beforeTable : effect.afterTable;
    dropTables.push(effect.tableName);
    dropRefs.set(effect.tableName, current?.references ?? []);
  }

  const restoreOrdered = dependencyOrder(restoreTables, restoreRefs, "parent-first");
  const dropOrdered = dependencyOrder(dropTables, dropRefs, "child-first");
  return [...restoreOrdered, ...dropOrdered]
    .map((tableName) => byTable.get(tableName))
    .filter((effect): effect is SchemaEffect => effect !== undefined);
}

async function captureSqliteSequenceSnapshot(
  executor: SqlExecutor,
  tableName: string,
): Promise<{ seqLiteral: string } | null> {
  if (!(await remoteTableExists(executor, SQLITE_SEQUENCE_TABLE))) {
    return null;
  }
  const result = await executor.execute({
    sql: "SELECT quote(seq) AS seq_literal FROM sqlite_sequence WHERE name = ? LIMIT 1",
    args: [tableName],
  });
  const row = rowsFrom(result)[0];
  if (!row) {
    return null;
  }
  return { seqLiteral: parseString(parseRowValue(row, "seq_literal"), "sqlite_sequence.seq_literal") };
}

async function restoreSqliteSequenceSnapshot(
  executor: SqlExecutor,
  tableName: string,
  snapshot: { seqLiteral: string } | null,
): Promise<void> {
  if (!snapshot || !(await remoteTableExists(executor, SQLITE_SEQUENCE_TABLE))) {
    return;
  }
  await executor.execute({
    sql: "DELETE FROM sqlite_sequence WHERE name = ?",
    args: [tableName],
  });
  await executor.execute(`INSERT INTO sqlite_sequence(name, seq) VALUES (${pragmaLiteral(tableName)}, ${snapshot.seqLiteral})`);
}

function encodeCellSqlLiteral(cell: EncodedCell | undefined, label: string): string {
  if (!cell) {
    throw projectionFailure(`Encoded cell is missing: ${label}`);
  }
  return cell.sqlLiteral;
}

async function restoreTableSnapshot(
  executor: SqlExecutor,
  tableName: string,
  snapshot: NonNullable<SchemaEffect["afterTable"]>,
): Promise<void> {
  const tmpTable = `__toss_restore_${tableName}_${crypto.randomUUID().replaceAll("-", "")}`;
  const quotedTmp = quoteIdentifier(tmpTable, { unsafe: true });
  const quotedTable = quoteIdentifier(tableName, { unsafe: true });
  const sequenceSnapshot = await captureSqliteSequenceSnapshot(executor, tableName);

  await executor.execute(rewriteCreateTableName(snapshot.ddlSql, tmpTable));

  const firstRow = snapshot.rows[0];
  if (firstRow) {
    const columns = Object.keys(firstRow).sort((a, b) => a.localeCompare(b));
    if (columns.length === 0) {
      throw projectionFailure(`Snapshot row must include at least one column for table ${tableName}`);
    }
    const expectedColumns = new Set(columns);
    const columnSql = columns.map((column) => quoteIdentifier(column, { unsafe: true })).join(", ");

    for (const row of snapshot.rows) {
      const rowColumns = Object.keys(row);
      if (rowColumns.length !== columns.length || rowColumns.some((column) => !expectedColumns.has(column))) {
        throw projectionFailure(`Snapshot row columns do not match table snapshot for table ${tableName}`);
      }
      const valueSql = columns
        .map((column) => encodeCellSqlLiteral(row[column], `${tableName}.${column}`))
        .join(", ");
      await executor.execute(`INSERT INTO ${quotedTmp} (${columnSql}) VALUES (${valueSql})`);
    }
  }

  await executor.execute(`DROP TABLE IF EXISTS ${quotedTable}`);
  await executor.execute(`ALTER TABLE ${quotedTmp} RENAME TO ${quotedTable}`);
  await restoreSqliteSequenceSnapshot(executor, tableName, sequenceSnapshot);

  for (const object of snapshot.secondaryObjects) {
    await executor.execute(object.sql);
  }
}

async function applySingleSchemaEffect(executor: SqlExecutor, effect: SchemaEffect, direction: ReplayDirection): Promise<void> {
  const target = direction === "forward" ? effect.afterTable : effect.beforeTable;
  if (!target) {
    await executor.execute(`DROP TABLE ${quoteIdentifier(effect.tableName, { unsafe: true })}`);
    return;
  }
  await restoreTableSnapshot(executor, effect.tableName, target);
}

async function canApplyUserRowEffectNow(executor: SqlExecutor, effect: RowEffect): Promise<boolean> {
  if (isSystemSideEffectTable(effect.tableName)) {
    return false;
  }
  if (!(await remoteTableExists(executor, effect.tableName))) {
    return false;
  }
  return (await missingReferencedTables(executor, effect.tableName)).length === 0;
}

async function applyUserRowAndSchemaEffects(
  executor: SqlExecutor,
  rowEffects: RowEffect[],
  schemaEffects: SchemaEffect[],
  direction: ReplayDirection,
  options: { disableTableTriggers: boolean },
): Promise<void> {
  const pendingRows = (direction === "forward" ? rowEffects : rowEffects.toReversed()).filter(
    (effect) => !isSystemSideEffectTable(effect.tableName),
  );
  const orderedSchemas = orderSchemaEffectsForReplay(schemaEffects, direction);
  let schemaIndex = 0;

  while (pendingRows.length > 0 || schemaIndex < orderedSchemas.length) {
    while (pendingRows.length > 0 && (await canApplyUserRowEffectNow(executor, pendingRows[0]!))) {
      await applyRowEffectsWithOptions(executor, [pendingRows.shift()!], direction, {
        disableTableTriggers: options.disableTableTriggers,
        includeUserEffects: true,
        includeSystemEffects: false,
      });
    }

    if (pendingRows.length === 0) {
      if (schemaIndex < orderedSchemas.length) {
        await applySingleSchemaEffect(executor, orderedSchemas[schemaIndex]!, direction);
        schemaIndex += 1;
        continue;
      }
      break;
    }

    if (schemaIndex < orderedSchemas.length) {
      await applySingleSchemaEffect(executor, orderedSchemas[schemaIndex]!, direction);
      schemaIndex += 1;
      continue;
    }

    const blocked = pendingRows[0]!;
    if (!(await remoteTableExists(executor, blocked.tableName))) {
      throw projectionFailure(`Observed row effect blocked because table does not exist: ${blocked.tableName}`);
    }
    const missingRefs = await missingReferencedTables(executor, blocked.tableName);
    if (missingRefs.length > 0) {
      throw projectionFailure(
        `Observed row effect blocked by missing referenced table(s): ${blocked.tableName} -> ${missingRefs.join(", ")}`,
      );
    }

    await applyRowEffectsWithOptions(executor, [blocked], direction, {
      disableTableTriggers: options.disableTableTriggers,
      includeUserEffects: true,
      includeSystemEffects: false,
    });
    pendingRows.shift();
  }
}

async function assertNoForeignKeyViolations(executor: SqlExecutor, context: string): Promise<void> {
  const result = await executor.execute("PRAGMA foreign_key_check");
  const rows = rowsFrom(result);
  if (rows.length === 0) {
    return;
  }
  const first = rows[0]!;
  const table = parseString(parseRowValue(first, "table"), "foreign_key_check.table");
  const rowid = parseInteger(parseRowValue(first, "rowid"), "foreign_key_check.rowid");
  const parent = parseString(parseRowValue(first, "parent"), "foreign_key_check.parent");
  const fkid = parseInteger(parseRowValue(first, "fkid"), "foreign_key_check.fkid");
  throw projectionFailure(`${context}: foreign_key_check failed at ${table} rowid=${rowid} parent=${parent} fk=${fkid}`);
}

async function assertStructuralIntegrity(executor: SqlExecutor, context: string): Promise<void> {
  const result = await executor.execute("PRAGMA quick_check(1)");
  const row = rowsFrom(result)[0];
  if (!row) {
    throw projectionFailure(`${context}: quick_check returned no rows`);
  }
  const values = Object.values(parseRemoteRow(row));
  const first = values[0];
  if (first === "ok") {
    return;
  }
  throw projectionFailure(`${context}: quick_check returned ${String(first)}`);
}

async function rebuildProjectionFromCanonicalHistory(executor: SqlExecutor, headSeqInclusive: number): Promise<void> {
  const userTables = await listRemoteUserTables(executor);

  await executor.execute("PRAGMA foreign_keys=OFF");
  try {
    for (const tableName of userTables) {
      await executor.execute(`DROP TABLE IF EXISTS ${quoteIdentifier(tableName, { unsafe: true })}`);
    }
  } finally {
    await executor.execute("PRAGMA foreign_keys=ON");
  }

  await setRemoteMetaValue(executor, LAST_MATERIALIZED_COMMIT_META_KEY, "");
  await setRemoteMetaValue(executor, LAST_MATERIALIZED_ERROR_META_KEY, "");

  const replayInputs = await fetchRemoteReplayInputsInSeqRange(executor, 0, headSeqInclusive);
  for (const replay of replayInputs) {
    await materializeSingleCommit(executor, replay);
    await writeMaterializedCheckpoint(executor, replay.commitId);
  }
  if (replayInputs.length === 0) {
    await writeMaterializedCheckpoint(executor, null);
  }
}

export async function materializeSingleCommit(executor: SqlExecutor, replay: CommitReplayInput): Promise<void> {
  await runProjectionStep(async () => {
    const beforeSchemaHash = await remoteSchemaHash(executor);
    if (beforeSchemaHash !== replay.schemaHashBefore) {
      throw projectionFailure(
        `schema_hash_before mismatch for replay ${replay.commitId}: expected ${replay.schemaHashBefore}, got ${beforeSchemaHash}`,
      );
    }

    const computedPlanHash = sha256Hex(replay.operations);
    if (computedPlanHash !== replay.planHash) {
      throw projectionFailure(
        `plan_hash mismatch for replay ${replay.commitId}: expected ${replay.planHash}, got ${computedPlanHash}`,
      );
    }

    await applyUserRowAndSchemaEffects(executor, replay.rowEffects, replay.schemaEffects, "forward", {
      disableTableTriggers: true,
    });
    await applyRowEffectsWithOptions(executor, replay.rowEffects, "forward", {
      disableTableTriggers: true,
      includeUserEffects: false,
      includeSystemEffects: true,
      systemPolicy: "reconcile",
    });
    await assertNoForeignKeyViolations(executor, `replay ${replay.commitId}`);
    await assertStructuralIntegrity(executor, `replay ${replay.commitId}`);

    const afterSchemaHash = await remoteSchemaHash(executor);
    if (afterSchemaHash !== replay.schemaHashAfter) {
      throw projectionFailure(
        `schema_hash_after mismatch for replay ${replay.commitId}: expected ${replay.schemaHashAfter}, got ${afterSchemaHash}`,
      );
    }

    const afterStateHash = await remoteStateHash(executor);
    if (afterStateHash !== replay.stateHashAfter) {
      throw projectionFailure(
        `state_hash_after mismatch for replay ${replay.commitId}: expected ${replay.stateHashAfter}, got ${afterStateHash}`,
      );
    }
  }, `replay ${replay.commitId}`);
}

export async function materializeRemoteToHead(client: Client): Promise<void> {
  const tx = await client.transaction("write");
  try {
    await tx.execute("PRAGMA foreign_keys=ON");
    await tx.execute("PRAGMA defer_foreign_keys=ON");

    const remoteHead = await fetchRemoteHead(tx);
    const checkpoint = remoteHead.commitId
      ? normalizeMetaString(await getRemoteMetaValue(tx, LAST_MATERIALIZED_COMMIT_META_KEY))
      : null;
    const checkpointSeq = checkpoint ? await remoteCommitSeq(tx, checkpoint) : null;
    const needsRebuild = !remoteHead.commitId || !checkpoint || checkpointSeq === null || checkpointSeq > remoteHead.seq;

    if (needsRebuild) {
      await runProjectionStep(() => rebuildProjectionFromCanonicalHistory(tx, remoteHead.seq), "rebuild projection");
      await tx.commit();
      return;
    }

    await runProjectionStep(() => verifyProjectionAtCommit(tx, checkpoint), `verify checkpoint ${checkpoint}`);

    const replayInputs = await fetchRemoteInputsAfterSeq(tx, checkpointSeq, remoteHead);
    for (const replay of replayInputs) {
      await materializeSingleCommit(tx, replay);
      await writeMaterializedCheckpoint(tx, replay.commitId);
    }

    await setRemoteMetaValue(tx, LAST_MATERIALIZED_AT_META_KEY, String(Date.now()));
    await setRemoteMetaValue(tx, LAST_MATERIALIZED_ERROR_META_KEY, "");
    await tx.commit();
  } catch (error) {
    const shouldWrapProjectionError =
      projectionErrorMessage(error) !== null || CodedError.hasCode(error, "SYNC_DIVERGED");
    const projectionError = shouldWrapProjectionError ? toProjectionError(error, "materialize to head") : null;
    try {
      await tx.rollback();
    } catch {
      // no-op
    }
    if (projectionError) {
      await persistMaterializationErrorBestEffort(client, projectionError);
      throw projectionError;
    }
    throw error;
  } finally {
    tx.close();
  }
}

export async function fetchRemoteProjectionStatus(
  executor: SqlExecutor,
  remoteHeadInput?: RemoteHead,
): Promise<RemoteProjectionStatus> {
  const remoteHead = remoteHeadInput ?? (await fetchRemoteHead(executor));
  const projectionHead = normalizeMetaString(await getRemoteMetaValue(executor, LAST_MATERIALIZED_COMMIT_META_KEY));
  const recordedError = normalizeMetaString(await getRemoteMetaValue(executor, LAST_MATERIALIZED_ERROR_META_KEY));

  if (!remoteHead.commitId) {
    return {
      projectionHead,
      projectionLagCommits: 0,
      projectionError: recordedError,
    };
  }

  let projectionSeq: number | null = null;
  if (projectionHead) {
    projectionSeq = await remoteCommitSeq(executor, projectionHead);
  }

  const projectionAheadOfHead = projectionSeq !== null && projectionSeq > remoteHead.seq;
  let projectionLagCommits: number;
  if (projectionSeq === null) {
    projectionLagCommits = remoteHead.seq;
  } else if (projectionAheadOfHead) {
    projectionLagCommits = 0;
  } else {
    projectionLagCommits = remoteHead.seq - projectionSeq;
  }
  let projectionError = projectionHead && projectionSeq === null
    ? `Materialization checkpoint is missing from canonical history: ${projectionHead}`
    : recordedError;
  if (projectionAheadOfHead) {
    projectionError = `Materialization checkpoint is ahead of remote HEAD: checkpoint=${projectionHead} seq=${projectionSeq} head=${remoteHead.commitId} seq=${remoteHead.seq}`;
  } else if (projectionHead && projectionSeq !== null) {
    try {
      await verifyProjectionAtCommit(executor, projectionHead);
    } catch (error) {
      projectionError = toProjectionError(error, `verify projection status ${projectionHead}`).message;
    }
  }

  return {
    projectionHead,
    projectionLagCommits,
    projectionError,
  };
}

async function insertReplayIntoRemote(tx: Transaction, replay: CommitReplayInput): Promise<void> {
  await tx.execute({
    sql: `
      INSERT INTO _toss_commit(
        commit_id, seq, kind, message, created_at, parent_count,
        schema_hash_before, schema_hash_after, state_hash_after, plan_hash,
        revertible, revert_target_id
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
      replay.revertible,
      replay.revertTargetId,
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
    const beforeJson = effect.beforeRow ? canonicalJson(effect.beforeRow) : null;
    const afterJson = effect.afterRow ? canonicalJson(effect.afterRow) : null;
    await tx.execute({
      sql: `
        INSERT INTO _toss_row_effect(
          commit_id, effect_index, table_name, pk_json, op_kind,
          before_json, after_json, before_hash, after_hash
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
        beforeJson,
        afterJson,
        beforeJson ? sha256Hex(beforeJson) : null,
        afterJson ? sha256Hex(afterJson) : null,
      ],
    });
  }

  for (let i = 0; i < replay.schemaEffects.length; i += 1) {
    const effect = replay.schemaEffects[i]!;
    await tx.execute({
      sql: `
        INSERT INTO _toss_schema_effect(
          commit_id, effect_index, table_name, before_json, after_json
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

export async function pushCommit(
  client: Client,
  replay: CommitReplayInput,
  expectedRemoteHead: string | null,
): Promise<void> {
  const tx = await client.transaction("write");
  try {
    await tx.execute("PRAGMA foreign_keys=ON");
    await tx.execute("PRAGMA defer_foreign_keys=ON");

    const currentHead = await fetchRemoteHead(tx);
    if (currentHead.commitId !== expectedRemoteHead) {
      throw new CodedError(
        "SYNC_NON_FAST_FORWARD",
        `Remote HEAD changed during push. expected=${expectedRemoteHead ?? "null"} actual=${currentHead.commitId ?? "null"}`,
      );
    }

    await insertReplayIntoRemote(tx, replay);
    await runProjectionStep(async () => {
      await materializeSingleCommit(tx, replay);
      await writeMaterializedCheckpoint(tx, replay.commitId);
    }, `materialize pushed commit ${replay.commitId}`);

    const update = await tx.execute({
      sql: `
        UPDATE _toss_ref
        SET commit_id = ?, updated_at = ?
        WHERE name = ? AND ((? IS NULL AND commit_id IS NULL) OR commit_id = ?)
      `,
      args: [replay.commitId, replay.createdAt, MAIN_REF_NAME, expectedRemoteHead, expectedRemoteHead],
    });
    if (update.rowsAffected !== 1) {
      throw new CodedError(
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
    await persistMaterializationErrorBestEffort(client, error);
    throw error;
  } finally {
    tx.close();
  }
}

async function fetchRemoteReplayInput(executor: SqlExecutor, commitId: string): Promise<CommitReplayInput> {
  const commitResult = await executor.execute({
    sql: `
      SELECT
        commit_id, seq, kind, message, created_at, parent_count,
        schema_hash_before, schema_hash_after, state_hash_after, plan_hash,
        revertible, revert_target_id
      FROM _toss_commit
      WHERE commit_id = ?
      LIMIT 1
    `,
    args: [commitId],
  });
  const commitRow = rowsFrom(commitResult)[0];
  if (!commitRow) {
    throw new CodedError("SYNC_DIVERGED", `Remote commit not found during pull: ${commitId}`);
  }

  const parentsResult = await executor.execute({
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

  const opsResult = await executor.execute({
    sql: `
      SELECT op_json
      FROM _toss_op
      WHERE commit_id = ?
      ORDER BY op_index ASC
    `,
    args: [commitId],
  });
  const operations = rowsFrom(opsResult).map((row) => parseJson<Operation>(parseRowValue(row, "op_json"), "_toss_op.op_json"));

  const rowEffectsResult = await executor.execute({
    sql: `
      SELECT table_name, pk_json, op_kind, before_json, after_json
      FROM _toss_row_effect
      WHERE commit_id = ?
      ORDER BY effect_index ASC
    `,
    args: [commitId],
  });
  const rowEffects = rowsFrom(rowEffectsResult).map((row) => ({
    tableName: parseString(parseRowValue(row, "table_name"), "_toss_row_effect.table_name"),
    pk: parseJson<Record<string, string>>(parseRowValue(row, "pk_json"), "_toss_row_effect.pk_json"),
    opKind: parseOpKind(parseString(parseRowValue(row, "op_kind"), "_toss_row_effect.op_kind")),
    beforeRow: parseRowValue(row, "before_json")
      ? parseJson<CommitReplayInput["rowEffects"][number]["beforeRow"]>(parseRowValue(row, "before_json"), "_toss_row_effect.before_json")
      : null,
    afterRow: parseRowValue(row, "after_json")
      ? parseJson<CommitReplayInput["rowEffects"][number]["afterRow"]>(parseRowValue(row, "after_json"), "_toss_row_effect.after_json")
      : null,
  }));

  const schemaEffectsResult = await executor.execute({
    sql: `
      SELECT table_name, before_json, after_json
      FROM _toss_schema_effect
      WHERE commit_id = ?
      ORDER BY effect_index ASC
    `,
    args: [commitId],
  });
  const schemaEffects = rowsFrom(schemaEffectsResult).map((row) => ({
    tableName: parseString(parseRowValue(row, "table_name"), "_toss_schema_effect.table_name"),
    beforeTable: parseRowValue(row, "before_json")
      ? parseJson<CommitReplayInput["schemaEffects"][number]["beforeTable"]>(parseRowValue(row, "before_json"), "_toss_schema_effect.before_json")
      : null,
    afterTable: parseRowValue(row, "after_json")
      ? parseJson<CommitReplayInput["schemaEffects"][number]["afterTable"]>(parseRowValue(row, "after_json"), "_toss_schema_effect.after_json")
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
    revertible: parseInteger(parseRowValue(commitRow, "revertible"), "_toss_commit.revertible"),
    revertTargetId: parseNullableString(parseRowValue(commitRow, "revert_target_id"), "_toss_commit.revert_target_id"),
    operations,
    rowEffects,
    schemaEffects,
  };
}

async function fetchRemoteReplayInputsInSeqRange(
  executor: SqlExecutor,
  fromSeqExclusive: number,
  toSeqInclusive: number,
): Promise<CommitReplayInput[]> {
  if (toSeqInclusive <= fromSeqExclusive) {
    return [];
  }
  const result = await executor.execute({
    sql: `
      SELECT commit_id
      FROM _toss_commit
      WHERE seq > ? AND seq <= ?
      ORDER BY seq ASC
    `,
    args: [fromSeqExclusive, toSeqInclusive],
  });
  const commitIds = rowsFrom(result).map((row) => parseString(parseRowValue(row, "commit_id"), "_toss_commit.commit_id"));
  const replayInputs: CommitReplayInput[] = [];
  for (const commitId of commitIds) {
    replayInputs.push(await fetchRemoteReplayInput(executor, commitId));
  }
  return replayInputs;
}

export async function fetchRemoteInputsAfterSeq(
  executor: SqlExecutor,
  fromSeqExclusive: number,
  remoteHeadInput?: RemoteHead,
): Promise<CommitReplayInput[]> {
  const remoteHead = remoteHeadInput ?? (await fetchRemoteHead(executor));
  if (!remoteHead.commitId) {
    return [];
  }
  return fetchRemoteReplayInputsInSeqRange(executor, fromSeqExclusive, remoteHead.seq);
}
