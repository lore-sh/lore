import { type Client, type InArgs, type ResultSet, type Row, type Transaction } from "@libsql/client";
import { createClient as createWebClient } from "@libsql/client/web";
import type { readCommit } from "./commit";
import { readAuthToken } from "./config";
import {
  COMMIT_PARENT_TABLE,
  COMMIT_TABLE,
  LAST_MATERIALIZED_AT_META_KEY,
  LAST_MATERIALIZED_COMMIT_META_KEY,
  LAST_MATERIALIZED_ERROR_META_KEY,
  MAIN_REF_NAME,
  META_TABLE,
  OP_TABLE,
  PRESERVED_META_DEFAULTS,
  REF_TABLE,
  ROW_EFFECT_TABLE,
  SCHEMA_EFFECT_TABLE,
  SYNC_PROTOCOL_VERSION_META_KEY,
  normalizeMetaString,
} from "./db";
import { CodedError } from "./error";
import { dependencyOrder, RowEffect, SchemaEffect } from "./effect";
import { canonicalJson, sha256Hex } from "./hash";
import { hashSchema } from "./inspect";
import { DRIZZLE_MIGRATIONS_TABLE, DRIZZLE_MIGRATIONS_TABLE_SQL, loadEngineMigrations, pendingEngineMigrations, type EngineMigration } from "./migration";
import { Operation, type Operation as Op } from "./operation";
import type { EncodedCell, EncodedRow } from "./schema";
import {
  buildPkWhereClause,
  buildRowSelectSql,
  extractCheckConstraints,
  normalizeSqlNullable,
  parseColumnDefinitionsFromCreateTable,
  pragmaLiteral,
  quoteIdentifier,
  rewriteCreateTableName,
} from "./sql";
import { stateHashForRemote } from "./state";
import type { RemotePlatform } from "./sync";

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

type ReplayDirection = "forward" | "inverse";

const SENSITIVE_QUERY_KEYS = [
  "token",
  "auth",
  "auth_token",
  "authorization",
  "access_token",
  "refresh_token",
  "api_key",
  "apikey",
  "secret",
  "password",
  "passwd",
] as const;
const SUPPORTED_REMOTE_SCHEMES = new Set(["https:", "libsql:"]);

function isSensitiveQueryKey(key: string): boolean {
  const normalized = key.toLowerCase();
  return SENSITIVE_QUERY_KEYS.some((candidate) => normalized.includes(candidate));
}

export function maskSensitiveUrl(input: string): string {
  if (!URL.canParse(input)) {
    return input;
  }
  const parsed = new URL(input);
  let changed = false;
  if (parsed.username) {
    parsed.username = "[REDACTED]";
    changed = true;
  }
  if (parsed.password) {
    parsed.password = "[REDACTED]";
    changed = true;
  }
  for (const key of Array.from(parsed.searchParams.keys())) {
    if (!isSensitiveQueryKey(key)) {
      continue;
    }
    parsed.searchParams.set(key, "[REDACTED]");
    changed = true;
  }
  return changed ? parsed.toString() : input;
}

export function maskSensitiveText(input: string): string {
  let output = input;
  output = output.replace(/\b(?:https?|libsql|wss?):\/\/[^\s"'`]+/gi, (url) => maskSensitiveUrl(url));
  output = output.replace(
    /"(auth[_-]?token|access[_-]?token|refresh[_-]?token|token|authorization|api[_-]?key|password)"\s*:\s*"[^"]*"/gi,
    '"$1":"[REDACTED]"',
  );
  output = output.replace(
    /\b(auth[_-]?token|access[_-]?token|refresh[_-]?token|token|authorization|api[_-]?key|password)\b(\s*[:=]\s*)([^,\s;]+)/gi,
    (_match, key, sep) => `${key}${sep}[REDACTED]`,
  );
  output = output.replace(/\bBearer\s+[A-Za-z0-9._~+/-]+=*/gi, "Bearer [REDACTED]");
  return output;
}

export function validateRemoteUrl(input: string): string {
  const normalized = input.trim();
  if (normalized.length === 0) {
    throw new CodedError("CONFIG", "Remote URL must not be empty");
  }
  if (!URL.canParse(normalized)) {
    throw new CodedError("CONFIG", `Remote URL is invalid: ${normalized}`);
  }
  const parsed = new URL(normalized);
  if (!SUPPORTED_REMOTE_SCHEMES.has(parsed.protocol)) {
    throw new CodedError("CONFIG", `Remote URL scheme is not supported: ${parsed.protocol}. Use https:// or libsql://.`);
  }
  return normalized;
}

function parseRemoteRow(row: Row): Record<string, unknown> {
  if (!row || typeof row !== "object" || Array.isArray(row)) {
    throw new CodedError("SYNC_DIVERGED", "Remote row payload is invalid");
  }
  return row as Record<string, unknown>;
}

export function parseRemoteDbName(remoteUrl: string): string | null {
  if (!URL.canParse(remoteUrl)) {
    return null;
  }
  const parsed = new URL(remoteUrl);
  const host = parsed.hostname.trim();
  if (!host) {
    return null;
  }
  const [name] = host.split(".");
  return name?.trim().length ? name.trim() : null;
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

const MAX_SAFE_INTEGER_BIGINT = BigInt(Number.MAX_SAFE_INTEGER);
const MIN_SAFE_INTEGER_BIGINT = BigInt(Number.MIN_SAFE_INTEGER);

function parseSafeBigInt(value: bigint, label: string): number {
  if (value > MAX_SAFE_INTEGER_BIGINT || value < MIN_SAFE_INTEGER_BIGINT) {
    throw new CodedError("SYNC_DIVERGED", `Remote ${label} is outside JavaScript safe integer range`);
  }
  return Number(value);
}

function parseInteger(value: unknown, label: string): number {
  if (typeof value === "number") {
    if (!Number.isInteger(value) || !Number.isSafeInteger(value)) {
      throw new CodedError("SYNC_DIVERGED", `Remote ${label} is not a safe integer`);
    }
    return value;
  }
  if (typeof value === "bigint") {
    return parseSafeBigInt(value, label);
  }
  if (typeof value === "string") {
    const normalized = value.trim();
    if (!/^[+-]?\d+$/.test(normalized)) {
      throw new CodedError("SYNC_DIVERGED", `Remote ${label} is not an integer`);
    }
    return parseSafeBigInt(BigInt(normalized), label);
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

function parseJson(value: unknown, label: string): unknown {
  const text = parseString(value, label);
  return JSON.parse(text) as unknown;
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
    return `${error.code}: ${maskSensitiveText(error.message)}`;
  }
  if (error instanceof Error) {
    return maskSensitiveText(error.message);
  }
  return maskSensitiveText(String(error));
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
    const sanitized = maskSensitiveText(error.message);
    if (sanitized === error.message) {
      return error;
    }
    return new CodedError(error.code, sanitized, { cause: error });
  }
  if (error instanceof Error && (error.name === "SyntaxError" || error.name === "ZodError")) {
    return new CodedError("SYNC_DIVERGED", maskSensitiveText(error.message), { cause: error });
  }
  if (error instanceof Error) {
    const rawMessage = error.message;
    const normalized = rawMessage.toLowerCase();
    const sanitized = maskSensitiveText(rawMessage);
    if (normalized.includes("unauthorized") || normalized.includes("401") || normalized.includes("auth")) {
      return new CodedError("SYNC_AUTH_FAILED", sanitized, { cause: error });
    }
    return new CodedError("SYNC_UNREACHABLE", sanitized, { cause: error });
  }
  return new CodedError("SYNC_UNREACHABLE", maskSensitiveText(String(error)));
}

export async function readRemoteSyncProtocolVersion(executor: Client | Transaction): Promise<string | null> {
  const raw = await getRemoteMetaValue(executor, SYNC_PROTOCOL_VERSION_META_KEY);
  return normalizeMetaString(raw);
}

export function remoteConfigForDisplay(config: { platform: RemotePlatform; remoteUrl: string; remoteDbName: string | null }) {
  return {
    platform: config.platform,
    remoteUrl: maskSensitiveUrl(config.remoteUrl),
    remoteDbName: config.remoteDbName,
  };
}

export function syncErrorForDisplay(message: string | null): string | null {
  if (message === null) {
    return null;
  }
  return maskSensitiveText(message);
}

export function normalizeToken(token: string | null | undefined): string | undefined {
  if (!token) {
    return undefined;
  }
  const trimmed = token.trim();
  return trimmed.length === 0 ? undefined : trimmed;
}

export function authTokenForPlatform(
  config: { platform: RemotePlatform; remoteUrl: string; remoteDbName: string | null },
  override?: string | null,
): string | undefined {
  if (override === null) {
    return undefined;
  }
  const fromOverride = normalizeToken(override);
  if (fromOverride) {
    return fromOverride;
  }
  return readAuthToken(config.platform);
}

export function openRemoteClient(
  config: { platform: RemotePlatform; remoteUrl: string; remoteDbName: string | null },
  authTokenOverride?: string | null,
): Client {
  const authToken = authTokenForPlatform(config, authTokenOverride);
  return createWebClient(authToken ? { url: config.remoteUrl, authToken } : { url: config.remoteUrl });
}

function rowsFrom(result: ResultSet): Row[] {
  return result.rows as Row[];
}

async function remoteTableExists(executor: Client | Transaction, tableName: string): Promise<boolean> {
  const result = await executor.execute({
    sql: "SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
    args: [tableName],
  });
  return rowsFrom(result).length > 0;
}

export async function detectRemoteReadState(client: Client): Promise<"initialized" | "empty"> {
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
      `Remote database has an incomplete lore schema (missing: ${missing.join(", ")}). Recreate remote schema with write access.`,
    );
  }
  return "initialized";
}

function metaInsertStatement(key: string, value: string): { sql: string; args: InArgs } {
  return {
    sql: "INSERT INTO _lore_meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO NOTHING",
    args: [key, value],
  };
}

async function ensureRemoteMigrationsTable(client: Client): Promise<void> {
  await client.execute(DRIZZLE_MIGRATIONS_TABLE_SQL);
}

async function readRemoteAppliedMigrationHashesById(client: Client): Promise<Map<string, string>> {
  const result = await client.execute(`SELECT id, hash FROM "${DRIZZLE_MIGRATIONS_TABLE}"`);
  const applied = new Map<string, string>();
  for (const row of rowsFrom(result)) {
    const id = parseRowValue(row, "id");
    const hash = parseRowValue(row, "hash");
    if (typeof id !== "string" || typeof hash !== "string") {
      throw new CodedError(
        "CONFIG",
        `Remote database uses an unsupported ${DRIZZLE_MIGRATIONS_TABLE} format. Recreate remote schema with write access.`,
      );
    }
    applied.set(id, hash);
  }
  return applied;
}

async function applyRemoteMigration(client: Client, migration: EngineMigration): Promise<void> {
  const tx = await client.transaction("write");
  try {
    await tx.executeMultiple(migration.sql);
    await tx.execute({
      sql: `INSERT INTO "${DRIZZLE_MIGRATIONS_TABLE}" (id, hash, created_at) VALUES (?, ?, ?)`,
      args: [migration.id, migration.hash, Date.now()],
    });
    await tx.commit();
  } catch (error) {
    await tx.rollback();
    throw error;
  }
}

async function applyRemoteEngineMigrations(client: Client): Promise<void> {
  await ensureRemoteMigrationsTable(client);
  const migrations = await loadEngineMigrations();
  const appliedById = await readRemoteAppliedMigrationHashesById(client);
  const pending = pendingEngineMigrations(migrations, appliedById);
  for (const migration of pending) {
    await applyRemoteMigration(client, migration);
  }
}

async function getRemoteMetaValue(executor: Client | Transaction, key: string): Promise<string | null> {
  const result = await executor.execute({
    sql: "SELECT value FROM _lore_meta WHERE key = ? LIMIT 1",
    args: [key],
  });
  const row = rowsFrom(result)[0];
  if (!row) {
    return null;
  }
  return parseString(parseRowValue(row, "value"), "_lore_meta.value");
}

async function setRemoteMetaValue(executor: Client | Transaction, key: string, value: string): Promise<void> {
  await executor.execute({
    sql: `
      INSERT INTO _lore_meta(key, value)
      VALUES (?, ?)
      ON CONFLICT(key) DO UPDATE SET value = excluded.value
    `,
    args: [key, value],
  });
}

async function writeMaterializedCheckpoint(executor: Client | Transaction, commitId: string | null): Promise<void> {
  await setRemoteMetaValue(executor, LAST_MATERIALIZED_COMMIT_META_KEY, commitId ?? "");
  await setRemoteMetaValue(executor, LAST_MATERIALIZED_AT_META_KEY, String(Date.now()));
  await setRemoteMetaValue(executor, LAST_MATERIALIZED_ERROR_META_KEY, "");
}

async function persistMaterializationErrorBestEffort(client: Client, error: unknown): Promise<void> {
  let message = projectionErrorMessage(error);
  if (!message && CodedError.hasCode(error, "SYNC_DIVERGED")) {
    message = error.message;
  } else if (!message && error instanceof Error && (error.name === "SyntaxError" || error.name === "ZodError")) {
    message = toProjectionError(error).message;
  }
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
  await applyRemoteEngineMigrations(client);

  await client.batch(
    [
      ...PRESERVED_META_DEFAULTS.map(([key, value]) => metaInsertStatement(key, value)),
      {
        sql: "INSERT INTO _lore_ref(name, commit_id, updated_at) VALUES (?, NULL, ?) ON CONFLICT(name) DO NOTHING",
        args: [MAIN_REF_NAME, Date.now()],
      },
    ],
    "write",
  );
}

export async function remoteHead(executor: Client | Transaction) {
  const result = await executor.execute({
    sql: `
      SELECT r.commit_id AS commit_id, c.seq AS seq
      FROM _lore_ref AS r
      LEFT JOIN _lore_commit AS c ON c.commit_id = r.commit_id
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

export async function remoteHasCommit(executor: Client | Transaction, commitId: string): Promise<boolean> {
  const result = await executor.execute({
    sql: "SELECT 1 AS ok FROM _lore_commit WHERE commit_id = ? LIMIT 1",
    args: [commitId],
  });
  return rowsFrom(result).length > 0;
}

export async function remoteCommitSeq(executor: Client | Transaction, commitId: string): Promise<number | null> {
  const result = await executor.execute({
    sql: "SELECT seq AS seq FROM _lore_commit WHERE commit_id = ? LIMIT 1",
    args: [commitId],
  });
  const row = rowsFrom(result)[0];
  if (!row) {
    return null;
  }
  return parseInteger(parseRowValue(row, "seq"), "_lore_commit.seq");
}

async function listRemoteUserTables(executor: Client | Transaction): Promise<string[]> {
  const result = await executor.execute({
    sql: "SELECT name FROM sqlite_master WHERE type='table' AND name NOT GLOB '_lore_*' AND name NOT GLOB '__drizzle_*' AND name NOT GLOB 'sqlite_*' ORDER BY name",
  });
  return rowsFrom(result).map((row) => parseString(parseRowValue(row, "name"), "sqlite_master.name"));
}

async function listRemoteUserViews(executor: Client | Transaction): Promise<string[]> {
  const result = await executor.execute({
    sql: "SELECT name FROM sqlite_master WHERE type='view' AND name NOT GLOB '_lore_*' AND name NOT GLOB '__drizzle_*' AND name NOT GLOB 'sqlite_*' ORDER BY name",
  });
  return rowsFrom(result).map((row) => parseString(parseRowValue(row, "name"), "sqlite_master.name"));
}

async function remoteStateHash(executor: Client | Transaction): Promise<string> {
  try {
    return await stateHashForRemote(executor);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw projectionFailure(`state hash failed: ${message}`);
  }
}

async function remoteSchemaHash(executor: Client | Transaction): Promise<string> {
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

  const descriptors: Parameters<typeof hashSchema>[0]["tables"] = [];
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
          notNull: parseInteger(parseRowValue(row, "notnull"), `PRAGMA table_xinfo(${tableName}).notnull`) === 1,
          defaultValue: parseNullableStringLike(parseRowValue(row, "dflt_value"), `PRAGMA table_xinfo(${tableName}).dflt_value`),
          primaryKey: parseInteger(parseRowValue(row, "pk"), `PRAGMA table_xinfo(${tableName}).pk`) > 0,
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

  const viewsResult = await executor.execute("SELECT name, sql FROM sqlite_master WHERE type='view' ORDER BY name ASC");
  const views = rowsFrom(viewsResult).map((row) => ({
    name: parseString(parseRowValue(row, "name"), "view.name"),
    sql: normalizeSqlNullable(parseNullableStringLike(parseRowValue(row, "sql"), "view.sql")),
  }));

  return hashSchema({ tables: descriptors, views });
}

async function verifyProjectionAtCommit(executor: Client | Transaction, commitId: string): Promise<void> {
  const result = await executor.execute({
    sql: "SELECT schema_hash_after, state_hash_after FROM _lore_commit WHERE commit_id = ? LIMIT 1",
    args: [commitId],
  });
  const row = rowsFrom(result)[0];
  if (!row) {
    throw projectionFailure(`checkpoint commit is missing from canonical history: ${commitId}`);
  }
  const expected = {
    schemaHashAfter: parseString(parseRowValue(row, "schema_hash_after"), "_lore_commit.schema_hash_after"),
    stateHashAfter: parseString(parseRowValue(row, "state_hash_after"), "_lore_commit.state_hash_after"),
  };
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

async function remoteTableColumns(executor: Client | Transaction, tableName: string): Promise<string[]> {
  const result = await executor.execute(`PRAGMA table_info(${quoteIdentifier(tableName, { unsafe: true })})`);
  const names = rowsFrom(result).map((row) =>
    parseString(parseRowValue(row, "name"), `PRAGMA table_info(${tableName}).name`),
  );
  if (names.length === 0) {
    throw projectionFailure(`Unable to inspect columns for table ${tableName}`);
  }
  return names;
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

function buildPkWhereClauseOrProjectionFailure(pk: Record<string, string>): string {
  const whereClause = buildPkWhereClause(pk);
  if (!whereClause.ok) {
    throw projectionFailure(whereClause.message);
  }
  return whereClause.whereClause;
}

async function fetchObservedRowByPk(executor: Client | Transaction, tableName: string, pk: Record<string, string>): Promise<EncodedRow | null> {
  if (!(await remoteTableExists(executor, tableName))) {
    if (isSystemSideEffectTable(tableName)) {
      return null;
    }
    throw projectionFailure(`Table does not exist while applying row effects: ${tableName}`);
  }

  const columns = await remoteTableColumns(executor, tableName);
  const keyColumns = Object.keys(pk).sort((a, b) => a.localeCompare(b));
  const quoteAliases = columns.map((_, i) => `__lore_quote_${i}`);
  const hexAliases = columns.map((_, i) => `__lore_hex_${i}`);
  const typeAliases = columns.map((_, i) => `__lore_type_${i}`);
  const whereClause = buildPkWhereClauseOrProjectionFailure(pk);
  const sql = `${buildRowSelectSql(tableName, columns, keyColumns, whereClause)} LIMIT 1`;
  const result = await executor.execute(sql);
  const row = rowsFrom(result)[0];
  if (!row) {
    return null;
  }
  return encodeRowFromRemote(parseRemoteRow(row), columns, quoteAliases, hexAliases, typeAliases);
}

async function insertEncodedRow(executor: Client | Transaction, tableName: string, row: EncodedRow): Promise<void> {
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
  executor: Client | Transaction,
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
    `UPDATE ${quoteIdentifier(tableName, { unsafe: true })} SET ${setSql} WHERE ${buildPkWhereClauseOrProjectionFailure(pk)}`,
  );
}

async function deleteByPk(executor: Client | Transaction, tableName: string, pk: Record<string, string>): Promise<void> {
  await executor.execute(`DELETE FROM ${quoteIdentifier(tableName, { unsafe: true })} WHERE ${buildPkWhereClauseOrProjectionFailure(pk)}`);
}

async function referencedTables(executor: Client | Transaction, tableName: string): Promise<string[]> {
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

async function missingReferencedTables(executor: Client | Transaction, tableName: string): Promise<string[]> {
  const refs = await referencedTables(executor, tableName);
  const missing: string[] = [];
  for (const ref of refs) {
    if (!(await remoteTableExists(executor, ref))) {
      missing.push(ref);
    }
  }
  return missing;
}

async function dropTriggersForTables(executor: Client | Transaction, effects: RowEffect[]) {
  const touched = Array.from(new Set(effects.map((effect) => effect.tableName))).sort((a, b) => a.localeCompare(b));
  const dropped: Array<{ name: string; sql: string }> = [];
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

async function applySystemRowEffectReconciled(
  executor: Client | Transaction,
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

async function applyRowEffects(
  executor: Client | Transaction,
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
      const forward = direction === "forward";
      const { beforeRow, afterRow } = effect;
      let expectedCurrent: EncodedRow | null;
      let target: EncodedRow | null;
      let opLabel: string;
      if (effect.opKind === "insert") {
        expectedCurrent = forward ? null : afterRow;
        target = forward ? afterRow : null;
        opLabel = forward ? "insert" : "inverse-delete";
      } else if (effect.opKind === "update") {
        expectedCurrent = forward ? beforeRow : afterRow;
        target = forward ? afterRow : beforeRow;
        opLabel = forward ? "update" : "inverse-update";
      } else {
        expectedCurrent = forward ? beforeRow : null;
        target = forward ? null : beforeRow;
        opLabel = forward ? "delete" : "inverse-insert";
      }
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
      for (const trigger of droppedTriggers) {
        await executor.execute(trigger.sql);
      }
    }
  }
}

function orderSchemaEffectsForReplay(effects: SchemaEffect[], direction: ReplayDirection): SchemaEffect[] {
  if (effects.length <= 1) {
    return effects;
  }

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
  executor: Client | Transaction,
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
  executor: Client | Transaction,
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
  executor: Client | Transaction,
  tableName: string,
  snapshot: NonNullable<SchemaEffect["afterTable"]>,
): Promise<void> {
  const tmpTable = `__lore_restore_${tableName}_${crypto.randomUUID().replaceAll("-", "")}`;
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

async function applySingleSchemaEffect(executor: Client | Transaction, effect: SchemaEffect, direction: ReplayDirection): Promise<void> {
  const target = direction === "forward" ? effect.afterTable : effect.beforeTable;
  const current = direction === "forward" ? effect.beforeTable : effect.afterTable;
  if (!target) {
    if ((current?.kind ?? "table") === "view") {
      await executor.execute(`DROP VIEW ${quoteIdentifier(effect.tableName, { unsafe: true })}`);
      return;
    }
    await executor.execute(`DROP TABLE ${quoteIdentifier(effect.tableName, { unsafe: true })}`);
    return;
  }
  if (target.kind === "view") {
    await executor.execute(target.ddlSql);
    return;
  }
  await restoreTableSnapshot(executor, effect.tableName, target);
}

async function canApplyUserRowEffectNow(executor: Client | Transaction, effect: RowEffect): Promise<boolean> {
  if (isSystemSideEffectTable(effect.tableName)) {
    return false;
  }
  if (!(await remoteTableExists(executor, effect.tableName))) {
    return false;
  }
  return (await missingReferencedTables(executor, effect.tableName)).length === 0;
}

async function applyEffects(
  executor: Client | Transaction,
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
      await applyRowEffects(executor, [pendingRows.shift()!], direction, {
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

    await applyRowEffects(executor, [blocked], direction, {
      disableTableTriggers: options.disableTableTriggers,
      includeUserEffects: true,
      includeSystemEffects: false,
    });
    pendingRows.shift();
  }
}

async function assertForeignKeys(executor: Client | Transaction, context: string): Promise<void> {
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

async function assertStructuralIntegrity(executor: Client | Transaction, context: string): Promise<void> {
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

async function rebuildProjectionFromCanonicalHistory(executor: Client | Transaction, headSeqInclusive: number): Promise<void> {
  const userTables = await listRemoteUserTables(executor);
  const userViews = await listRemoteUserViews(executor);

  await executor.execute("PRAGMA foreign_keys=OFF");
  try {
    for (const viewName of userViews) {
      await executor.execute(`DROP VIEW IF EXISTS ${quoteIdentifier(viewName, { unsafe: true })}`);
    }
    for (const tableName of userTables) {
      await executor.execute(`DROP TABLE IF EXISTS ${quoteIdentifier(tableName, { unsafe: true })}`);
    }
  } finally {
    await executor.execute("PRAGMA foreign_keys=ON");
  }

  await setRemoteMetaValue(executor, LAST_MATERIALIZED_COMMIT_META_KEY, "");
  await setRemoteMetaValue(executor, LAST_MATERIALIZED_ERROR_META_KEY, "");

  const replayInputs = await fetchCommitRange(executor, 0, headSeqInclusive);
  for (const replay of replayInputs) {
    await materializeCommit(executor, replay);
    await writeMaterializedCheckpoint(executor, replay.commit.commitId);
  }
  if (replayInputs.length === 0) {
    await writeMaterializedCheckpoint(executor, null);
  }
}

export async function materializeCommit(
  executor: Client | Transaction,
  replay: ReturnType<typeof readCommit>,
): Promise<void> {
  await runProjectionStep(async () => {
    const beforeSchemaHash = await remoteSchemaHash(executor);
    if (beforeSchemaHash !== replay.commit.schemaHashBefore) {
      throw projectionFailure(
        `schema_hash_before mismatch for replay ${replay.commit.commitId}: expected ${replay.commit.schemaHashBefore}, got ${beforeSchemaHash}`,
      );
    }

    const computedPlanHash = sha256Hex(replay.operations);
    if (computedPlanHash !== replay.commit.planHash) {
      throw projectionFailure(
        `plan_hash mismatch for replay ${replay.commit.commitId}: expected ${replay.commit.planHash}, got ${computedPlanHash}`,
      );
    }

    await applyEffects(executor, replay.rowEffects, replay.schemaEffects, "forward", {
      disableTableTriggers: true,
    });
    await applyRowEffects(executor, replay.rowEffects, "forward", {
      disableTableTriggers: true,
      includeUserEffects: false,
      includeSystemEffects: true,
      systemPolicy: "reconcile",
    });
    await assertForeignKeys(executor, `replay ${replay.commit.commitId}`);
    await assertStructuralIntegrity(executor, `replay ${replay.commit.commitId}`);

    const afterSchemaHash = await remoteSchemaHash(executor);
    if (afterSchemaHash !== replay.commit.schemaHashAfter) {
      throw projectionFailure(
        `schema_hash_after mismatch for replay ${replay.commit.commitId}: expected ${replay.commit.schemaHashAfter}, got ${afterSchemaHash}`,
      );
    }

    const afterStateHash = await remoteStateHash(executor);
    if (afterStateHash !== replay.commit.stateHashAfter) {
      throw projectionFailure(
        `state_hash_after mismatch for replay ${replay.commit.commitId}: expected ${replay.commit.stateHashAfter}, got ${afterStateHash}`,
      );
    }
  }, `replay ${replay.commit.commitId}`);
}

export async function materializeToHead(client: Client): Promise<void> {
  const tx = await client.transaction("write");
  try {
    await tx.execute("PRAGMA foreign_keys=ON");
    await tx.execute("PRAGMA defer_foreign_keys=ON");

    const head = await remoteHead(tx);
    const checkpoint = head.commitId
      ? normalizeMetaString(await getRemoteMetaValue(tx, LAST_MATERIALIZED_COMMIT_META_KEY))
      : null;
    const checkpointSeq = checkpoint ? await remoteCommitSeq(tx, checkpoint) : null;
    const needsRebuild = !head.commitId || !checkpoint || checkpointSeq === null || checkpointSeq > head.seq;

    if (needsRebuild) {
      await runProjectionStep(() => rebuildProjectionFromCanonicalHistory(tx, head.seq), "rebuild projection");
      await tx.commit();
      return;
    }

    await runProjectionStep(() => verifyProjectionAtCommit(tx, checkpoint), `verify checkpoint ${checkpoint}`);

    const replayInputs = await runProjectionStep(
      () => fetchCommitsAfter(tx, checkpointSeq, head),
      `load commits after checkpoint ${checkpoint}`,
    );
    for (const replay of replayInputs) {
      await materializeCommit(tx, replay);
      await writeMaterializedCheckpoint(tx, replay.commit.commitId);
    }

    await setRemoteMetaValue(tx, LAST_MATERIALIZED_AT_META_KEY, String(Date.now()));
    await setRemoteMetaValue(tx, LAST_MATERIALIZED_ERROR_META_KEY, "");
    await tx.commit();
  } catch (error) {
    const shouldWrapProjectionError =
      projectionErrorMessage(error) !== null ||
      CodedError.hasCode(error, "SYNC_DIVERGED") ||
      (error instanceof Error && (error.name === "SyntaxError" || error.name === "ZodError"));
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

export async function projectionStatus(
  executor: Client | Transaction,
  remoteHeadInput?: Awaited<ReturnType<typeof remoteHead>>,
) {
  const head = remoteHeadInput ?? (await remoteHead(executor));
  const projectionHead = normalizeMetaString(await getRemoteMetaValue(executor, LAST_MATERIALIZED_COMMIT_META_KEY));
  const recordedError = normalizeMetaString(await getRemoteMetaValue(executor, LAST_MATERIALIZED_ERROR_META_KEY));

  if (!head.commitId) {
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

  const projectionAheadOfHead = projectionSeq !== null && projectionSeq > head.seq;
  let projectionLagCommits: number;
  if (projectionSeq === null) {
    projectionLagCommits = head.seq;
  } else if (projectionAheadOfHead) {
    projectionLagCommits = 0;
  } else {
    projectionLagCommits = head.seq - projectionSeq;
  }
  let projectionError = projectionHead && projectionSeq === null
    ? `Materialization checkpoint is missing from canonical history: ${projectionHead}`
    : recordedError;
  if (projectionAheadOfHead) {
    projectionError = `Materialization checkpoint is ahead of remote HEAD: checkpoint=${projectionHead} seq=${projectionSeq} head=${head.commitId} seq=${head.seq}`;
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

async function writeToRemote(tx: Transaction, replay: ReturnType<typeof readCommit>): Promise<void> {
  await tx.execute({
    sql: `
      INSERT INTO _lore_commit(
        commit_id, seq, kind, message, created_at, parent_count,
        schema_hash_before, schema_hash_after, state_hash_after, plan_hash,
        revertible, revert_target_id
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(commit_id) DO NOTHING
    `,
    args: [
      replay.commit.commitId,
      replay.commit.seq,
      replay.commit.kind,
      replay.commit.message,
      replay.commit.createdAt,
      replay.parentIds.length,
      replay.commit.schemaHashBefore,
      replay.commit.schemaHashAfter,
      replay.commit.stateHashAfter,
      replay.commit.planHash,
      replay.commit.revertible,
      replay.commit.revertTargetId,
    ],
  });

  for (let i = 0; i < replay.parentIds.length; i += 1) {
    await tx.execute({
      sql: `
        INSERT INTO _lore_commit_parent(commit_id, parent_commit_id, ord)
        VALUES (?, ?, ?)
        ON CONFLICT(commit_id, ord) DO NOTHING
      `,
      args: [replay.commit.commitId, replay.parentIds[i]!, i],
    });
  }

  for (let i = 0; i < replay.operations.length; i += 1) {
    const operation = replay.operations[i]!;
    await tx.execute({
      sql: `
        INSERT INTO _lore_op(commit_id, op_index, op_type, op_json)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(commit_id, op_index) DO NOTHING
      `,
      args: [replay.commit.commitId, i, operation.type, canonicalJson(operation)],
    });
  }

  for (let i = 0; i < replay.rowEffects.length; i += 1) {
    const effect = replay.rowEffects[i]!;
    const beforeJson = effect.beforeRow ? canonicalJson(effect.beforeRow) : null;
    const afterJson = effect.afterRow ? canonicalJson(effect.afterRow) : null;
    await tx.execute({
      sql: `
        INSERT INTO _lore_row_effect(
          commit_id, effect_index, table_name, pk_json, op_kind,
          before_json, after_json, before_hash, after_hash
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(commit_id, effect_index) DO NOTHING
      `,
      args: [
        replay.commit.commitId,
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
        INSERT INTO _lore_schema_effect(
          commit_id, effect_index, table_name, before_json, after_json
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(commit_id, effect_index) DO NOTHING
      `,
      args: [
        replay.commit.commitId,
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
  replay: ReturnType<typeof readCommit>,
  expectedRemoteHead: string | null,
): Promise<void> {
  const tx = await client.transaction("write");
  try {
    await tx.execute("PRAGMA foreign_keys=ON");
    await tx.execute("PRAGMA defer_foreign_keys=ON");

    const currentHead = await remoteHead(tx);
    if (currentHead.commitId !== expectedRemoteHead) {
      throw new CodedError(
        "SYNC_NON_FAST_FORWARD",
        `Remote HEAD changed during push. expected=${expectedRemoteHead ?? "null"} actual=${currentHead.commitId ?? "null"}`,
      );
    }

    await writeToRemote(tx, replay);
    await runProjectionStep(async () => {
      await materializeCommit(tx, replay);
      await writeMaterializedCheckpoint(tx, replay.commit.commitId);
    }, `materialize pushed commit ${replay.commit.commitId}`);

    const update = await tx.execute({
      sql: `
        UPDATE _lore_ref
        SET commit_id = ?, updated_at = ?
        WHERE name = ? AND ((? IS NULL AND commit_id IS NULL) OR commit_id = ?)
      `,
      args: [replay.commit.commitId, replay.commit.createdAt, MAIN_REF_NAME, expectedRemoteHead, expectedRemoteHead],
    });
    if (update.rowsAffected !== 1) {
      throw new CodedError(
        "SYNC_NON_FAST_FORWARD",
        `Remote HEAD changed during push CAS update. expected=${expectedRemoteHead ?? "null"}`,
      );
    }

    await tx.execute({
      sql: `
        INSERT INTO _lore_reflog(ref_name, old_commit_id, new_commit_id, reason, created_at)
        VALUES (?, ?, ?, ?, ?)
      `,
      args: [
        MAIN_REF_NAME,
        expectedRemoteHead,
        replay.commit.commitId,
        replay.commit.kind === "revert" ? "revert" : "apply",
        Date.now(),
      ],
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

async function fetchCommitRange(
  executor: Client | Transaction,
  fromSeqExclusive: number,
  toSeqInclusive: number,
): Promise<Array<ReturnType<typeof readCommit>>> {
  if (toSeqInclusive <= fromSeqExclusive) {
    return [];
  }
  const commitResult = await executor.execute({
    sql: `
      SELECT
        commit_id, seq, kind, message, created_at, parent_count,
        schema_hash_before, schema_hash_after, state_hash_after, plan_hash,
        revertible, revert_target_id
      FROM _lore_commit
      WHERE seq > ? AND seq <= ?
      ORDER BY seq ASC
    `,
    args: [fromSeqExclusive, toSeqInclusive],
  });
  const commits = rowsFrom(commitResult).map((row) => ({
    commitId: parseString(parseRowValue(row, "commit_id"), "_lore_commit.commit_id"),
    seq: parseInteger(parseRowValue(row, "seq"), "_lore_commit.seq"),
    kind: parseCommitKind(parseString(parseRowValue(row, "kind"), "_lore_commit.kind")),
    message: parseString(parseRowValue(row, "message"), "_lore_commit.message"),
    createdAt: parseInteger(parseRowValue(row, "created_at"), "_lore_commit.created_at"),
    parentCount: parseInteger(parseRowValue(row, "parent_count"), "_lore_commit.parent_count"),
    schemaHashBefore: parseString(parseRowValue(row, "schema_hash_before"), "_lore_commit.schema_hash_before"),
    schemaHashAfter: parseString(parseRowValue(row, "schema_hash_after"), "_lore_commit.schema_hash_after"),
    stateHashAfter: parseString(parseRowValue(row, "state_hash_after"), "_lore_commit.state_hash_after"),
    planHash: parseString(parseRowValue(row, "plan_hash"), "_lore_commit.plan_hash"),
    revertible: parseInteger(parseRowValue(row, "revertible"), "_lore_commit.revertible"),
    revertTargetId: parseNullableString(parseRowValue(row, "revert_target_id"), "_lore_commit.revert_target_id"),
  }));
  if (commits.length === 0) {
    return [];
  }

  const parentRows = rowsFrom(await executor.execute({
    sql: `
      SELECT cp.commit_id, cp.parent_commit_id
      FROM _lore_commit_parent cp
      JOIN _lore_commit c ON c.commit_id = cp.commit_id
      WHERE c.seq > ? AND c.seq <= ?
      ORDER BY c.seq ASC, cp.ord ASC
    `,
    args: [fromSeqExclusive, toSeqInclusive],
  }));
  const parentIdsByCommit = new Map<string, string[]>();
  for (const row of parentRows) {
    const commitId = parseString(parseRowValue(row, "commit_id"), "_lore_commit_parent.commit_id");
    const parentId = parseString(parseRowValue(row, "parent_commit_id"), "_lore_commit_parent.parent_commit_id");
    const parentIds = parentIdsByCommit.get(commitId) ?? [];
    parentIds.push(parentId);
    parentIdsByCommit.set(commitId, parentIds);
  }

  const operationRows = rowsFrom(await executor.execute({
    sql: `
      SELECT o.commit_id, o.op_json
      FROM _lore_op o
      JOIN _lore_commit c ON c.commit_id = o.commit_id
      WHERE c.seq > ? AND c.seq <= ?
      ORDER BY c.seq ASC, o.op_index ASC
    `,
    args: [fromSeqExclusive, toSeqInclusive],
  }));
  const operationsByCommit = new Map<string, Op[]>();
  for (const row of operationRows) {
    const commitId = parseString(parseRowValue(row, "commit_id"), "_lore_op.commit_id");
    const operation = Operation.parse(parseJson(parseRowValue(row, "op_json"), "_lore_op.op_json"));
    const operations = operationsByCommit.get(commitId) ?? [];
    operations.push(operation);
    operationsByCommit.set(commitId, operations);
  }

  const rowEffectRows = rowsFrom(await executor.execute({
    sql: `
      SELECT re.commit_id, re.table_name, re.pk_json, re.op_kind, re.before_json, re.after_json, re.before_hash, re.after_hash
      FROM _lore_row_effect re
      JOIN _lore_commit c ON c.commit_id = re.commit_id
      WHERE c.seq > ? AND c.seq <= ?
      ORDER BY c.seq ASC, re.effect_index ASC
    `,
    args: [fromSeqExclusive, toSeqInclusive],
  }));
  const rowEffectsByCommit = new Map<string, RowEffect[]>();
  for (const row of rowEffectRows) {
    const commitId = parseString(parseRowValue(row, "commit_id"), "_lore_row_effect.commit_id");
    const effect = RowEffect.parse({
      tableName: parseString(parseRowValue(row, "table_name"), "_lore_row_effect.table_name"),
      pk: parseJson(parseRowValue(row, "pk_json"), "_lore_row_effect.pk_json"),
      opKind: parseOpKind(parseString(parseRowValue(row, "op_kind"), "_lore_row_effect.op_kind")),
      beforeRow: parseRowValue(row, "before_json")
        ? parseJson(parseRowValue(row, "before_json"), "_lore_row_effect.before_json")
        : null,
      afterRow: parseRowValue(row, "after_json")
        ? parseJson(parseRowValue(row, "after_json"), "_lore_row_effect.after_json")
        : null,
      beforeHash: parseNullableString(parseRowValue(row, "before_hash"), "_lore_row_effect.before_hash"),
      afterHash: parseNullableString(parseRowValue(row, "after_hash"), "_lore_row_effect.after_hash"),
    });
    const effects = rowEffectsByCommit.get(commitId) ?? [];
    effects.push(effect);
    rowEffectsByCommit.set(commitId, effects);
  }

  const schemaEffectRows = rowsFrom(await executor.execute({
    sql: `
      SELECT se.commit_id, se.table_name, se.before_json, se.after_json
      FROM _lore_schema_effect se
      JOIN _lore_commit c ON c.commit_id = se.commit_id
      WHERE c.seq > ? AND c.seq <= ?
      ORDER BY c.seq ASC, se.effect_index ASC
    `,
    args: [fromSeqExclusive, toSeqInclusive],
  }));
  const schemaEffectsByCommit = new Map<string, SchemaEffect[]>();
  for (const row of schemaEffectRows) {
    const commitId = parseString(parseRowValue(row, "commit_id"), "_lore_schema_effect.commit_id");
    const effect = SchemaEffect.parse({
      tableName: parseString(parseRowValue(row, "table_name"), "_lore_schema_effect.table_name"),
      beforeTable: parseRowValue(row, "before_json")
        ? parseJson(parseRowValue(row, "before_json"), "_lore_schema_effect.before_json")
        : null,
      afterTable: parseRowValue(row, "after_json")
        ? parseJson(parseRowValue(row, "after_json"), "_lore_schema_effect.after_json")
        : null,
    });
    const effects = schemaEffectsByCommit.get(commitId) ?? [];
    effects.push(effect);
    schemaEffectsByCommit.set(commitId, effects);
  }

  return commits.map((commit) => ({
    commit,
    parentIds: parentIdsByCommit.get(commit.commitId) ?? [],
    operations: operationsByCommit.get(commit.commitId) ?? [],
    rowEffects: rowEffectsByCommit.get(commit.commitId) ?? [],
    schemaEffects: schemaEffectsByCommit.get(commit.commitId) ?? [],
  }));
}

export async function fetchCommitsAfter(
  executor: Client | Transaction,
  fromSeqExclusive: number,
  remoteHeadInput?: Awaited<ReturnType<typeof remoteHead>>,
): Promise<Array<ReturnType<typeof readCommit>>> {
  const head = remoteHeadInput ?? (await remoteHead(executor));
  if (!head.commitId) {
    return [];
  }
  return fetchCommitRange(executor, fromSeqExclusive, head.seq);
}
