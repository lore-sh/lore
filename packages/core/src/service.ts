import { createHash } from "node:crypto";
import { cp, mkdir, readFile, rm } from "node:fs/promises";
import { dirname, join } from "node:path";
import type { Database } from "bun:sqlite";
import { canonicalJson, sha256Hex } from "./checksum";
import {
  assertInitialized,
  closeDatabase,
  DEFAULT_SNAPSHOT_INTERVAL,
  DEFAULT_SNAPSHOT_RETAIN,
  detectLegacySchema,
  FORMAT_GENERATION,
  getMetaValue,
  HISTORY_ENGINE,
  initializeStorage,
  listUserTables,
  MAIN_REF_NAME,
  openDatabase,
  runInTransaction,
  SQLITE_MIN_VERSION,
} from "./db";
import { TossError } from "./errors";
import { executeOperation } from "./executors/apply";
import { executeReadSql } from "./executors/read";
import {
  appendCommit,
  getCommitById,
  getHeadCommit,
  getHeadCommitId,
  getNextCommitSeq,
  getRowEffectsByCommitId,
  getSchemaEffectsByCommitId,
  listCommits,
  type RowEffect,
  type SchemaEffect,
  type StoredRowEffect,
  type StoredSchemaEffect,
} from "./log";
import { generateSkills, type GeneratedSkills } from "./skills";
import type {
  CommitEntry,
  JsonObject,
  JsonPrimitive,
  Operation,
  RevertConflict,
  RevertResult,
  SnapshotEntry,
  TossStatus,
  VerifyResult,
} from "./types";
import { parseAndValidateOperationPlan } from "./validators/operation";
import { validateReadSql } from "./validators/sql";

const IDENTIFIER_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

interface TableInfoRow {
  cid: number;
  name: string;
  type: string;
  notnull: number;
  dflt_value: string | null;
  pk: number;
}

interface ServiceOptions {
  dbPath?: string;
}

export interface InitDatabaseOptions extends ServiceOptions {
  generateSkills?: boolean;
  workspacePath?: string;
  forceNew?: boolean;
}

function quoteIdentifier(value: string): string {
  if (!IDENTIFIER_PATTERN.test(value)) {
    throw new TossError("INVALID_IDENTIFIER", `Invalid identifier: ${value}`);
  }
  return `"${value.replaceAll('"', '""')}"`;
}

function serializeValue(value: JsonPrimitive): JsonPrimitive {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      return null;
    }
    return value;
  }
  return value;
}

function normalizeRowObject(row: Record<string, unknown>): JsonObject {
  const output: JsonObject = {};
  for (const [key, value] of Object.entries(row)) {
    if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      output[key] = value;
      continue;
    }
    if (value instanceof Uint8Array) {
      output[key] = Buffer.from(value).toString("base64");
      continue;
    }
    output[key] = JSON.stringify(value);
  }
  return output;
}

function hashFile(path: string): Promise<string> {
  return readFile(path).then((buffer) => createHash("sha256").update(buffer).digest("hex"));
}

function tableInfo(db: Database, table: string): TableInfoRow[] {
  return db.query(`PRAGMA table_info(${quoteIdentifier(table)})`).all() as TableInfoRow[];
}

function primaryKeyColumns(db: Database, table: string): string[] {
  return tableInfo(db, table)
    .filter((column) => column.pk > 0)
    .sort((a, b) => a.pk - b.pk)
    .map((column) => column.name);
}

function tableDDL(db: Database, table: string): string | null {
  const row = db
    .query("SELECT sql FROM sqlite_master WHERE type='table' AND name=? LIMIT 1")
    .get(table) as { sql: string | null } | null;
  return row?.sql ?? null;
}

function whereClauseFromRecord(
  values: Record<string, JsonPrimitive>,
): { clause: string; bindings: JsonPrimitive[] } {
  const keys = Object.keys(values);
  if (keys.length === 0) {
    throw new TossError("INVALID_OPERATION", "where must not be empty");
  }

  const terms: string[] = [];
  const bindings: JsonPrimitive[] = [];
  for (const key of keys) {
    const value = values[key];
    if (value === undefined) {
      throw new TossError("INVALID_OPERATION", `where value missing for key: ${key}`);
    }
    const quoted = quoteIdentifier(key);
    if (value === null) {
      terms.push(`${quoted} IS NULL`);
      continue;
    }
    terms.push(`${quoted} = ?`);
    bindings.push(serializeValue(value));
  }
  return { clause: terms.join(" AND "), bindings };
}

function fetchRowsByWhere(
  db: Database,
  table: string,
  where: Record<string, JsonPrimitive>,
): Array<Record<string, unknown>> {
  const { clause, bindings } = whereClauseFromRecord(where);
  const sql = `SELECT rowid AS __toss_rowid, * FROM ${quoteIdentifier(table)} WHERE ${clause}`;
  return db.query(sql).all(...bindings) as Array<Record<string, unknown>>;
}

function fetchAllRows(db: Database, table: string): JsonObject[] {
  const rows = db.query(`SELECT rowid AS __toss_rowid, * FROM ${quoteIdentifier(table)} ORDER BY rowid ASC`).all() as Array<
    Record<string, unknown>
  >;
  return rows.map((row) => normalizeRowObject(row));
}

function pkFromRow(db: Database, table: string, row: Record<string, unknown>): Record<string, JsonPrimitive> {
  const pkCols = primaryKeyColumns(db, table);
  if (pkCols.length === 0) {
    const rowid = row.__toss_rowid;
    if (typeof rowid !== "number") {
      throw new TossError("INVALID_OPERATION", `Cannot determine rowid primary key for ${table}`);
    }
    return { __rowid: rowid };
  }

  const pk: Record<string, JsonPrimitive> = {};
  for (const column of pkCols) {
    const value = row[column];
    if (value === undefined) {
      throw new TossError("INVALID_OPERATION", `PK column missing in row: ${table}.${column}`);
    }
    if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      pk[column] = value;
    } else {
      throw new TossError("INVALID_OPERATION", `Unsupported PK value type in ${table}.${column}`);
    }
  }
  return pk;
}

function fetchRowByPk(
  db: Database,
  table: string,
  pk: Record<string, JsonPrimitive>,
): Record<string, unknown> | null {
  if (Object.hasOwn(pk, "__rowid")) {
    const rowid = pk.__rowid;
    if (typeof rowid !== "number") {
      throw new TossError("INVALID_OPERATION", "Invalid __rowid key");
    }
    const row = db
      .query(`SELECT rowid AS __toss_rowid, * FROM ${quoteIdentifier(table)} WHERE rowid = ? LIMIT 1`)
      .get(rowid) as Record<string, unknown> | null;
    return row;
  }

  const { clause, bindings } = whereClauseFromRecord(pk);
  const row = db
    .query(`SELECT rowid AS __toss_rowid, * FROM ${quoteIdentifier(table)} WHERE ${clause} LIMIT 1`)
    .get(...bindings) as Record<string, unknown> | null;
  return row;
}

function rowHash(row: JsonObject | null): string | null {
  if (!row) {
    return null;
  }
  return sha256Hex(canonicalJson(row));
}

function schemaHash(db: Database): string {
  const rows = db
    .query(
      "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE '_toss_%' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .all() as Array<{ name: string; sql: string | null }>;
  return sha256Hex(rows);
}

function stateHash(db: Database): string {
  const tables = listUserTables(db);
  const state: Record<string, JsonObject[]> = {};
  for (const table of tables) {
    state[table] = fetchAllRows(db, table);
  }
  return sha256Hex(state);
}

function readPlanInput(planRef: string): Promise<string> {
  if (planRef === "-") {
    return Bun.stdin.text();
  }
  return readFile(planRef, "utf8");
}

function buildRowEffectsForUpdateDelete(
  db: Database,
  table: string,
  opKind: "update" | "delete",
  beforeRows: Array<Record<string, unknown>>,
): RowEffect[] {
  const effects: RowEffect[] = [];
  for (const beforeRow of beforeRows) {
    const pk = pkFromRow(db, table, beforeRow);
    const afterRow = opKind === "delete" ? null : fetchRowByPk(db, table, pk);
    effects.push({
      tableName: table,
      pk,
      opKind,
      beforeRow: normalizeRowObject(beforeRow),
      afterRow: afterRow ? normalizeRowObject(afterRow) : null,
    });
  }
  return effects;
}

function applyOperationWithEffects(db: Database, operation: Operation): { rowEffects: RowEffect[]; schemaEffects: SchemaEffect[] } {
  if (operation.type === "insert") {
    executeOperation(db, operation);
    const pkCols = primaryKeyColumns(db, operation.table);
    let insertedRow: Record<string, unknown> | null = null;
    if (pkCols.length > 0 && pkCols.every((column) => Object.hasOwn(operation.values, column))) {
      const pkWhere = Object.fromEntries(pkCols.map((column) => [column, operation.values[column] ?? null])) as Record<
        string,
        JsonPrimitive
      >;
      insertedRow = fetchRowByPk(db, operation.table, pkWhere);
    } else {
      const row = db
        .query(`SELECT rowid AS __toss_rowid, * FROM ${quoteIdentifier(operation.table)} WHERE rowid = last_insert_rowid()`)
        .get() as Record<string, unknown> | null;
      insertedRow = row;
    }

    if (!insertedRow) {
      throw new TossError("APPLY_FAILED", `Unable to capture inserted row for table ${operation.table}`);
    }

    return {
      rowEffects: [
        {
          tableName: operation.table,
          pk: pkFromRow(db, operation.table, insertedRow),
          opKind: "insert",
          beforeRow: null,
          afterRow: normalizeRowObject(insertedRow),
        },
      ],
      schemaEffects: [],
    };
  }

  if (operation.type === "update") {
    const beforeRows = fetchRowsByWhere(db, operation.table, operation.where);
    executeOperation(db, operation);
    return { rowEffects: buildRowEffectsForUpdateDelete(db, operation.table, "update", beforeRows), schemaEffects: [] };
  }

  if (operation.type === "delete") {
    const beforeRows = fetchRowsByWhere(db, operation.table, operation.where);
    executeOperation(db, operation);
    return { rowEffects: buildRowEffectsForUpdateDelete(db, operation.table, "delete", beforeRows), schemaEffects: [] };
  }

  if (operation.type === "create_table") {
    executeOperation(db, operation);
    return {
      rowEffects: [],
      schemaEffects: [
        {
          tableName: operation.table,
          columnName: null,
          opKind: "create_table",
          ddlBeforeSql: null,
          ddlAfterSql: tableDDL(db, operation.table),
          tableRowsBefore: null,
        },
      ],
    };
  }

  if (operation.type === "add_column") {
    const beforeDDL = tableDDL(db, operation.table);
    const rowsBefore = fetchAllRows(db, operation.table);
    executeOperation(db, operation);
    return {
      rowEffects: [],
      schemaEffects: [
        {
          tableName: operation.table,
          columnName: operation.column.name,
          opKind: "add_column",
          ddlBeforeSql: beforeDDL,
          ddlAfterSql: tableDDL(db, operation.table),
          tableRowsBefore: rowsBefore,
        },
      ],
    };
  }

  if (operation.type === "drop_table") {
    const beforeDDL = tableDDL(db, operation.table);
    const rowsBefore = fetchAllRows(db, operation.table);
    executeOperation(db, operation);
    return {
      rowEffects: [],
      schemaEffects: [
        {
          tableName: operation.table,
          columnName: null,
          opKind: "drop_table",
          ddlBeforeSql: beforeDDL,
          ddlAfterSql: null,
          tableRowsBefore: rowsBefore,
        },
      ],
    };
  }

  if (operation.type === "drop_column") {
    const beforeDDL = tableDDL(db, operation.table);
    const rowsBefore = fetchAllRows(db, operation.table);
    executeOperation(db, operation);
    return {
      rowEffects: [],
      schemaEffects: [
        {
          tableName: operation.table,
          columnName: operation.column,
          opKind: "drop_column",
          ddlBeforeSql: beforeDDL,
          ddlAfterSql: tableDDL(db, operation.table),
          tableRowsBefore: rowsBefore,
        },
      ],
    };
  }

  if (operation.type === "alter_column_type") {
    const beforeDDL = tableDDL(db, operation.table);
    const rowsBefore = fetchAllRows(db, operation.table);
    executeOperation(db, operation);
    return {
      rowEffects: [],
      schemaEffects: [
        {
          tableName: operation.table,
          columnName: operation.column,
          opKind: "alter_column_type",
          ddlBeforeSql: beforeDDL,
          ddlAfterSql: tableDDL(db, operation.table),
          tableRowsBefore: rowsBefore,
        },
      ],
    };
  }

  throw new TossError("UNSUPPORTED_OPERATION", `Unsupported operation type: ${(operation as Operation).type}`);
}

function applyOperationsWithEffects(db: Database, operations: Operation[]): { rowEffects: RowEffect[]; schemaEffects: SchemaEffect[] } {
  const rowEffects: RowEffect[] = [];
  const schemaEffects: SchemaEffect[] = [];
  for (const operation of operations) {
    const captured = applyOperationWithEffects(db, operation);
    rowEffects.push(...captured.rowEffects);
    schemaEffects.push(...captured.schemaEffects);
  }
  return { rowEffects, schemaEffects };
}

function putMeta(db: Database, key: string, value: string): void {
  db.query(
    `
    INSERT INTO _toss_repo_meta(key, value) VALUES(?, ?)
    ON CONFLICT(key) DO UPDATE SET value=excluded.value
    `,
  ).run(key, value);
}

function getSnapshotInterval(db: Database): number {
  const value = getMetaValue(db, "snapshot_interval");
  return value ? Number(value) : DEFAULT_SNAPSHOT_INTERVAL;
}

function getSnapshotRetain(db: Database): number {
  const value = getMetaValue(db, "snapshot_retain");
  return value ? Number(value) : DEFAULT_SNAPSHOT_RETAIN;
}

async function maybeCreateSnapshot(dbPath: string, commit: CommitEntry): Promise<void> {
  const { db } = openDatabase(dbPath);
  try {
    const interval = getSnapshotInterval(db);
    if (interval <= 0 || commit.seq % interval !== 0) {
      return;
    }
  } finally {
    closeDatabase(db);
  }

  const snapshotsDir = join(dirname(dbPath), ".toss", "snapshots");
  await mkdir(snapshotsDir, { recursive: true });
  const snapshotPath = join(snapshotsDir, `${commit.seq}-${commit.commitId}.db`);

  const { db: snapshotDb } = openDatabase(dbPath);
  try {
    snapshotDb.run(`VACUUM INTO '${snapshotPath.replaceAll("'", "''")}'`);
  } finally {
    closeDatabase(snapshotDb);
  }

  const digest = await hashFile(snapshotPath);
  const { db: writeDb } = openDatabase(dbPath);
  try {
    const rowCountHint = listUserTables(writeDb).reduce((acc, table) => {
      const row = writeDb.query(`SELECT COUNT(*) AS c FROM ${quoteIdentifier(table)}`).get() as { c: number };
      return acc + row.c;
    }, 0);
    writeDb
      .query("INSERT OR REPLACE INTO _toss_snapshot(commit_id, file_path, file_sha256, created_at, row_count_hint) VALUES(?, ?, ?, ?, ?)")
      .run(commit.commitId, snapshotPath, digest, new Date().toISOString(), rowCountHint);

    const retain = getSnapshotRetain(writeDb);
    const stale = writeDb
      .query(
        `
        SELECT commit_id, file_path FROM _toss_snapshot
        ORDER BY created_at DESC
        LIMIT -1 OFFSET ?
        `,
      )
      .all(retain) as Array<{ commit_id: string; file_path: string }>;
    for (const row of stale) {
      await rm(row.file_path, { force: true });
      writeDb.query("DELETE FROM _toss_snapshot WHERE commit_id=?").run(row.commit_id);
    }
  } finally {
    closeDatabase(writeDb);
  }
}

async function removeExistingDbFiles(dbPath: string): Promise<void> {
  await rm(dbPath, { force: true });
  await rm(`${dbPath}-shm`, { force: true });
  await rm(`${dbPath}-wal`, { force: true });
}

export async function initDatabase(
  options: InitDatabaseOptions = {},
): Promise<{ dbPath: string; generatedSkills: GeneratedSkills | null }> {
  const { db, dbPath } = openDatabase(options.dbPath);
  let shouldClose = true;
  try {
    if (options.forceNew) {
      closeDatabase(db);
      shouldClose = false;
      await removeExistingDbFiles(dbPath);
      const reopened = openDatabase(dbPath);
      try {
        initializeStorage(reopened.db);
      } finally {
        closeDatabase(reopened.db);
      }
    } else {
      const hasRepoMetaTable =
        (db
          .query("SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='_toss_repo_meta' LIMIT 1")
          .get() as { ok?: number } | null)?.ok === 1;
      const hasFormatGenerationMeta = hasRepoMetaTable && getMetaValue(db, "format_generation") !== null;
      const hasHistoryEngineMeta = hasRepoMetaTable && getMetaValue(db, "history_engine") !== null;

      if (detectLegacySchema(db) || (hasFormatGenerationMeta && !hasHistoryEngineMeta)) {
        throw new TossError(
          "FORMAT_MISMATCH",
          `Legacy toss format detected at ${dbPath}. Run \`toss init --force-new\` to reinitialize.`,
        );
      }
      initializeStorage(db);
    }
  } finally {
    if (shouldClose) {
      closeDatabase(db);
    }
  }

  const generatedSkills = options.generateSkills ? await generateSkills(options.workspacePath) : null;
  return { dbPath, generatedSkills };
}

function buildCommitOperationsResult(db: Database, planOperations: Operation[], kind: "apply" | "revert", message: string, revertedTargetId: string | null): CommitEntry {
  const parent = getHeadCommit(db);
  const parentIds = parent ? [parent.commitId] : [];
  const seq = getNextCommitSeq(db);
  const createdAt = new Date().toISOString();
  const beforeSchemaHash = schemaHash(db);

  const captured = applyOperationsWithEffects(db, planOperations);
  const afterSchemaHash = schemaHash(db);
  const afterStateHash = stateHash(db);
  const planHash = sha256Hex(planOperations);

  return appendCommit(db, {
    seq,
    kind,
    message,
    createdAt,
    parentIds,
    schemaHashBefore: beforeSchemaHash,
    schemaHashAfter: afterSchemaHash,
    stateHashAfter: afterStateHash,
    planHash,
    inverseReady: true,
    revertedTargetId,
    operations: planOperations,
    rowEffects: captured.rowEffects,
    schemaEffects: captured.schemaEffects,
  });
}

export async function applyPlan(planRef: string, options: ServiceOptions = {}): Promise<CommitEntry> {
  const payload = await readPlanInput(planRef);
  const plan = parseAndValidateOperationPlan(payload);

  const { db, dbPath } = openDatabase(options.dbPath);
  let commit: CommitEntry;
  try {
    assertInitialized(db, dbPath);
    commit = runInTransaction(db, () => buildCommitOperationsResult(db, plan.operations, "apply", plan.message, null));
  } finally {
    closeDatabase(db);
  }

  await maybeCreateSnapshot(dbPath, commit);
  return commit;
}

export function readQuery(sqlInput: string, options: ServiceOptions = {}): Record<string, unknown>[] {
  const sql = validateReadSql(sqlInput);
  const { db, dbPath } = openDatabase(options.dbPath);
  try {
    assertInitialized(db, dbPath);
    return executeReadSql(db, sql);
  } finally {
    closeDatabase(db);
  }
}

export function getStatus(options: ServiceOptions = {}): TossStatus {
  const { db, dbPath } = openDatabase(options.dbPath);
  try {
    assertInitialized(db, dbPath);

    const tables = listUserTables(db).map((table) => {
      const row = db.query(`SELECT COUNT(*) AS c FROM ${quoteIdentifier(table)}`).get() as { c: number };
      return { name: table, count: row.c };
    });

    const head = getHeadCommit(db);
    const snapshotCountRow = db.query("SELECT COUNT(*) AS c FROM _toss_snapshot").get() as { c: number };

    return {
      dbPath,
      historyEngine: getMetaValue(db, "history_engine") ?? HISTORY_ENGINE,
      formatGeneration: Number(getMetaValue(db, "format_generation") ?? FORMAT_GENERATION),
      sqliteMinVersion: getMetaValue(db, "sqlite_min_version") ?? SQLITE_MIN_VERSION,
      tableCount: tables.length,
      tables,
      headCommit: head
        ? {
            commitId: head.commitId,
            seq: head.seq,
            kind: head.kind,
            message: head.message,
            createdAt: head.createdAt,
          }
        : null,
      snapshotCount: snapshotCountRow.c,
      lastVerifiedAt: getMetaValue(db, "last_verified_at"),
    };
  } finally {
    closeDatabase(db);
  }
}

export function getHistory(
  options: ServiceOptions & {
    verbose?: boolean;
  } = {},
): CommitEntry[] {
  const { db, dbPath } = openDatabase(options.dbPath);
  try {
    assertInitialized(db, dbPath);
    return listCommits(db, true);
  } finally {
    closeDatabase(db);
  }
}

function hasLaterSchemaChange(
  schemaEffects: StoredSchemaEffect[],
  laterSchemaEffects: StoredSchemaEffect[],
): RevertConflict[] {
  const conflicts: RevertConflict[] = [];
  for (const effect of schemaEffects) {
    const matched = laterSchemaEffects.filter((later) => {
      if (later.tableName !== effect.tableName) {
        return false;
      }
      if (!effect.columnName) {
        return true;
      }
      return later.columnName === effect.columnName || later.columnName === null;
    });
    for (const later of matched) {
      conflicts.push({
        kind: "schema",
        table: effect.tableName,
        column: effect.columnName ?? undefined,
        reason: `Later schema change found on ${later.tableName}${later.columnName ? `.${later.columnName}` : ""}`,
      });
    }
  }
  return conflicts;
}

function detectRowConflict(
  db: Database,
  targetRowEffects: StoredRowEffect[],
  laterRowEffects: StoredRowEffect[],
): RevertConflict[] {
  const conflicts: RevertConflict[] = [];
  for (const effect of targetRowEffects) {
    const pkJson = canonicalJson(effect.pk);
    const touchedLater = laterRowEffects.some((later) => later.tableName === effect.tableName && canonicalJson(later.pk) === pkJson);
    if (touchedLater) {
      conflicts.push({
        kind: "row",
        table: effect.tableName,
        pk: effect.pk,
        reason: "Later commits touched the same row.",
      });
      continue;
    }

    const currentRowRaw = fetchRowByPk(db, effect.tableName, effect.pk);
    const currentRow = currentRowRaw ? normalizeRowObject(currentRowRaw) : null;
    const currentHash = rowHash(currentRow);
    if (effect.opKind === "update" && currentHash !== effect.afterHash) {
      conflicts.push({
        kind: "row",
        table: effect.tableName,
        pk: effect.pk,
        reason: "Current row hash differs from target after-image.",
      });
      continue;
    }
    if (effect.opKind === "insert" && currentHash !== effect.afterHash) {
      conflicts.push({
        kind: "row",
        table: effect.tableName,
        pk: effect.pk,
        reason: "Inserted row was changed or removed after the target commit.",
      });
      continue;
    }
    if (effect.opKind === "delete" && currentRow && currentHash !== effect.beforeHash) {
      conflicts.push({
        kind: "row",
        table: effect.tableName,
        pk: effect.pk,
        reason: "Row reappeared with different values; revert would overwrite divergent data.",
      });
    }
  }
  return conflicts;
}

function fetchLaterEffects(db: Database, seq: number): { rows: StoredRowEffect[]; schemas: StoredSchemaEffect[] } {
  const laterCommits = db
    .query("SELECT commit_id FROM _toss_commit WHERE seq > ? ORDER BY seq ASC")
    .all(seq) as Array<{ commit_id: string }>;
  const rows: StoredRowEffect[] = [];
  const schemas: StoredSchemaEffect[] = [];
  for (const commit of laterCommits) {
    rows.push(...getRowEffectsByCommitId(db, commit.commit_id));
    schemas.push(...getSchemaEffectsByCommitId(db, commit.commit_id));
  }
  return { rows, schemas };
}

function reconstructTableFromDdl(db: Database, tableName: string, ddlSql: string, rowsBefore: JsonObject[] | null): void {
  const quotedTable = quoteIdentifier(tableName);
  const tmpTable = `__toss_restore_${tableName}_${crypto.randomUUID().replaceAll("-", "")}`;
  const quotedTmp = quoteIdentifier(tmpTable);

  db.run(ddlSql.replace(new RegExp(`CREATE TABLE\\s+${quotedTable}`, "i"), `CREATE TABLE ${quotedTmp}`));

  if (rowsBefore && rowsBefore.length > 0) {
    const firstRow = rowsBefore[0];
    if (firstRow) {
      const columns = Object.keys(firstRow).filter((key) => key !== "__toss_rowid");
      if (columns.length > 0) {
        const columnSql = columns.map((column) => quoteIdentifier(column)).join(", ");
        const placeholderSql = columns.map(() => "?").join(", ");
        const stmt = db.query(`INSERT INTO ${quotedTmp} (${columnSql}) VALUES (${placeholderSql})`);
        for (const row of rowsBefore) {
          const values = columns.map((column) => {
            const value = row[column];
            if (
              value === null ||
              typeof value === "string" ||
              typeof value === "number" ||
              typeof value === "boolean"
            ) {
              return value;
            }
            return JSON.stringify(value);
          });
          stmt.run(...values);
        }
      }
    }
  }

  db.run(`DROP TABLE IF EXISTS ${quotedTable}`);
  db.run(`ALTER TABLE ${quotedTmp} RENAME TO ${quotedTable}`);
}

function applyInverseEffects(db: Database, targetCommit: CommitEntry): {
  operations: Operation[];
  rowEffects: RowEffect[];
  schemaEffects: SchemaEffect[];
} {
  const rowEffects = getRowEffectsByCommitId(db, targetCommit.commitId);
  const schemaEffects = getSchemaEffectsByCommitId(db, targetCommit.commitId);
  const inverseOperations: Operation[] = [];
  const inverseRowEffects: RowEffect[] = [];
  const inverseSchemaEffects: SchemaEffect[] = [];

  for (const effect of schemaEffects.toReversed()) {
    if (effect.opKind === "drop_table") {
      if (!effect.ddlBeforeSql) {
        throw new TossError("REVERT_FAILED", `Missing ddl_before_sql for ${targetCommit.commitId}`);
      }
      reconstructTableFromDdl(db, effect.tableName, effect.ddlBeforeSql, effect.tableRowsBefore);
      inverseOperations.push({ type: "create_table", table: effect.tableName, columns: [] });
      inverseSchemaEffects.push({
        tableName: effect.tableName,
        columnName: null,
        opKind: "create_table",
        ddlBeforeSql: effect.ddlAfterSql,
        ddlAfterSql: effect.ddlBeforeSql,
        tableRowsBefore: null,
      });
      continue;
    }

    if (effect.opKind === "drop_column" || effect.opKind === "add_column" || effect.opKind === "alter_column_type") {
      if (!effect.ddlBeforeSql) {
        throw new TossError("REVERT_FAILED", `Missing ddl_before_sql for ${targetCommit.commitId}`);
      }
      const rowsBeforeInverse = fetchAllRows(db, effect.tableName);
      reconstructTableFromDdl(db, effect.tableName, effect.ddlBeforeSql, effect.tableRowsBefore);
      if (effect.opKind === "drop_column") {
        inverseOperations.push({
          type: "add_column",
          table: effect.tableName,
          column: { name: effect.columnName ?? "unknown_column", type: "TEXT" },
        });
        inverseSchemaEffects.push({
          tableName: effect.tableName,
          columnName: effect.columnName,
          opKind: "add_column",
          ddlBeforeSql: effect.ddlAfterSql,
          ddlAfterSql: effect.ddlBeforeSql,
          tableRowsBefore: rowsBeforeInverse,
        });
      } else if (effect.opKind === "add_column") {
        inverseOperations.push({ type: "drop_column", table: effect.tableName, column: effect.columnName ?? "unknown_column" });
        inverseSchemaEffects.push({
          tableName: effect.tableName,
          columnName: effect.columnName,
          opKind: "drop_column",
          ddlBeforeSql: effect.ddlAfterSql,
          ddlAfterSql: effect.ddlBeforeSql,
          tableRowsBefore: rowsBeforeInverse,
        });
      } else {
        inverseOperations.push({
          type: "alter_column_type",
          table: effect.tableName,
          column: effect.columnName ?? "unknown_column",
          newType: "TEXT",
        });
        inverseSchemaEffects.push({
          tableName: effect.tableName,
          columnName: effect.columnName,
          opKind: "alter_column_type",
          ddlBeforeSql: effect.ddlAfterSql,
          ddlAfterSql: effect.ddlBeforeSql,
          tableRowsBefore: rowsBeforeInverse,
        });
      }
      continue;
    }

    if (effect.opKind === "create_table") {
      const rowsBeforeInverse = fetchAllRows(db, effect.tableName);
      db.run(`DROP TABLE IF EXISTS ${quoteIdentifier(effect.tableName)}`);
      inverseOperations.push({ type: "drop_table", table: effect.tableName });
      inverseSchemaEffects.push({
        tableName: effect.tableName,
        columnName: null,
        opKind: "drop_table",
        ddlBeforeSql: effect.ddlAfterSql,
        ddlAfterSql: effect.ddlBeforeSql,
        tableRowsBefore: rowsBeforeInverse,
      });
    }
  }

  for (const effect of rowEffects.toReversed()) {
    if (effect.opKind === "insert") {
      const where = effect.pk;
      const { clause, bindings } = whereClauseFromRecord(where);
      db.query(`DELETE FROM ${quoteIdentifier(effect.tableName)} WHERE ${clause}`).run(...bindings);
      inverseOperations.push({ type: "delete", table: effect.tableName, where });
      inverseRowEffects.push({
        tableName: effect.tableName,
        pk: effect.pk,
        opKind: "delete",
        beforeRow: effect.afterRow,
        afterRow: null,
      });
      continue;
    }

    if (effect.opKind === "delete") {
      if (!effect.beforeRow) {
        continue;
      }
      const values = { ...effect.beforeRow };
      delete values.__toss_rowid;
      const keys = Object.keys(values);
      if (keys.length > 0) {
        const columns = keys.map((key) => quoteIdentifier(key)).join(", ");
        const placeholders = keys.map(() => "?").join(", ");
        const bindings = keys.map((key) => values[key] as JsonPrimitive);
        db.query(`INSERT INTO ${quoteIdentifier(effect.tableName)} (${columns}) VALUES (${placeholders})`).run(...bindings);
      }
      inverseOperations.push({ type: "insert", table: effect.tableName, values: values as Record<string, JsonPrimitive> });
      inverseRowEffects.push({
        tableName: effect.tableName,
        pk: effect.pk,
        opKind: "insert",
        beforeRow: null,
        afterRow: effect.beforeRow,
      });
      continue;
    }

    if (effect.opKind === "update") {
      if (!effect.beforeRow) {
        continue;
      }
      const values: Record<string, JsonPrimitive> = {};
      for (const [key, value] of Object.entries(effect.beforeRow)) {
        if (key === "__toss_rowid") {
          continue;
        }
        if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
          values[key] = value;
        }
      }
      const valueKeys = Object.keys(values);
      if (valueKeys.length > 0) {
        const setSql = valueKeys.map((key) => `${quoteIdentifier(key)} = ?`).join(", ");
        const { clause, bindings } = whereClauseFromRecord(effect.pk);
        const setBindings = valueKeys.map((key) => {
          const value = values[key];
          if (value === undefined) {
            throw new TossError("REVERT_FAILED", `Missing value for inverse update: ${key}`);
          }
          return value;
        });
        db.query(`UPDATE ${quoteIdentifier(effect.tableName)} SET ${setSql} WHERE ${clause}`).run(...setBindings, ...bindings);
      }
      inverseOperations.push({ type: "update", table: effect.tableName, values, where: effect.pk });
      inverseRowEffects.push({
        tableName: effect.tableName,
        pk: effect.pk,
        opKind: "update",
        beforeRow: effect.afterRow,
        afterRow: effect.beforeRow,
      });
    }
  }

  return { operations: inverseOperations, rowEffects: inverseRowEffects, schemaEffects: inverseSchemaEffects };
}

export function revertCommit(commitId: string, options: ServiceOptions = {}): RevertResult {
  const { db, dbPath } = openDatabase(options.dbPath);
  try {
    assertInitialized(db, dbPath);
    const targetCommit = getCommitById(db, commitId);
    if (!targetCommit) {
      throw new TossError("NOT_FOUND", `Commit not found: ${commitId}`);
    }
    if (!targetCommit.inverseReady) {
      throw new TossError("REVERT_UNSUPPORTED", `Commit ${commitId} has no inverse metadata`);
    }

    const already = db
      .query("SELECT 1 AS ok FROM _toss_commit WHERE kind='revert' AND reverted_target_id=? LIMIT 1")
      .get(commitId) as { ok?: number } | null;
    if (already?.ok === 1) {
      throw new TossError("ALREADY_REVERTED", `Commit is already reverted: ${commitId}`);
    }

    const targetRows = getRowEffectsByCommitId(db, commitId);
    const targetSchemas = getSchemaEffectsByCommitId(db, commitId);
    const later = fetchLaterEffects(db, targetCommit.seq);
    const conflicts = [...detectRowConflict(db, targetRows, later.rows), ...hasLaterSchemaChange(targetSchemas, later.schemas)];
    if (conflicts.length > 0) {
      return { ok: false, conflicts };
    }

    const result = runInTransaction(db, () => {
      const parent = getHeadCommit(db);
      const parentIds = parent ? [parent.commitId] : [];
      const seq = getNextCommitSeq(db);
      const createdAt = new Date().toISOString();
      const schemaHashBefore = schemaHash(db);
      const inverse = applyInverseEffects(db, targetCommit);
      const schemaHashAfter = schemaHash(db);
      const stateHashAfter = stateHash(db);
      const planHash = sha256Hex(inverse.operations);
      return appendCommit(db, {
        seq,
        kind: "revert",
        message: `Revert ${targetCommit.commitId}: ${targetCommit.message}`,
        createdAt,
        parentIds,
        schemaHashBefore,
        schemaHashAfter,
        stateHashAfter,
        planHash,
        inverseReady: true,
        revertedTargetId: targetCommit.commitId,
        operations: inverse.operations,
        rowEffects: inverse.rowEffects,
        schemaEffects: inverse.schemaEffects,
      });
    });
    return { ok: true, revertCommit: result };
  } finally {
    closeDatabase(db);
  }
}

export function verifyDatabase(options: ServiceOptions & { full?: boolean } = {}): VerifyResult {
  const { db, dbPath } = openDatabase(options.dbPath);
  try {
    assertInitialized(db, dbPath);
    const mode = options.full ? "full" : "quick";
    const issues: string[] = [];

    const commits = listCommits(db, false);
    for (const commit of commits) {
      const expected = sha256Hex({
        seq: commit.seq,
        kind: commit.kind,
        message: commit.message,
        createdAt: commit.createdAt,
        parentIds: commit.parentIds,
        schemaHashBefore: commit.schemaHashBefore,
        schemaHashAfter: commit.schemaHashAfter,
        stateHashAfter: commit.stateHashAfter,
        planHash: commit.planHash,
        inverseReady: commit.inverseReady,
        revertedTargetId: commit.revertedTargetId,
        operations: commit.operations,
      });
      if (expected !== commit.commitId) {
        issues.push(`Commit hash mismatch: ${commit.commitId}`);
      }
      if (commit.parentCount !== commit.parentIds.length) {
        issues.push(`Parent count mismatch: ${commit.commitId}`);
      }
    }

    const quickCheckRow = db.query("PRAGMA quick_check").get() as { quick_check: string } | null;
    const quickCheck = quickCheckRow?.quick_check ?? "unknown";
    if (quickCheck.toLowerCase() !== "ok") {
      issues.push(`quick_check failed: ${quickCheck}`);
    }

    let integrityCheck: string | undefined;
    if (options.full) {
      const integrityRow = db.query("PRAGMA integrity_check").get() as { integrity_check: string } | null;
      integrityCheck = integrityRow?.integrity_check ?? "unknown";
      if (integrityCheck.toLowerCase() !== "ok") {
        issues.push(`integrity_check failed: ${integrityCheck}`);
      }
    }

    const checkedAt = new Date().toISOString();
    putMeta(db, "last_verified_at", checkedAt);

    return {
      ok: issues.length === 0,
      mode,
      chainValid: !issues.some((issue) => issue.startsWith("Commit hash mismatch") || issue.startsWith("Parent count mismatch")),
      quickCheck,
      integrityCheck,
      issues,
      checkedAt,
    };
  } finally {
    closeDatabase(db);
  }
}

export function listSnapshots(options: ServiceOptions = {}): SnapshotEntry[] {
  const { db, dbPath } = openDatabase(options.dbPath);
  try {
    assertInitialized(db, dbPath);
    const rows = db
      .query("SELECT commit_id, file_path, file_sha256, created_at, row_count_hint FROM _toss_snapshot ORDER BY created_at DESC")
      .all() as Array<{
      commit_id: string;
      file_path: string;
      file_sha256: string;
      created_at: string;
      row_count_hint: number;
    }>;
    return rows.map((row) => ({
      commitId: row.commit_id,
      filePath: row.file_path,
      fileSha256: row.file_sha256,
      createdAt: row.created_at,
      rowCountHint: row.row_count_hint,
    }));
  } finally {
    closeDatabase(db);
  }
}

export async function recoverFromSnapshot(
  commitId: string,
  options: ServiceOptions = {},
): Promise<{ dbPath: string; restoredCommitId: string; replayedCommits: number }> {
  const { db, dbPath } = openDatabase(options.dbPath);
  let snapshotPath: string | null = null;
  let targetSeq = 0;
  let replayCommits: Array<{ message: string; operations: Operation[] }> = [];
  try {
    assertInitialized(db, dbPath);
    const snapshot = db
      .query(
        `
        SELECT s.file_path, c.seq
        FROM _toss_snapshot s
        JOIN _toss_commit c ON c.commit_id = s.commit_id
        WHERE s.commit_id=?
        LIMIT 1
        `,
      )
      .get(commitId) as { file_path: string; seq: number } | null;
    if (!snapshot) {
      throw new TossError("NOT_FOUND", `Snapshot not found for commit: ${commitId}`);
    }
    snapshotPath = snapshot.file_path;
    targetSeq = snapshot.seq;

    const laterRows = db
      .query("SELECT commit_id, message FROM _toss_commit WHERE seq > ? ORDER BY seq ASC")
      .all(targetSeq) as Array<{ commit_id: string; message: string }>;
    replayCommits = laterRows.map((row) => ({
      message: row.message,
      operations: db
        .query("SELECT op_json FROM _toss_op WHERE commit_id=? ORDER BY op_index ASC")
        .all(row.commit_id)
        .map((op) => JSON.parse((op as { op_json: string }).op_json) as Operation),
    }));
  } finally {
    closeDatabase(db);
  }

  if (!snapshotPath) {
    throw new TossError("RECOVER_FAILED", `Snapshot path missing for commit: ${commitId}`);
  }

  await removeExistingDbFiles(dbPath);
  await cp(snapshotPath, dbPath);

  if (replayCommits.length === 0) {
    return { dbPath, restoredCommitId: commitId, replayedCommits: 0 };
  }

  for (const replay of replayCommits) {
    const { db: replayDb, dbPath: replayDbPath } = openDatabase(dbPath);
    try {
      assertInitialized(replayDb, replayDbPath);
      runInTransaction(replayDb, () =>
        buildCommitOperationsResult(replayDb, replay.operations, "apply", `[replay] ${replay.message}`, null),
      );
    } finally {
      closeDatabase(replayDb);
    }
  }

  return { dbPath, restoredCommitId: commitId, replayedCommits: replayCommits.length };
}
