import type { Database } from "bun:sqlite";
import { canonicalJson, sha256Hex } from "./checksum";
import {
  assertInitialized,
  closeDatabase,
  COMMIT_TABLE,
  openDatabase,
  runInTransaction,
} from "./db";
import { TossError } from "./errors";
import {
  appendCommit,
  getCommitById,
  getHeadCommit,
  getNextCommitSeq,
  getRowEffectsByCommitId,
  getSchemaEffectsByCommitId,
  type RowEffect,
  type SchemaEffect,
  type StoredRowEffect,
  type StoredSchemaEffect,
} from "./log";
import {
  fetchAllRows,
  fetchRowByPk,
  normalizeRowObject,
  rowHash,
  schemaHash,
  stateHash,
  whereClauseFromRecord,
} from "./rows";
import { quoteIdentifier } from "./sql";
import type {
  CommitEntry,
  JsonObject,
  JsonPrimitive,
  Operation,
  RevertConflict,
  RevertResult,
  ServiceOptions,
} from "./types";

export function detectSchemaConflicts(
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

export function detectRowConflict(
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

export function fetchLaterEffects(db: Database, seq: number): { rows: StoredRowEffect[]; schemas: StoredSchemaEffect[] } {
  const laterCommits = db
    .query(`SELECT commit_id FROM ${COMMIT_TABLE} WHERE seq > ? ORDER BY seq ASC`)
    .all(seq) as Array<{ commit_id: string }>;
  const rows: StoredRowEffect[] = [];
  const schemas: StoredSchemaEffect[] = [];
  for (const commit of laterCommits) {
    rows.push(...getRowEffectsByCommitId(db, commit.commit_id));
    schemas.push(...getSchemaEffectsByCommitId(db, commit.commit_id));
  }
  return { rows, schemas };
}

export function reconstructTableFromDdl(db: Database, tableName: string, ddlSql: string, rowsBefore: JsonObject[] | null): void {
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

export function applyInverseEffects(db: Database, targetCommit: CommitEntry): {
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

      const columnName = effect.columnName ?? "unknown_column";
      let inverseOp: Operation;
      let inverseOpKind: SchemaEffect["opKind"];
      if (effect.opKind === "drop_column") {
        inverseOp = { type: "add_column", table: effect.tableName, column: { name: columnName, type: "TEXT" } };
        inverseOpKind = "add_column";
      } else if (effect.opKind === "add_column") {
        inverseOp = { type: "drop_column", table: effect.tableName, column: columnName };
        inverseOpKind = "drop_column";
      } else {
        inverseOp = { type: "alter_column_type", table: effect.tableName, column: columnName, newType: "TEXT" };
        inverseOpKind = "alter_column_type";
      }

      inverseOperations.push(inverseOp);
      inverseSchemaEffects.push({
        tableName: effect.tableName,
        columnName: effect.columnName,
        opKind: inverseOpKind,
        ddlBeforeSql: effect.ddlAfterSql,
        ddlAfterSql: effect.ddlBeforeSql,
        tableRowsBefore: rowsBeforeInverse,
      });
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
      .query(`SELECT 1 AS ok FROM ${COMMIT_TABLE} WHERE kind='revert' AND reverted_target_id=? LIMIT 1`)
      .get(commitId) as { ok?: number } | null;
    if (already?.ok === 1) {
      throw new TossError("ALREADY_REVERTED", `Commit is already reverted: ${commitId}`);
    }

    const targetRows = getRowEffectsByCommitId(db, commitId);
    const targetSchemas = getSchemaEffectsByCommitId(db, commitId);
    const later = fetchLaterEffects(db, targetCommit.seq);
    const conflicts = [...detectRowConflict(db, targetRows, later.rows), ...detectSchemaConflicts(targetSchemas, later.schemas)];
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
