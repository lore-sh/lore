import { asc, desc, eq, gt, sql } from "drizzle-orm";
import { z } from "zod";
import { MAIN_REF_NAME, type Database } from "./db";
import {
  applyEffects,
  applyRowEffects,
  assertForeignKeys,
  captureState,
  diffState,
  RowEffect,
  SchemaEffect,
} from "./effect";
import { CodedError, type ErrorCode } from "./error";
import { canonicalJson, sha256Hex } from "./hash";
import { schemaHash, stateHash } from "./inspect";
import { type Operation as Op } from "./operation";
import {
  CommitKind,
  CommitParentTable,
  CommitTable,
  OpTable,
  RefTable,
  ReflogTable,
  RowEffectTable,
  SchemaEffectTable,
} from "./schema";

export const Commit = z.object({
  commitId: z.string(),
  seq: z.number(),
  kind: CommitKind,
  message: z.string(),
  createdAt: z.number(),
  parentCount: z.number(),
  schemaHashBefore: z.string(),
  schemaHashAfter: z.string(),
  stateHashAfter: z.string(),
  planHash: z.string(),
  revertible: z.number(),
  revertTargetId: z.string().nullable(),
});
export type Commit = z.infer<typeof Commit>;

const SQLITE_VARIABLE_LIMIT = 999;

function chunkSizeForInsert(columnsPerRow: number): number {
  return Math.max(1, Math.floor(SQLITE_VARIABLE_LIMIT / columnsPerRow));
}

const COMMIT_PARENT_INSERT_CHUNK_SIZE = chunkSizeForInsert(3);
const OP_INSERT_CHUNK_SIZE = chunkSizeForInsert(4);
const ROW_EFFECT_INSERT_CHUNK_SIZE = chunkSizeForInsert(9);
const SCHEMA_EFFECT_INSERT_CHUNK_SIZE = chunkSizeForInsert(5);

export function computeCommitId(input: {
  seq: number;
  kind: Commit["kind"];
  message: string;
  createdAt: number;
  schemaHashBefore: string;
  schemaHashAfter: string;
  stateHashAfter: string;
  planHash: string;
  revertible: number;
  revertTargetId: string | null;
  parentIds: string[];
  operations: Op[];
  rowEffects: RowEffect[];
  schemaEffects: SchemaEffect[];
}) {
  return sha256Hex({
    seq: input.seq,
    kind: input.kind,
    message: input.message,
    createdAt: input.createdAt,
    schemaHashBefore: input.schemaHashBefore,
    schemaHashAfter: input.schemaHashAfter,
    stateHashAfter: input.stateHashAfter,
    planHash: input.planHash,
    revertible: input.revertible,
    revertTargetId: input.revertTargetId,
    parentIds: input.parentIds,
    operations: input.operations,
    rowEffects: input.rowEffects.map((effect) => ({
      tableName: effect.tableName,
      pk: effect.pk,
      opKind: effect.opKind,
      beforeRow: effect.beforeRow,
      afterRow: effect.afterRow,
    })),
    schemaEffects: input.schemaEffects.map((effect) => ({
      tableName: effect.tableName,
      beforeTable: effect.beforeTable,
      afterTable: effect.afterTable,
    })),
  });
}

function writeCommit(
  db: Database,
  input: {
    commitId: string;
    seq: number;
    kind: Commit["kind"];
    message: string;
    createdAt: number;
    schemaHashBefore: string;
    schemaHashAfter: string;
    stateHashAfter: string;
    planHash: string;
    revertible: number;
    revertTargetId: string | null;
    parentIds: string[];
    operations: Op[];
    rowEffects: RowEffect[];
    schemaEffects: SchemaEffect[];
  },
) {
  const oldHead = headCommit(db)?.commitId ?? null;
  const commit: Commit = {
    commitId: input.commitId,
    seq: input.seq,
    kind: input.kind,
    message: input.message,
    createdAt: input.createdAt,
    parentCount: input.parentIds.length,
    schemaHashBefore: input.schemaHashBefore,
    schemaHashAfter: input.schemaHashAfter,
    stateHashAfter: input.stateHashAfter,
    planHash: input.planHash,
    revertible: input.revertible,
    revertTargetId: input.revertTargetId,
  };

  db.insert(CommitTable).values(commit).run();

  if (input.parentIds.length > 0) {
    for (let start = 0; start < input.parentIds.length; start += COMMIT_PARENT_INSERT_CHUNK_SIZE) {
      const chunk = input.parentIds.slice(start, start + COMMIT_PARENT_INSERT_CHUNK_SIZE);
      db.insert(CommitParentTable)
        .values(
          chunk.map((parentCommitId, offset) => ({
            commitId: input.commitId,
            parentCommitId,
            ord: start + offset,
          })),
        )
        .run();
    }
  }

  if (input.operations.length > 0) {
    for (let start = 0; start < input.operations.length; start += OP_INSERT_CHUNK_SIZE) {
      const chunk = input.operations.slice(start, start + OP_INSERT_CHUNK_SIZE);
      db.insert(OpTable)
        .values(
          chunk.map((operation, offset) => ({
            commitId: input.commitId,
            opIndex: start + offset,
            opType: operation.type,
            opJson: canonicalJson(operation),
          })),
        )
        .run();
    }
  }

  if (input.rowEffects.length > 0) {
    for (let start = 0; start < input.rowEffects.length; start += ROW_EFFECT_INSERT_CHUNK_SIZE) {
      const chunk = input.rowEffects.slice(start, start + ROW_EFFECT_INSERT_CHUNK_SIZE);
      db.insert(RowEffectTable)
        .values(
          chunk.map((effect, offset) => {
            const beforeJson = effect.beforeRow ? canonicalJson(effect.beforeRow) : null;
            const afterJson = effect.afterRow ? canonicalJson(effect.afterRow) : null;
            return {
              commitId: input.commitId,
              effectIndex: start + offset,
              tableName: effect.tableName,
              pkJson: canonicalJson(effect.pk),
              opKind: effect.opKind,
              beforeJson,
              afterJson,
              beforeHash: beforeJson ? sha256Hex(beforeJson) : null,
              afterHash: afterJson ? sha256Hex(afterJson) : null,
            };
          }),
        )
        .run();
    }
  }

  if (input.schemaEffects.length > 0) {
    for (let start = 0; start < input.schemaEffects.length; start += SCHEMA_EFFECT_INSERT_CHUNK_SIZE) {
      const chunk = input.schemaEffects.slice(start, start + SCHEMA_EFFECT_INSERT_CHUNK_SIZE);
      db.insert(SchemaEffectTable)
        .values(
          chunk.map((effect, offset) => ({
            commitId: input.commitId,
            effectIndex: start + offset,
            tableName: effect.tableName,
            beforeJson: effect.beforeTable ? canonicalJson(effect.beforeTable) : null,
            afterJson: effect.afterTable ? canonicalJson(effect.afterTable) : null,
          })),
        )
        .run();
    }
  }

  db.update(RefTable)
    .set({ commitId: input.commitId, updatedAt: input.createdAt })
    .where(eq(RefTable.name, MAIN_REF_NAME))
    .run();

  db.insert(ReflogTable)
    .values({
      refName: MAIN_REF_NAME,
      oldCommitId: oldHead,
      newCommitId: input.commitId,
      reason: input.kind === "revert" ? "revert" : "apply",
      createdAt: input.createdAt,
    })
    .run();

  return commit;
}

export function headCommit(db: Database) {
  const row = db
    .select({ commitId: RefTable.commitId })
    .from(RefTable)
    .where(eq(RefTable.name, MAIN_REF_NAME))
    .limit(1)
    .get();
  if (!row?.commitId) {
    return null;
  }
  return findCommit(db, row.commitId);
}

export function findCommit(db: Database, commitId: string): Commit | null {
  return db.select().from(CommitTable).where(eq(CommitTable.commitId, commitId)).limit(1).get() ?? null;
}

export function listCommits(db: Database, descending: boolean): Commit[] {
  return db
    .select()
    .from(CommitTable)
    .orderBy(descending ? desc(CommitTable.seq) : asc(CommitTable.seq))
    .all();
}

export function commitOperations(db: Database, commitId: string): Op[] {
  return db
    .select({ opJson: OpTable.opJson })
    .from(OpTable)
    .where(eq(OpTable.commitId, commitId))
    .orderBy(asc(OpTable.opIndex))
    .all()
    .map((row) => {
      const operation: Op = JSON.parse(row.opJson);
      return operation;
    });
}

function decodeRowEffects(
  rows: Array<{
    tableName: string;
    pkJson: string;
    opKind: RowEffect["opKind"];
    beforeJson: string | null;
    afterJson: string | null;
    beforeHash: string | null;
    afterHash: string | null;
  }>,
): RowEffect[] {
  return rows.map((row) => {
    const pk: Record<string, string> = JSON.parse(row.pkJson);
    const beforeRow: RowEffect["beforeRow"] = row.beforeJson ? JSON.parse(row.beforeJson) : null;
    const afterRow: RowEffect["afterRow"] = row.afterJson ? JSON.parse(row.afterJson) : null;
    if ((beforeRow === null) !== (row.beforeHash === null)) {
      throw new CodedError("INTERNAL", "row effect before_hash is inconsistent with before_json");
    }
    if ((afterRow === null) !== (row.afterHash === null)) {
      throw new CodedError("INTERNAL", "row effect after_hash is inconsistent with after_json");
    }
    if (beforeRow !== null) {
      const computed = sha256Hex(beforeRow);
      if (row.beforeHash !== computed) {
        throw new CodedError("INTERNAL", "row effect before_hash does not match before_json");
      }
    }
    if (afterRow !== null) {
      const computed = sha256Hex(afterRow);
      if (row.afterHash !== computed) {
        throw new CodedError("INTERNAL", "row effect after_hash does not match after_json");
      }
    }
    return {
      tableName: row.tableName,
      pk,
      opKind: row.opKind,
      beforeRow,
      afterRow,
      beforeHash: row.beforeHash,
      afterHash: row.afterHash,
    };
  });
}

export function commitRowEffects(db: Database, commitId: string): RowEffect[] {
  const rows = db
    .select({
      tableName: RowEffectTable.tableName,
      pkJson: RowEffectTable.pkJson,
      opKind: RowEffectTable.opKind,
      beforeJson: RowEffectTable.beforeJson,
      afterJson: RowEffectTable.afterJson,
      beforeHash: RowEffectTable.beforeHash,
      afterHash: RowEffectTable.afterHash,
    })
    .from(RowEffectTable)
    .where(eq(RowEffectTable.commitId, commitId))
    .orderBy(asc(RowEffectTable.effectIndex))
    .all();
  return decodeRowEffects(rows);
}

export function commitSchemaEffects(db: Database, commitId: string): SchemaEffect[] {
  return db
    .select({
      tableName: SchemaEffectTable.tableName,
      beforeJson: SchemaEffectTable.beforeJson,
      afterJson: SchemaEffectTable.afterJson,
    })
    .from(SchemaEffectTable)
    .where(eq(SchemaEffectTable.commitId, commitId))
    .orderBy(asc(SchemaEffectTable.effectIndex))
    .all()
    .map((row) => {
      const beforeTable: SchemaEffect["beforeTable"] = row.beforeJson ? JSON.parse(row.beforeJson) : null;
      const afterTable: SchemaEffect["afterTable"] = row.afterJson ? JSON.parse(row.afterJson) : null;
      return { tableName: row.tableName, beforeTable, afterTable };
    });
}

export function commitSeq(db: Database, commitId: string) {
  const row = db
    .select({ seq: CommitTable.seq })
    .from(CommitTable)
    .where(eq(CommitTable.commitId, commitId))
    .limit(1)
    .get();
  return row?.seq ?? null;
}

export function readCommit(db: Database, commitId: string) {
  const commit = findCommit(db, commitId);
  if (!commit) {
    throw new CodedError("NOT_FOUND", `Commit not found: ${commitId}`);
  }
  const parentIds = db
    .select({ parentCommitId: CommitParentTable.parentCommitId })
    .from(CommitParentTable)
    .where(eq(CommitParentTable.commitId, commitId))
    .orderBy(asc(CommitParentTable.ord))
    .all()
    .map((row) => row.parentCommitId);
  return {
    commit,
    parentIds,
    operations: commitOperations(db, commitId),
    rowEffects: commitRowEffects(db, commitId),
    schemaEffects: commitSchemaEffects(db, commitId),
  };
}

export function readCommitsAfter(db: Database, fromSeqExclusive: number) {
  const commits = db
    .select()
    .from(CommitTable)
    .where(gt(CommitTable.seq, fromSeqExclusive))
    .orderBy(asc(CommitTable.seq))
    .all();
  if (commits.length === 0) {
    return [];
  }

  const parentRows = db
    .select({
      commitId: CommitParentTable.commitId,
      parentCommitId: CommitParentTable.parentCommitId,
    })
    .from(CommitParentTable)
    .innerJoin(CommitTable, eq(CommitTable.commitId, CommitParentTable.commitId))
    .where(gt(CommitTable.seq, fromSeqExclusive))
    .orderBy(asc(CommitTable.seq), asc(CommitParentTable.ord))
    .all();
  const parentsByCommit = new Map<string, string[]>();
  for (const row of parentRows) {
    const parentIds = parentsByCommit.get(row.commitId) ?? [];
    parentIds.push(row.parentCommitId);
    parentsByCommit.set(row.commitId, parentIds);
  }

  const operationRows = db
    .select({
      commitId: OpTable.commitId,
      opJson: OpTable.opJson,
    })
    .from(OpTable)
    .innerJoin(CommitTable, eq(CommitTable.commitId, OpTable.commitId))
    .where(gt(CommitTable.seq, fromSeqExclusive))
    .orderBy(asc(CommitTable.seq), asc(OpTable.opIndex))
    .all();
  const operationsByCommit = new Map<string, Op[]>();
  for (const row of operationRows) {
    const operations = operationsByCommit.get(row.commitId) ?? [];
    const operation: Op = JSON.parse(row.opJson);
    operations.push(operation);
    operationsByCommit.set(row.commitId, operations);
  }

  const rowEffectRows = db
    .select({
      commitId: RowEffectTable.commitId,
      tableName: RowEffectTable.tableName,
      pkJson: RowEffectTable.pkJson,
      opKind: RowEffectTable.opKind,
      beforeJson: RowEffectTable.beforeJson,
      afterJson: RowEffectTable.afterJson,
      beforeHash: RowEffectTable.beforeHash,
      afterHash: RowEffectTable.afterHash,
    })
    .from(RowEffectTable)
    .innerJoin(CommitTable, eq(CommitTable.commitId, RowEffectTable.commitId))
    .where(gt(CommitTable.seq, fromSeqExclusive))
    .orderBy(asc(CommitTable.seq), asc(RowEffectTable.effectIndex))
    .all();
  const rowEffectsByCommit = new Map<string, typeof rowEffectRows>();
  for (const row of rowEffectRows) {
    const rows = rowEffectsByCommit.get(row.commitId) ?? [];
    rows.push(row);
    rowEffectsByCommit.set(row.commitId, rows);
  }

  const schemaEffectRows = db
    .select({
      commitId: SchemaEffectTable.commitId,
      tableName: SchemaEffectTable.tableName,
      beforeJson: SchemaEffectTable.beforeJson,
      afterJson: SchemaEffectTable.afterJson,
    })
    .from(SchemaEffectTable)
    .innerJoin(CommitTable, eq(CommitTable.commitId, SchemaEffectTable.commitId))
    .where(gt(CommitTable.seq, fromSeqExclusive))
    .orderBy(asc(CommitTable.seq), asc(SchemaEffectTable.effectIndex))
    .all();
  const schemaEffectsByCommit = new Map<string, SchemaEffect[]>();
  for (const row of schemaEffectRows) {
    const effects = schemaEffectsByCommit.get(row.commitId) ?? [];
    const beforeTable: SchemaEffect["beforeTable"] = row.beforeJson ? JSON.parse(row.beforeJson) : null;
    const afterTable: SchemaEffect["afterTable"] = row.afterJson ? JSON.parse(row.afterJson) : null;
    effects.push({ tableName: row.tableName, beforeTable, afterTable });
    schemaEffectsByCommit.set(row.commitId, effects);
  }

  return commits.map((commit) => ({
    commit,
    parentIds: parentsByCommit.get(commit.commitId) ?? [],
    operations: operationsByCommit.get(commit.commitId) ?? [],
    rowEffects: decodeRowEffects(rowEffectsByCommit.get(commit.commitId) ?? []),
    schemaEffects: schemaEffectsByCommit.get(commit.commitId) ?? [],
  }));
}

export function createCommit(
  db: Database,
  input: {
    operations: Op[];
    kind: "apply" | "revert";
    message: string;
    revertTargetId: string | null;
    beforeSchemaHash: string;
    beforeState: ReturnType<typeof captureState>;
  },
) {
  const parent = headCommit(db);
  const parentIds = parent ? [parent.commitId] : [];
  const seqRow = db
    .select({ n: sql<number>`coalesce(max(${CommitTable.seq}), 0) + 1` })
    .from(CommitTable)
    .get();
  const seq = seqRow?.n ?? 1;
  const createdAt = Date.now();
  const afterState = captureState(db);
  const { rowEffects, schemaEffects } = diffState(input.beforeState, afterState);
  const payload = {
    seq,
    kind: input.kind,
    message: input.message,
    createdAt,
    schemaHashBefore: input.beforeSchemaHash,
    schemaHashAfter: schemaHash(db),
    stateHashAfter: stateHash(db),
    planHash: sha256Hex(input.operations),
    revertible: 1,
    revertTargetId: input.revertTargetId,
    parentIds,
    operations: input.operations,
    rowEffects,
    schemaEffects,
  } as const;
  const commitId = computeCommitId(payload);
  return writeCommit(db, { ...payload, commitId });
}

export function replayCommit(
  db: Database,
  replay: {
    commit: Commit;
    parentIds: string[];
    operations: Op[];
    rowEffects: RowEffect[];
    schemaEffects: SchemaEffect[];
  },
  options: { errorCode?: ErrorCode } = {},
) {
  const errorCode = options.errorCode ?? "RECOVER_FAILED";
  const beforeSchemaHash = schemaHash(db);
  if (beforeSchemaHash !== replay.commit.schemaHashBefore) {
    throw new CodedError(
      errorCode,
      `schema_hash_before mismatch for replay ${replay.commit.commitId}: expected ${replay.commit.schemaHashBefore}, got ${beforeSchemaHash}`,
    );
  }

  const computedPlanHash = sha256Hex(replay.operations);
  if (computedPlanHash !== replay.commit.planHash) {
    throw new CodedError(
      errorCode,
      `plan_hash mismatch for replay ${replay.commit.commitId}: expected ${replay.commit.planHash}, got ${computedPlanHash}`,
    );
  }

  applyEffects(db, replay.rowEffects, replay.schemaEffects, "forward", {
    disableTableTriggers: true,
  });
  applyRowEffects(db, replay.rowEffects, "forward", {
    disableTableTriggers: true,
    includeUserEffects: false,
    includeSystemEffects: true,
    systemPolicy: "reconcile",
  });
  assertForeignKeys(db, errorCode, `replay ${replay.commit.commitId}`);

  const afterSchemaHash = schemaHash(db);
  if (afterSchemaHash !== replay.commit.schemaHashAfter) {
    throw new CodedError(
      errorCode,
      `schema_hash_after mismatch for replay ${replay.commit.commitId}: expected ${replay.commit.schemaHashAfter}, got ${afterSchemaHash}`,
    );
  }

  const afterStateHash = stateHash(db);
  if (afterStateHash !== replay.commit.stateHashAfter) {
    throw new CodedError(
      errorCode,
      `state_hash_after mismatch for replay ${replay.commit.commitId}: expected ${replay.commit.stateHashAfter}, got ${afterStateHash}`,
    );
  }

  const expectedCommitId = computeCommitId({
    seq: replay.commit.seq,
    kind: replay.commit.kind,
    message: replay.commit.message,
    createdAt: replay.commit.createdAt,
    schemaHashBefore: replay.commit.schemaHashBefore,
    schemaHashAfter: replay.commit.schemaHashAfter,
    stateHashAfter: replay.commit.stateHashAfter,
    planHash: replay.commit.planHash,
    revertible: replay.commit.revertible,
    revertTargetId: replay.commit.revertTargetId,
    parentIds: replay.parentIds,
    operations: replay.operations,
    rowEffects: replay.rowEffects,
    schemaEffects: replay.schemaEffects,
  });
  if (expectedCommitId !== replay.commit.commitId) {
    throw new CodedError(
      errorCode,
      `commitId mismatch for replay ${replay.commit.commitId}: expected ${expectedCommitId}`,
    );
  }

  const { parentCount: _, ...commitFields } = replay.commit;
  writeCommit(db, {
    ...commitFields,
    parentIds: replay.parentIds,
    operations: replay.operations,
    rowEffects: replay.rowEffects,
    schemaEffects: replay.schemaEffects,
  });
}
