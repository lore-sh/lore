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
import { Operation, type Operation as Op } from "./operation";
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
  options: { errorCode?: ErrorCode } = {},
) {
  const errorCode = options.errorCode ?? "RECOVER_FAILED";
  const { commitId, ...payload } = input;
  const expected = computeCommitId(payload);
  if (expected !== input.commitId) {
    throw new CodedError(errorCode, `Commit payload mismatch for replayed commit ${input.commitId}`);
  }

  const oldHead = headCommit(db)?.commitId ?? null;
  const commit = Commit.parse({
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
  });

  db.insert(CommitTable).values(commit).run();

  for (let i = 0; i < input.parentIds.length; i += 1) {
    db.insert(CommitParentTable)
      .values({ commitId: input.commitId, parentCommitId: input.parentIds[i]!, ord: i })
      .run();
  }

  for (let i = 0; i < input.operations.length; i += 1) {
    const operation = input.operations[i]!;
    db.insert(OpTable)
      .values({ commitId: input.commitId, opIndex: i, opType: operation.type, opJson: canonicalJson(operation) })
      .run();
  }

  for (let i = 0; i < input.rowEffects.length; i += 1) {
    const effect = input.rowEffects[i]!;
    const beforeJson = effect.beforeRow ? canonicalJson(effect.beforeRow) : null;
    const afterJson = effect.afterRow ? canonicalJson(effect.afterRow) : null;
    db.insert(RowEffectTable)
      .values({
        commitId: input.commitId,
        effectIndex: i,
        tableName: effect.tableName,
        pkJson: canonicalJson(effect.pk),
        opKind: effect.opKind,
        beforeJson,
        afterJson,
        beforeHash: beforeJson ? sha256Hex(beforeJson) : null,
        afterHash: afterJson ? sha256Hex(afterJson) : null,
      })
      .run();
  }

  for (let i = 0; i < input.schemaEffects.length; i += 1) {
    const effect = input.schemaEffects[i]!;
    db.insert(SchemaEffectTable)
      .values({
        commitId: input.commitId,
        effectIndex: i,
        tableName: effect.tableName,
        beforeJson: effect.beforeTable ? canonicalJson(effect.beforeTable) : null,
        afterJson: effect.afterTable ? canonicalJson(effect.afterTable) : null,
      })
      .run();
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

export function findCommit(db: Database, commitId: string) {
  const row = db.select().from(CommitTable).where(eq(CommitTable.commitId, commitId)).limit(1).get();
  return row ? Commit.parse(row) : null;
}

export function listCommits(db: Database, descending: boolean) {
  const rows = db
    .select()
    .from(CommitTable)
    .orderBy(descending ? desc(CommitTable.seq) : asc(CommitTable.seq))
    .all();
  return rows.map((row) => Commit.parse(row));
}

export function commitOperations(db: Database, commitId: string) {
  return db
    .select({ opJson: OpTable.opJson })
    .from(OpTable)
    .where(eq(OpTable.commitId, commitId))
    .orderBy(asc(OpTable.opIndex))
    .all()
    .map((row) => Operation.parse(JSON.parse(row.opJson)));
}

export function commitRowEffects(db: Database, commitId: string) {
  return db
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
}

export function decodeRowEffects(rows: ReturnType<typeof commitRowEffects>) {
  return rows.map((row) => {
    const beforeRow = row.beforeJson ? JSON.parse(row.beforeJson) : null;
    const afterRow = row.afterJson ? JSON.parse(row.afterJson) : null;
    return RowEffect.parse({
      tableName: row.tableName,
      pk: JSON.parse(row.pkJson),
      opKind: row.opKind,
      beforeRow,
      afterRow,
      beforeHash: beforeRow ? sha256Hex(beforeRow) : null,
      afterHash: afterRow ? sha256Hex(afterRow) : null,
    });
  });
}

export function commitSchemaEffects(db: Database, commitId: string) {
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
    .map((row) =>
      SchemaEffect.parse({
        tableName: row.tableName,
        beforeTable: row.beforeJson ? JSON.parse(row.beforeJson) : null,
        afterTable: row.afterJson ? JSON.parse(row.afterJson) : null,
      }),
    );
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
    rowEffects: decodeRowEffects(commitRowEffects(db, commitId)),
    schemaEffects: commitSchemaEffects(db, commitId),
  };
}

export function readCommitsAfter(db: Database, fromSeqExclusive: number) {
  const rows = db
    .select({ commitId: CommitTable.commitId })
    .from(CommitTable)
    .where(gt(CommitTable.seq, fromSeqExclusive))
    .orderBy(asc(CommitTable.seq))
    .all();
  return rows.map((row) => readCommit(db, row.commitId));
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

  writeCommit(
    db,
    {
      commitId: replay.commit.commitId,
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
    },
    { errorCode },
  );
}
