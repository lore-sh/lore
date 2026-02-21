import type { Database } from "bun:sqlite";
import { sha256Hex } from "./checksum";
import { appendCommitExact, type CommitReplayInput, getCommitById, getRowEffectsByCommitId, getSchemaEffectsByCommitId } from "./log";
import {
  applyRowEffectsWithOptions,
  applyUserRowAndSchemaEffects,
  assertNoForeignKeyViolations,
} from "./observed";
import { schemaHash, stateHash } from "./rows";
import { TossError } from "../errors";
import { asc, eq, gt } from "drizzle-orm";
import { createEngineDb } from "./client";
import { CommitTable } from "./schema.sql";

export function getCommitReplayInput(db: Database, commitId: string): CommitReplayInput {
  const commit = getCommitById(db, commitId);
  if (!commit) {
    throw new TossError("NOT_FOUND", `Commit not found: ${commitId}`);
  }
  const { parentCount: _, ...base } = commit;
  return {
    ...base,
    rowEffects: getRowEffectsByCommitId(db, commit.commitId),
    schemaEffects: getSchemaEffectsByCommitId(db, commit.commitId),
  };
}

export function loadCommitReplayInputs(db: Database, fromSeqExclusive: number): CommitReplayInput[] {
  const commitRows = createEngineDb(db)
    .select({ commitId: CommitTable.commitId })
    .from(CommitTable)
    .where(gt(CommitTable.seq, fromSeqExclusive))
    .orderBy(asc(CommitTable.seq))
    .all();
  return commitRows.map((row) => getCommitReplayInput(db, row.commitId));
}

export function findCommitSeq(db: Database, commitId: string): number | null {
  const row = createEngineDb(db)
    .select({ seq: CommitTable.seq })
    .from(CommitTable)
    .where(eq(CommitTable.commitId, commitId))
    .limit(1)
    .get();
  return row?.seq ?? null;
}

export function replayCommitExactly(
  db: Database,
  replay: CommitReplayInput,
  options: { errorCode?: string } = {},
): void {
  const errorCode = options.errorCode ?? "RECOVER_FAILED";
  const beforeSchemaHash = schemaHash(db);
  if (beforeSchemaHash !== replay.schemaHashBefore) {
    throw new TossError(
      errorCode,
      `schema_hash_before mismatch for replay ${replay.commitId}: expected ${replay.schemaHashBefore}, got ${beforeSchemaHash}`,
    );
  }

  const computedPlanHash = sha256Hex(replay.operations);
  if (computedPlanHash !== replay.planHash) {
    throw new TossError(
      errorCode,
      `plan_hash mismatch for replay ${replay.commitId}: expected ${replay.planHash}, got ${computedPlanHash}`,
    );
  }

  applyUserRowAndSchemaEffects(db, replay.rowEffects, replay.schemaEffects, "forward", {
    disableTableTriggers: true,
  });
  applyRowEffectsWithOptions(db, replay.rowEffects, "forward", {
    disableTableTriggers: true,
    includeUserEffects: false,
    includeSystemEffects: true,
    systemPolicy: "reconcile",
  });
  assertNoForeignKeyViolations(db, errorCode, `replay ${replay.commitId}`);

  const afterSchemaHash = schemaHash(db);
  if (afterSchemaHash !== replay.schemaHashAfter) {
    throw new TossError(
      errorCode,
      `schema_hash_after mismatch for replay ${replay.commitId}: expected ${replay.schemaHashAfter}, got ${afterSchemaHash}`,
    );
  }

  const afterStateHash = stateHash(db);
  if (afterStateHash !== replay.stateHashAfter) {
    throw new TossError(
      errorCode,
      `state_hash_after mismatch for replay ${replay.commitId}: expected ${replay.stateHashAfter}, got ${afterStateHash}`,
    );
  }

  appendCommitExact(db, replay, { errorCode });
}
