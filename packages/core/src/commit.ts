import type { Database } from "bun:sqlite";
import { sha256Hex } from "./checksum";
import { runInTransaction, withInitializedDatabaseAsync } from "./db";
import { executeOperation } from "./executors/apply";
import { appendCommit, getHeadCommit, getNextCommitSeq } from "./log";
import { captureObservedState, diffObservedState, type CapturedObservedState } from "./observed";
import { schemaHash, stateHash } from "./rows";
import type { CommitEntry, Operation } from "./types";
import { parseAndValidateOperationPlan } from "./validators/operation";

export function readPlanInput(planRef: string): Promise<string> {
  if (planRef === "-") {
    return Bun.stdin.text();
  }
  return Bun.file(planRef).text();
}

export function appendCommitFromObservedChange(
  db: Database,
  input: {
    operations: Operation[];
    kind: "apply" | "revert";
    message: string;
    revertedTargetId: string | null;
    beforeSchemaHash: string;
    beforeObservedState: CapturedObservedState;
  },
): CommitEntry {
  const parent = getHeadCommit(db);
  const parentIds = parent ? [parent.commitId] : [];
  const seq = getNextCommitSeq(db);
  const createdAt = Date.now();
  const afterObservedState = captureObservedState(db);
  const captured = diffObservedState(input.beforeObservedState, afterObservedState);
  const afterSchemaHash = schemaHash(db);
  const afterStateHash = stateHash(db);
  const planHash = sha256Hex(input.operations);

  return appendCommit(db, {
    seq,
    kind: input.kind,
    message: input.message,
    createdAt,
    parentIds,
    schemaHashBefore: input.beforeSchemaHash,
    schemaHashAfter: afterSchemaHash,
    stateHashAfter: afterStateHash,
    planHash,
    inverseReady: true,
    revertedTargetId: input.revertedTargetId,
    operations: input.operations,
    rowEffects: captured.rowEffects,
    schemaEffects: captured.schemaEffects,
  });
}

export async function applyPlan(planRef: string): Promise<CommitEntry> {
  const payload = await readPlanInput(planRef);
  const plan = parseAndValidateOperationPlan(payload);

  const commit = await withInitializedDatabaseAsync(async ({ db }) =>
    runInTransaction(db, () => {
      const beforeSchemaHash = schemaHash(db);
      const beforeObservedState = captureObservedState(db);
      for (const operation of plan.operations) {
        executeOperation(db, operation);
      }
      return appendCommitFromObservedChange(db, {
        operations: plan.operations,
        kind: "apply",
        message: plan.message,
        revertedTargetId: null,
        beforeSchemaHash,
        beforeObservedState,
      });
    }),
  );

  const { maybeCreateSnapshot } = await import("./snapshot");
  await maybeCreateSnapshot(commit);
  return commit;
}
