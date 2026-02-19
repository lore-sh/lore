import type { Database } from "bun:sqlite";
import { ENGINE_META_TABLE, getRow, withInitializedDatabase } from "./db";
import { computeCommitId, getRowEffectsByCommitId, getSchemaEffectsByCommitId, listCommits } from "./log";
import type { DatabaseOptions, VerifyResult } from "./types";

export function putMeta(db: Database, key: string, value: string): void {
  db.query(
    `
    INSERT INTO ${ENGINE_META_TABLE}(key, value) VALUES(?, ?)
    ON CONFLICT(key) DO UPDATE SET value=excluded.value
    `,
  ).run(key, value);
}

export function verifyDatabase(options: DatabaseOptions & { full?: boolean } = {}): VerifyResult {
  return withInitializedDatabase(options, ({ db }) => {
    const mode = options.full ? "full" : "quick";
    const issues: string[] = [];

    const commits = listCommits(db, false);
    for (const commit of commits) {
      const expected = computeCommitId({
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
        rowEffects: getRowEffectsByCommitId(db, commit.commitId),
        schemaEffects: getSchemaEffectsByCommitId(db, commit.commitId),
      });
      if (expected !== commit.commitId) {
        issues.push(`Commit hash mismatch: ${commit.commitId}`);
      }
      if (commit.parentCount !== commit.parentIds.length) {
        issues.push(`Parent count mismatch: ${commit.commitId}`);
      }
    }

    const quickCheckRow = getRow<{ quick_check: string }>(db, "PRAGMA quick_check");
    const quickCheck = quickCheckRow?.quick_check ?? "unknown";
    if (quickCheck.toLowerCase() !== "ok") {
      issues.push(`quick_check failed: ${quickCheck}`);
    }

    let integrityCheck: string | undefined;
    if (options.full) {
      const integrityRow = getRow<{ integrity_check: string }>(db, "PRAGMA integrity_check");
      integrityCheck = integrityRow?.integrity_check ?? "unknown";
      if (integrityCheck.toLowerCase() !== "ok") {
        issues.push(`integrity_check failed: ${integrityCheck}`);
      }
    }

    const checkedAt = new Date().toISOString();
    putMeta(db, "last_verified_at", checkedAt);
    const ok = issues.length === 0;
    putMeta(db, "last_verified_ok", ok ? "1" : "0");
    if (ok) {
      putMeta(db, "last_verified_ok_at", checkedAt);
    }

    return {
      ok,
      mode,
      chainValid: !issues.some((issue) => issue.startsWith("Commit hash mismatch") || issue.startsWith("Parent count mismatch")),
      quickCheck,
      integrityCheck,
      issues,
      checkedAt,
    };
  });
}
