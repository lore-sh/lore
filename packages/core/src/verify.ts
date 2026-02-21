import type { Database } from "bun:sqlite";
import { getRow, setMetaValue, withInitializedDatabase } from "./engine/db";
import { computeCommitId, getRowEffectsByCommitId, getSchemaEffectsByCommitId, listCommits } from "./engine/log";
import type { VerifyResult } from "./types";

export function verifyDatabase(options: { full?: boolean } = {}): VerifyResult {
  return withInitializedDatabase(({ db }) => {
    const mode = options.full ? "full" : "quick";
    const issues: string[] = [];

    const commits = listCommits(db, false);
    for (const commit of commits) {
      const { commitId, parentCount, ...fields } = commit;
      const expected = computeCommitId({
        ...fields,
        rowEffects: getRowEffectsByCommitId(db, commitId),
        schemaEffects: getSchemaEffectsByCommitId(db, commitId),
      });
      if (expected !== commitId) {
        issues.push(`Commit hash mismatch: ${commitId}`);
      }
      if (parentCount !== commit.parentIds.length) {
        issues.push(`Parent count mismatch: ${commitId}`);
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
    setMetaValue(db, "last_verified_at", checkedAt);
    const ok = issues.length === 0;
    setMetaValue(db, "last_verified_ok", ok ? "1" : "0");

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
