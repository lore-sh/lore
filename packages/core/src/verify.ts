import type { Database } from "bun:sqlite";
import { sha256Hex } from "./checksum";
import { assertInitialized, closeDatabase, ENGINE_META_TABLE, openDatabase } from "./db";
import { listCommits } from "./log";
import type { ServiceOptions, VerifyResult } from "./types";

export function putMeta(db: Database, key: string, value: string): void {
  db.query(
    `
    INSERT INTO ${ENGINE_META_TABLE}(key, value) VALUES(?, ?)
    ON CONFLICT(key) DO UPDATE SET value=excluded.value
    `,
  ).run(key, value);
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
  } finally {
    closeDatabase(db);
  }
}
