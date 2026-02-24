import { existsSync } from "node:fs";
import {
  commitSeq,
  findCommit,
  readCommit,
  headCommit,
  readCommitsAfter,
  replayCommit,
} from "./commit";
import { clearAuthToken, parseRemotePlatform, readAuthToken, readRemoteConfig, writeAuthToken, writeRemoteConfig } from "./config";
import {
  LAST_PULLED_COMMIT_META_KEY,
  LAST_PUSHED_COMMIT_META_KEY,
  LAST_SYNC_ERROR_META_KEY,
  LAST_SYNC_STATE_META_KEY,
  getMetaValue,
  initDb,
  normalizeMetaString,
  openDb,
  resolveDbPath,
  runInDeferredTransaction,
  setMetaValue,
  type Database,
} from "./db";
import { CodedError } from "./error";
import { canonicalJson } from "./hash";
import {
  authTokenForPlatform,
  classifySyncBoundaryError,
  detectRemoteReadState,
  ensureRemoteInitialized,
  remoteHead,
  fetchCommitsAfter,
  projectionStatus,
  materializeToHead,
  normalizeToken,
  openRemoteClient,
  parseRemoteDbName,
  pushCommit,
  remoteCommitSeq,
  remoteHasCommit,
} from "./remote";

export type RemotePlatform = "turso" | "libsql";

const COMMIT_SIZE_WARNING_THRESHOLD_BYTES = 256 * 1024;

export function syncConfig() {
  const remote = readRemoteConfig();
  if (!remote) {
    return null;
  }
  return {
    platform: remote.platform,
    remoteUrl: remote.url,
    remoteDbName: parseRemoteDbName(remote.url),
  };
}

function writeSyncState(db: Database, state: "synced" | "pending" | "conflict" | "offline", error: string | null): void {
  setMetaValue(db, LAST_SYNC_STATE_META_KEY, state);
  setMetaValue(db, LAST_SYNC_ERROR_META_KEY, error ?? "");
}

function pendingCommitsFromHead(db: Database, lastPushedCommit: string | null): number {
  const head = headCommit(db);
  if (!head) {
    return 0;
  }
  if (!lastPushedCommit) {
    return head.seq;
  }
  const pushedSeq = commitSeq(db, lastPushedCommit);
  if (!pushedSeq) {
    return head.seq;
  }
  return Math.max(head.seq - pushedSeq, 0);
}

export async function push(db: Database) {
  const config = syncConfig();
  if (!config) {
    writeSyncState(db, "offline", "Remote is not configured");
    throw new CodedError("SYNC_NOT_CONFIGURED", "Remote is not configured");
  }

  const client = openRemoteClient(config);
  try {
    await ensureRemoteInitialized(client);
    await materializeToHead(client);
    const remoteHeadBefore = await remoteHead(client);

    if (remoteHeadBefore.commitId && !findCommit(db, remoteHeadBefore.commitId)) {
      const message = `Remote HEAD ${remoteHeadBefore.commitId} is unknown locally. Pull before push.`;
      writeSyncState(db, "conflict", message);
      throw new CodedError("SYNC_NON_FAST_FORWARD", message);
    }

    const fromSeq = remoteHeadBefore.commitId ? (commitSeq(db, remoteHeadBefore.commitId) ?? 0) : 0;
    const replays = readCommitsAfter(db, fromSeq);
    let expectedRemoteHead = remoteHeadBefore.commitId;
    let pushed = 0;
    for (const replay of replays) {
      await pushCommit(client, replay, expectedRemoteHead);
      expectedRemoteHead = replay.commit.commitId;
      pushed += 1;
    }

    const localHeadAfter = headCommit(db)?.commitId ?? null;
    const remoteHeadAfter = await remoteHead(client);
    setMetaValue(db, LAST_PUSHED_COMMIT_META_KEY, remoteHeadAfter.commitId ?? "");
    const pending = pendingCommitsFromHead(db, remoteHeadAfter.commitId);
    const state = pending > 0 ? "pending" : "synced";
    writeSyncState(db, state, null);
    return {
      action: "push" as const,
      state,
      pushed,
      pulled: 0,
      localHead: localHeadAfter,
      remoteHead: remoteHeadAfter.commitId,
      conflict: undefined,
      error: undefined,
    };
  } catch (error) {
    const mapped = classifySyncBoundaryError(error);
    if (CodedError.hasCode(mapped, "SYNC_NON_FAST_FORWARD") || CodedError.hasCode(mapped, "SYNC_DIVERGED")) {
      writeSyncState(db, "conflict", mapped.message);
    } else {
      writeSyncState(db, "pending", mapped.message);
    }
    throw mapped;
  } finally {
    client.close();
  }
}

export async function pull(db: Database) {
  const config = syncConfig();
  if (!config) {
    writeSyncState(db, "offline", "Remote is not configured");
    throw new CodedError("SYNC_NOT_CONFIGURED", "Remote is not configured");
  }

  const client = openRemoteClient(config);
  try {
    const localHead = headCommit(db)?.commitId ?? null;
    const remoteState = await detectRemoteReadState(client);
    if (remoteState === "empty") {
      setMetaValue(db, LAST_PULLED_COMMIT_META_KEY, "");
      setMetaValue(db, LAST_PUSHED_COMMIT_META_KEY, "");
      const pending = pendingCommitsFromHead(db, null);
      const state = pending > 0 ? "pending" : "synced";
      writeSyncState(db, state, null);
      return {
        action: "pull" as const,
        state,
        pushed: 0,
        pulled: 0,
        localHead,
        remoteHead: null,
        conflict: undefined,
        error: undefined,
      };
    }
    const pulledRemoteHead = await remoteHead(client);
    let fromSeq = 0;

    if (localHead) {
      const remoteHasLocalHead = await remoteHasCommit(client, localHead);
      if (remoteHasLocalHead) {
        fromSeq = (await remoteCommitSeq(client, localHead)) ?? 0;
      } else if (pulledRemoteHead.commitId && findCommit(db, pulledRemoteHead.commitId)) {
        const pending = pendingCommitsFromHead(db, normalizeMetaString(getMetaValue(db, LAST_PUSHED_COMMIT_META_KEY)));
        const state = pending > 0 ? "pending" : "synced";
        writeSyncState(db, state, null);
        return {
          action: "pull" as const,
          state,
          pushed: 0,
          pulled: 0,
          localHead,
          remoteHead: pulledRemoteHead.commitId,
          conflict: undefined,
          error: undefined,
        };
      } else if (pulledRemoteHead.commitId !== null) {
        const message = `Local HEAD ${localHead} is not present on remote, and remote HEAD ${pulledRemoteHead.commitId} is not present locally.`;
        writeSyncState(db, "conflict", message);
        throw new CodedError("SYNC_DIVERGED", message);
      }
    }

    const replayInputs = await fetchCommitsAfter(client, fromSeq, pulledRemoteHead);
    let pulled = 0;
    for (const replay of replayInputs) {
      if (findCommit(db, replay.commit.commitId)) {
        continue;
      }
      runInDeferredTransaction(db, () => {
        replayCommit(db, replay, { errorCode: "SYNC_DIVERGED" });
      });
      pulled += 1;
    }

    const localHeadAfter = headCommit(db)?.commitId ?? null;
    const remoteHeadAfter = await remoteHead(client);
    setMetaValue(db, LAST_PULLED_COMMIT_META_KEY, remoteHeadAfter.commitId ?? "");
    if (localHeadAfter === remoteHeadAfter.commitId) {
      setMetaValue(db, LAST_PUSHED_COMMIT_META_KEY, remoteHeadAfter.commitId ?? "");
    }
    const pending = pendingCommitsFromHead(db, normalizeMetaString(getMetaValue(db, LAST_PUSHED_COMMIT_META_KEY)));
    const state = pending > 0 ? "pending" : "synced";
    writeSyncState(db, state, null);
    return {
      action: "pull" as const,
      state,
      pushed: 0,
      pulled,
      localHead: localHeadAfter,
      remoteHead: remoteHeadAfter.commitId,
      conflict: undefined,
      error: undefined,
    };
  } catch (error) {
    const mapped = classifySyncBoundaryError(error);
    if (CodedError.hasCode(mapped, "SYNC_DIVERGED")) {
      writeSyncState(db, "conflict", mapped.message);
    } else {
      writeSyncState(db, "pending", mapped.message);
    }
    throw mapped;
  } finally {
    client.close();
  }
}

function syncConfigFromInputs(options: {
  platform: RemotePlatform;
  url: string;
}) {
  const platform = parseRemotePlatform(options.platform);
  const trimmedUrl = options.url.trim();
  if (trimmedUrl.length === 0) {
    throw new CodedError("CONFIG", "Remote URL must not be empty");
  }
  return {
    platform,
    remoteUrl: trimmedUrl,
    remoteDbName: parseRemoteDbName(trimmedUrl),
  };
}

export async function connect(
  db: Database,
  options: {
    platform: RemotePlatform;
    url: string;
    authToken?: string | null | undefined;
  },
) {
  const config = syncConfigFromInputs(options);
  const previousConfig = syncConfig();
  const previousIdentity = previousConfig ? `${previousConfig.platform}\u0000${previousConfig.remoteUrl}\u0000${previousConfig.remoteDbName ?? ""}` : null;
  const nextIdentity = `${config.platform}\u0000${config.remoteUrl}\u0000${config.remoteDbName ?? ""}`;
  const remoteChanged = previousIdentity !== nextIdentity;
  const client = openRemoteClient(config, options.authToken);
  try {
    await detectRemoteReadState(client);
    writeRemoteConfig({
      platform: config.platform,
      url: config.remoteUrl,
    });
    const token = normalizeToken(options.authToken);
    if (token) {
      writeAuthToken(config.platform, token);
    } else if (options.authToken === null) {
      clearAuthToken(config.platform);
    }
    if (remoteChanged) {
      setMetaValue(db, LAST_PUSHED_COMMIT_META_KEY, "");
      setMetaValue(db, LAST_PULLED_COMMIT_META_KEY, "");
    }
    writeSyncState(db, "pending", null);
    return config;
  } catch (error) {
    throw classifySyncBoundaryError(error);
  } finally {
    client.close();
  }
}

export async function sync(
  db: Database,
  options: { action?: "push" | "pull" | "sync" | "auto_sync" | "clone" } = {},
) {
  const action = options.action ?? "sync";
  const pullResult = await pull(db);
  const pushResult = await push(db);
  return {
    action,
    state: pushResult.state,
    pushed: pushResult.pushed,
    pulled: pullResult.pulled,
    localHead: pushResult.localHead,
    remoteHead: pushResult.remoteHead,
    conflict: undefined,
    error: undefined,
  };
}

export async function autoSync(db: Database) {
  const config = syncConfig();
  if (!config) {
    return null;
  }
  try {
    return await sync(db, { action: "auto_sync" });
  } catch (error) {
    const mapped = classifySyncBoundaryError(error);
    const localHead = headCommit(db)?.commitId ?? null;
    const isConflict = CodedError.hasCode(mapped, "SYNC_NON_FAST_FORWARD") || CodedError.hasCode(mapped, "SYNC_DIVERGED");
    const state: "conflict" | "pending" = isConflict ? "conflict" : "pending";
    writeSyncState(db, state, mapped.message);
    return {
      action: "auto_sync",
      state,
      pushed: 0,
      pulled: 0,
      localHead,
      remoteHead: null,
      conflict: isConflict
        ? {
            kind: CodedError.hasCode(mapped, "SYNC_DIVERGED") ? "diverged" : "non_fast_forward",
            message: mapped.message,
            localHead,
            remoteHead: null,
          }
        : undefined,
      error: mapped.message,
    };
  }
}

export async function remoteStatus(db: Database) {
  const config = syncConfig();
  const localHead = headCommit(db)?.commitId ?? null;
  const localPending = pendingCommitsFromHead(db, normalizeMetaString(getMetaValue(db, LAST_PUSHED_COMMIT_META_KEY)));
  if (!config) {
    return {
      config: null,
      localHead,
      remoteHead: null,
      pendingCommits: localPending,
      hasAuthToken: readAuthToken("turso") !== undefined || readAuthToken("libsql") !== undefined,
      projectionHead: null,
      projectionLagCommits: null,
      projectionError: null,
    };
  }
  const client = openRemoteClient(config);
  try {
    const remoteState = await detectRemoteReadState(client);
    if (remoteState === "empty") {
      return {
        config,
        localHead,
        remoteHead: null,
        pendingCommits: pendingCommitsFromHead(db, null),
        hasAuthToken: authTokenForPlatform(config) !== undefined,
        projectionHead: null,
        projectionLagCommits: 0,
        projectionError: null,
      };
    }
    const statusRemoteHead = await remoteHead(client);
    const projection = await projectionStatus(client, statusRemoteHead);
    return {
      config,
      localHead,
      remoteHead: statusRemoteHead,
      pendingCommits: localPending,
      hasAuthToken: authTokenForPlatform(config) !== undefined,
      projectionHead: projection.projectionHead,
      projectionLagCommits: projection.projectionLagCommits,
      projectionError: projection.projectionError,
    };
  } catch (error) {
    throw classifySyncBoundaryError(error);
  } finally {
    client.close();
  }
}

export async function clone(options: {
  platform: RemotePlatform;
  url: string;
  forceNew?: boolean | undefined;
  authToken?: string | null | undefined;
  dbPath?: string | undefined;
}) {
  const targetDbPath = resolveDbPath(options.dbPath);
  const forceNew = options.forceNew ?? false;
  if (!forceNew && existsSync(targetDbPath)) {
    throw new CodedError("CONFIG", `Clone target already exists: ${targetDbPath}`);
  }
  const initialized = await initDb({ dbPath: targetDbPath, forceNew });
  const db = openDb(initialized.path);
  try {
    await connect(db, {
      platform: options.platform,
      url: options.url,
      authToken: options.authToken,
    });
    const pulled = await pull(db);
    return {
      dbPath: initialized.path,
      sync: {
        ...pulled,
        action: "clone" as const,
      },
    };
  } finally {
    db.$client.close(false);
  }
}

export function syncStatus(db: Database) {
  const config = syncConfig();
  const lastPushedCommit = normalizeMetaString(getMetaValue(db, LAST_PUSHED_COMMIT_META_KEY));
  const lastPulledCommit = normalizeMetaString(getMetaValue(db, LAST_PULLED_COMMIT_META_KEY));
  const storedState = normalizeMetaString(getMetaValue(db, LAST_SYNC_STATE_META_KEY));
  const lastError = normalizeMetaString(getMetaValue(db, LAST_SYNC_ERROR_META_KEY));
  const pendingCommits = pendingCommitsFromHead(db, lastPushedCommit);
  let state: "synced" | "pending" | "conflict" | "offline";
  if (!config) {
    state = "offline";
  } else if (storedState === "conflict") {
    state = "conflict";
  } else if (storedState === "offline") {
    state = "offline";
  } else if (pendingCommits > 0 || storedState === "pending") {
    state = "pending";
  } else {
    state = "synced";
  }
  return {
    configured: config !== null,
    remotePlatform: config?.platform ?? null,
    remoteUrl: config?.remoteUrl ?? null,
    remoteDbName: config?.remoteDbName ?? null,
    state,
    lastPushedCommit,
    lastPulledCommit,
    pendingCommits,
    lastError,
    projectionHead: null,
    projectionLagCommits: null,
    projectionError: null,
  };
}

export function sizeWarning(db: Database, commitId: string): string | null {
  const replay = readCommit(db, commitId);
  const payloadSize = canonicalJson({
    operations: replay.operations,
    rowEffects: replay.rowEffects,
    schemaEffects: replay.schemaEffects,
  }).length;
  if (payloadSize < COMMIT_SIZE_WARNING_THRESHOLD_BYTES) {
    return null;
  }
  return `Commit payload is large (${payloadSize} bytes). Frequent update/delete operations can grow history quickly.`;
}
