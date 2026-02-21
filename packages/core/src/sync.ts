import type { Database } from "bun:sqlite";
import { existsSync } from "node:fs";
import {
  LAST_PULLED_COMMIT_META_KEY,
  LAST_PUSHED_COMMIT_META_KEY,
  LAST_SYNC_ERROR_META_KEY,
  LAST_SYNC_STATE_META_KEY,
  getMetaValue,
  resolveDbPath,
  runInTransactionWithDeferredForeignKeys,
  setMetaValue,
  withInitializedDatabase,
  withInitializedDatabaseAsync,
} from "./engine/db";
import { clearAuthToken, parseRemotePlatform, readAuthToken, readRemoteConfig, writeAuthToken, writeRemoteConfig } from "./config";
import { getClientPath } from "./engine/client";
import { TossError } from "./errors";
import { getHeadCommit, getHeadCommitId, getCommitById } from "./engine/log";
import { initDatabase } from "./init";
import { findCommitSeq, getCommitReplayInput, loadCommitReplayInputs, replayCommitExactly } from "./engine/replay";
import type {
  RemoteHead,
  SyncConfig,
  SyncConflict,
  SyncResult,
  SyncState,
  TossSyncStatus,
} from "./types";
import {
  authTokenForPlatform,
  classifySyncBoundaryError,
  detectRemoteReadState,
  ensureRemoteInitialized,
  fetchRemoteHead,
  fetchRemoteReplayInputsAfterSeq,
  normalizeToken,
  openRemoteClient,
  parseRemoteDbName,
  pushReplayCommitWithCas,
  remoteCommitSeq,
  remoteHasCommit,
} from "./engine/remote";
import { canonicalJson } from "./engine/checksum";

const COMMIT_SIZE_WARNING_THRESHOLD_BYTES = 256 * 1024;

function normalizeMetaString(value: string | null): string | null {
  if (value === null) {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length === 0 ? null : trimmed;
}

function readSyncConfig(): SyncConfig | null {
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

function writeSyncState(db: Database, state: SyncState, error: string | null): void {
  setMetaValue(db, LAST_SYNC_STATE_META_KEY, state);
  setMetaValue(db, LAST_SYNC_ERROR_META_KEY, error ?? "");
}

function writeLastPushedCommit(db: Database, commitId: string | null): void {
  setMetaValue(db, LAST_PUSHED_COMMIT_META_KEY, commitId ?? "");
}

function writeLastPulledCommit(db: Database, commitId: string | null): void {
  setMetaValue(db, LAST_PULLED_COMMIT_META_KEY, commitId ?? "");
}

function pendingCommitsFromHead(db: Database, lastPushedCommit: string | null): number {
  const head = getHeadCommit(db);
  if (!head) {
    return 0;
  }
  if (!lastPushedCommit) {
    return head.seq;
  }
  const pushedSeq = findCommitSeq(db, lastPushedCommit);
  if (!pushedSeq) {
    return head.seq;
  }
  return Math.max(head.seq - pushedSeq, 0);
}

function buildSyncResult(
  action: SyncResult["action"],
  state: SyncState,
  pushed: number,
  pulled: number,
  localHead: string | null,
  remoteHead: string | null,
  options: { conflict?: SyncConflict | undefined; error?: string | undefined } = {},
): SyncResult {
  return {
    action,
    state,
    pushed,
    pulled,
    localHead,
    remoteHead,
    conflict: options.conflict,
    error: options.error,
  };
}

function syncStateFromPending(pending: number): SyncState {
  return pending > 0 ? "pending" : "synced";
}

function statusStateForConfiguredDb(storedState: string | null, pendingCommits: number): SyncState {
  if (storedState === "conflict") {
    return "conflict";
  }
  if (pendingCommits > 0 || storedState === "pending") {
    return "pending";
  }
  return "synced";
}

async function runPush(action: SyncResult["action"]): Promise<SyncResult> {
  return await withInitializedDatabaseAsync(async ({ db }) => {
    const config = readSyncConfig();
    if (!config) {
      writeSyncState(db, "offline", "Remote is not configured");
      throw new TossError("CONFIG_ERROR", "Remote is not configured. Run `toss remote connect`.");
    }

    const client = openRemoteClient(config);
    try {
      await ensureRemoteInitialized(client);
      const localHead = getHeadCommitId(db);
      const remoteHeadBefore = await fetchRemoteHead(client);

      if (remoteHeadBefore.commitId && !getCommitById(db, remoteHeadBefore.commitId)) {
        const message = `Remote HEAD ${remoteHeadBefore.commitId} is unknown locally. Pull before push.`;
        writeSyncState(db, "conflict", message);
        throw new TossError("SYNC_NON_FAST_FORWARD", message);
      }

      const fromSeq = remoteHeadBefore.commitId ? (findCommitSeq(db, remoteHeadBefore.commitId) ?? 0) : 0;
      const replays = loadCommitReplayInputs(db, fromSeq);
      let expectedRemoteHead = remoteHeadBefore.commitId;
      let pushed = 0;
      for (const replay of replays) {
        await pushReplayCommitWithCas(client, replay, expectedRemoteHead);
        expectedRemoteHead = replay.commitId;
        pushed += 1;
      }

      const localHeadAfter = getHeadCommitId(db);
      const remoteHeadAfter = await fetchRemoteHead(client);
      writeLastPushedCommit(db, remoteHeadAfter.commitId);
      const pending = pendingCommitsFromHead(db, remoteHeadAfter.commitId);
      const state = syncStateFromPending(pending);
      writeSyncState(db, state, null);
      return buildSyncResult(action, state, pushed, 0, localHeadAfter, remoteHeadAfter.commitId);
    } catch (error) {
      const mapped = classifySyncBoundaryError(error);
      if (mapped.code === "SYNC_NON_FAST_FORWARD" || mapped.code === "SYNC_DIVERGED") {
        writeSyncState(db, "conflict", mapped.message);
      } else {
        writeSyncState(db, "pending", mapped.message);
      }
      throw mapped;
    } finally {
      client.close();
    }
  });
}

async function runPull(action: SyncResult["action"]): Promise<SyncResult> {
  return await withInitializedDatabaseAsync(async ({ db }) => {
    const config = readSyncConfig();
    if (!config) {
      writeSyncState(db, "offline", "Remote is not configured");
      throw new TossError("CONFIG_ERROR", "Remote is not configured. Run `toss remote connect`.");
    }

    const client = openRemoteClient(config);
    try {
      const localHead = getHeadCommitId(db);
      const remoteState = await detectRemoteReadState(client);
      if (remoteState === "empty") {
        writeLastPulledCommit(db, null);
        writeLastPushedCommit(db, null);
        const pending = pendingCommitsFromHead(db, null);
        const state = syncStateFromPending(pending);
        writeSyncState(db, state, null);
        return buildSyncResult(action, state, 0, 0, localHead, null);
      }
      const remoteHead = await fetchRemoteHead(client);
      let fromSeq = 0;

      if (localHead) {
        const remoteHasLocalHead = await remoteHasCommit(client, localHead);
        if (remoteHasLocalHead) {
          fromSeq = (await remoteCommitSeq(client, localHead)) ?? 0;
        } else if (remoteHead.commitId && getCommitById(db, remoteHead.commitId)) {
          const pending = pendingCommitsFromHead(db, normalizeMetaString(getMetaValue(db, LAST_PUSHED_COMMIT_META_KEY)));
          const state = syncStateFromPending(pending);
          writeSyncState(db, state, null);
          return buildSyncResult(action, state, 0, 0, localHead, remoteHead.commitId);
        } else if (remoteHead.commitId !== null) {
          const message = `Local HEAD ${localHead} is not present on remote, and remote HEAD ${remoteHead.commitId} is not present locally.`;
          writeSyncState(db, "conflict", message);
          throw new TossError("SYNC_DIVERGED", message);
        }
      }

      const replayInputs = await fetchRemoteReplayInputsAfterSeq(client, fromSeq);
      let pulled = 0;
      for (const replay of replayInputs) {
        if (getCommitById(db, replay.commitId)) {
          continue;
        }
        runInTransactionWithDeferredForeignKeys(db, () => {
          replayCommitExactly(db, replay, { errorCode: "SYNC_DIVERGED" });
        });
        pulled += 1;
      }

      const localHeadAfter = getHeadCommitId(db);
      const remoteHeadAfter = await fetchRemoteHead(client);
      writeLastPulledCommit(db, remoteHeadAfter.commitId);
      if (localHeadAfter === remoteHeadAfter.commitId) {
        writeLastPushedCommit(db, remoteHeadAfter.commitId);
      }
      const pending = pendingCommitsFromHead(db, normalizeMetaString(getMetaValue(db, LAST_PUSHED_COMMIT_META_KEY)));
      const state = syncStateFromPending(pending);
      writeSyncState(db, state, null);
      return buildSyncResult(action, state, 0, pulled, localHeadAfter, remoteHeadAfter.commitId);
    } catch (error) {
      const mapped = classifySyncBoundaryError(error);
      if (mapped.code === "SYNC_DIVERGED") {
        writeSyncState(db, "conflict", mapped.message);
      } else {
        writeSyncState(db, "pending", mapped.message);
      }
      throw mapped;
    } finally {
      client.close();
    }
  });
}

function syncConfigFromInputs(options: {
  platform: SyncConfig["platform"];
  url: string;
}): SyncConfig {
  const platform = parseRemotePlatform(options.platform);
  const trimmedUrl = options.url.trim();
  if (trimmedUrl.length === 0) {
    throw new TossError("CONFIG_ERROR", "Remote URL must not be empty");
  }
  return {
    platform,
    remoteUrl: trimmedUrl,
    remoteDbName: parseRemoteDbName(trimmedUrl),
  };
}

export async function connectRemote(options: {
  platform: SyncConfig["platform"];
  url: string;
  authToken?: string | null | undefined;
}): Promise<SyncConfig> {
  return await withInitializedDatabaseAsync(async ({ db }) => {
    const config = syncConfigFromInputs(options);
    const previousConfig = readSyncConfig();
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
        writeLastPushedCommit(db, null);
        writeLastPulledCommit(db, null);
      }
      writeSyncState(db, "pending", null);
      return config;
    } catch (error) {
      throw classifySyncBoundaryError(error);
    } finally {
      client.close();
    }
  });
}

export function getSyncConfig(): SyncConfig | null {
  return withInitializedDatabase(() => readSyncConfig());
}

export async function pushToRemote(): Promise<SyncResult> {
  return await runPush("push");
}

export async function pullFromRemote(): Promise<SyncResult> {
  return await runPull("pull");
}

export async function syncWithRemote(options: { action?: SyncResult["action"] } = {}): Promise<SyncResult> {
  const action = options.action ?? "sync";
  const pullResult = await runPull(action);
  const pushResult = await runPush(action);
  return buildSyncResult(
    action,
    pushResult.state,
    pushResult.pushed,
    pullResult.pulled,
    pushResult.localHead,
    pushResult.remoteHead,
  );
}

export async function autoSyncAfterApply(): Promise<SyncResult | null> {
  return await withInitializedDatabaseAsync(async ({ db }) => {
    const config = readSyncConfig();
    if (!config) {
      return null;
    }
    try {
      return await syncWithRemote({ action: "auto_sync" });
    } catch (error) {
      const mapped = classifySyncBoundaryError(error);
      const localHead = getHeadCommitId(db);
      const isConflict = mapped.code === "SYNC_NON_FAST_FORWARD" || mapped.code === "SYNC_DIVERGED";
      const state: SyncState = isConflict ? "conflict" : "pending";
      writeSyncState(db, state, mapped.message);
      return buildSyncResult("auto_sync", state, 0, 0, localHead, null, {
        conflict: isConflict
          ? {
              kind: mapped.code === "SYNC_DIVERGED" ? "diverged" : "non_fast_forward",
              message: mapped.message,
              localHead,
              remoteHead: null,
            }
          : undefined,
        error: mapped.message,
      });
    }
  });
}

export async function getRemoteStatus(): Promise<{
  config: SyncConfig | null;
  localHead: string | null;
  remoteHead: RemoteHead | null;
  pendingCommits: number;
  hasAuthToken: boolean;
}> {
  return await withInitializedDatabaseAsync(async ({ db }) => {
    const config = readSyncConfig();
    const localHead = getHeadCommitId(db);
    const localPending = pendingCommitsFromHead(db, normalizeMetaString(getMetaValue(db, LAST_PUSHED_COMMIT_META_KEY)));
    if (!config) {
      return {
        config: null,
        localHead,
        remoteHead: null,
        pendingCommits: localPending,
        hasAuthToken: readAuthToken("turso") !== undefined,
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
        };
      }
      return {
        config,
        localHead,
        remoteHead: await fetchRemoteHead(client),
        pendingCommits: localPending,
        hasAuthToken: authTokenForPlatform(config) !== undefined,
      };
    } catch (error) {
      throw classifySyncBoundaryError(error);
    } finally {
      client.close();
    }
  });
}

export async function cloneFromRemote(options: {
  platform: SyncConfig["platform"];
  url: string;
  forceNew?: boolean | undefined;
  authToken?: string | null | undefined;
}): Promise<{ dbPath: string; sync: SyncResult }> {
  const targetDbPath = getClientPath() ?? resolveDbPath();
  const forceNew = options.forceNew ?? false;
  if (!forceNew && existsSync(targetDbPath)) {
    throw new TossError("CONFIG_ERROR", `Clone target already exists: ${targetDbPath}. Use --force-new to replace it.`);
  }
  const initialized = await initDatabase({ dbPath: targetDbPath, forceNew });
  await connectRemote({
    platform: options.platform,
    url: options.url,
    authToken: options.authToken,
  });
  const sync = await runPull("clone");
  return { dbPath: initialized.dbPath, sync };
}

export function buildSyncStatus(db: Database): TossSyncStatus {
  const config = readSyncConfig();
  const lastPushedCommit = normalizeMetaString(getMetaValue(db, LAST_PUSHED_COMMIT_META_KEY));
  const lastPulledCommit = normalizeMetaString(getMetaValue(db, LAST_PULLED_COMMIT_META_KEY));
  const storedState = normalizeMetaString(getMetaValue(db, LAST_SYNC_STATE_META_KEY));
  const lastError = normalizeMetaString(getMetaValue(db, LAST_SYNC_ERROR_META_KEY));
  const pendingCommits = pendingCommitsFromHead(db, lastPushedCommit);
  const state = config ? statusStateForConfiguredDb(storedState, pendingCommits) : "offline";
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
  };
}

export function commitSizeWarning(commitId: string): string | null {
  return withInitializedDatabase(({ db }) => {
    const replay = getCommitReplayInput(db, commitId);
    const payloadSize = canonicalJson({
      operations: replay.operations,
      rowEffects: replay.rowEffects,
      schemaEffects: replay.schemaEffects,
    }).length;
    if (payloadSize < COMMIT_SIZE_WARNING_THRESHOLD_BYTES) {
      return null;
    }
    return `Commit payload is large (${payloadSize} bytes). Frequent update/delete operations can grow history quickly.`;
  });
}
