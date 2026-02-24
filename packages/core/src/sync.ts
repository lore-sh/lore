import { existsSync } from "node:fs";
import {
  LAST_PULLED_COMMIT_META_KEY,
  LAST_PUSHED_COMMIT_META_KEY,
  LAST_SYNC_ERROR_META_KEY,
  LAST_SYNC_STATE_META_KEY,
  getMetaValue,
  normalizeMetaString,
  openDb,
  resolveDbPath,
  runInDeferredTransaction,
  setMetaValue,
  type Database,
  initDb,
} from "./db";
import { clearAuthToken, parseRemotePlatform, readAuthToken, readRemoteConfig, writeAuthToken, writeRemoteConfig } from "./config";
import { CodedError } from "./error";
import {
  findCommitSeq,
  getCommitById,
  getCommitReplayInput,
  getHeadCommit,
  getHeadCommitId,
  loadCommitReplayInputs,
  replayCommitExactly,
} from "./commit";
import {
  authTokenForPlatform,
  classifySyncBoundaryError,
  detectRemoteReadState,
  ensureRemoteInitialized,
  fetchRemoteHead,
  fetchRemoteProjectionStatus,
  fetchRemoteInputsAfterSeq,
  materializeRemoteToHead,
  normalizeToken,
  openRemoteClient,
  parseRemoteDbName,
  pushCommit,
  remoteCommitSeq,
  remoteHasCommit,
} from "./remote";
import { canonicalJson } from "./sql";

export type RemotePlatform = "turso" | "libsql";

export interface SyncConfig {
  platform: RemotePlatform;
  remoteUrl: string;
  remoteDbName: string | null;
}

export interface RemoteHead {
  commitId: string | null;
  seq: number;
}

export interface SyncConflict {
  kind: "non_fast_forward" | "diverged";
  message: string;
  localHead: string | null;
  remoteHead: string | null;
}

export type SyncState = "synced" | "pending" | "conflict" | "offline";

export interface SyncResult {
  action: "push" | "pull" | "sync" | "auto_sync" | "clone";
  state: SyncState;
  pushed: number;
  pulled: number;
  localHead: string | null;
  remoteHead: string | null;
  conflict?: SyncConflict | undefined;
  error?: string | undefined;
}

export interface SyncStatus {
  configured: boolean;
  remotePlatform: RemotePlatform | null;
  remoteUrl: string | null;
  remoteDbName: string | null;
  state: SyncState;
  lastPushedCommit: string | null;
  lastPulledCommit: string | null;
  pendingCommits: number;
  lastError: string | null;
  projectionHead: string | null;
  projectionLagCommits: number | null;
  projectionError: string | null;
}

const COMMIT_SIZE_WARNING_THRESHOLD_BYTES = 256 * 1024;

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
  if (storedState === "offline") {
    return "offline";
  }
  if (pendingCommits > 0 || storedState === "pending") {
    return "pending";
  }
  return "synced";
}

async function runPush(db: Database, action: SyncResult["action"]): Promise<SyncResult> {
  const config = readSyncConfig();
  if (!config) {
    writeSyncState(db, "offline", "Remote is not configured");
    throw new CodedError("SYNC_NOT_CONFIGURED", "Remote is not configured");
  }

  const client = openRemoteClient(config);
  try {
    await ensureRemoteInitialized(client);
    await materializeRemoteToHead(client);
    const remoteHeadBefore = await fetchRemoteHead(client);

    if (remoteHeadBefore.commitId && !getCommitById(db, remoteHeadBefore.commitId)) {
      const message = `Remote HEAD ${remoteHeadBefore.commitId} is unknown locally. Pull before push.`;
      writeSyncState(db, "conflict", message);
      throw new CodedError("SYNC_NON_FAST_FORWARD", message);
    }

    const fromSeq = remoteHeadBefore.commitId ? (findCommitSeq(db, remoteHeadBefore.commitId) ?? 0) : 0;
    const replays = loadCommitReplayInputs(db, fromSeq);
    let expectedRemoteHead = remoteHeadBefore.commitId;
    let pushed = 0;
    for (const replay of replays) {
      await pushCommit(client, replay, expectedRemoteHead);
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

async function runPull(db: Database, action: SyncResult["action"]): Promise<SyncResult> {
  const config = readSyncConfig();
  if (!config) {
    writeSyncState(db, "offline", "Remote is not configured");
    throw new CodedError("SYNC_NOT_CONFIGURED", "Remote is not configured");
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
        throw new CodedError("SYNC_DIVERGED", message);
      }
    }

    const replayInputs = await fetchRemoteInputsAfterSeq(client, fromSeq, remoteHead);
    let pulled = 0;
    for (const replay of replayInputs) {
      if (getCommitById(db, replay.commitId)) {
        continue;
      }
      runInDeferredTransaction(db, () => {
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
  platform: SyncConfig["platform"];
  url: string;
}): SyncConfig {
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
    platform: SyncConfig["platform"];
    url: string;
    authToken?: string | null | undefined;
  },
): Promise<SyncConfig> {
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
}

export function getSyncConfig(): SyncConfig | null {
  return readSyncConfig();
}

export function push(db: Database): Promise<SyncResult> {
  return runPush(db, "push");
}

export function pull(db: Database): Promise<SyncResult> {
  return runPull(db, "pull");
}

export async function sync(db: Database, options: { action?: SyncResult["action"] } = {}): Promise<SyncResult> {
  const action = options.action ?? "sync";
  const pullResult = await runPull(db, action);
  const pushResult = await runPush(db, action);
  return buildSyncResult(
    action,
    pushResult.state,
    pushResult.pushed,
    pullResult.pulled,
    pushResult.localHead,
    pushResult.remoteHead,
  );
}

export async function autoSync(db: Database): Promise<SyncResult | null> {
  const config = readSyncConfig();
  if (!config) {
    return null;
  }
  try {
    return await sync(db, { action: "auto_sync" });
  } catch (error) {
    const mapped = classifySyncBoundaryError(error);
    const localHead = getHeadCommitId(db);
    const isConflict = CodedError.hasCode(mapped, "SYNC_NON_FAST_FORWARD") || CodedError.hasCode(mapped, "SYNC_DIVERGED");
    const state: SyncState = isConflict ? "conflict" : "pending";
    writeSyncState(db, state, mapped.message);
    return buildSyncResult("auto_sync", state, 0, 0, localHead, null, {
      conflict: isConflict
        ? {
            kind: CodedError.hasCode(mapped, "SYNC_DIVERGED") ? "diverged" : "non_fast_forward",
            message: mapped.message,
            localHead,
            remoteHead: null,
          }
        : undefined,
      error: mapped.message,
    });
  }
}

export async function remoteStatus(db: Database): Promise<{
  config: SyncConfig | null;
  localHead: string | null;
  remoteHead: RemoteHead | null;
  pendingCommits: number;
  hasAuthToken: boolean;
  projectionHead: string | null;
  projectionLagCommits: number | null;
  projectionError: string | null;
}> {
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
    const remoteHead = await fetchRemoteHead(client);
    const projection = await fetchRemoteProjectionStatus(client, remoteHead);
    return {
      config,
      localHead,
      remoteHead,
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
  platform: SyncConfig["platform"];
  url: string;
  forceNew?: boolean | undefined;
  authToken?: string | null | undefined;
  dbPath?: string | undefined;
}): Promise<{ dbPath: string; sync: SyncResult }> {
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
    const sync = await runPull(db, "clone");
    return { dbPath: initialized.path, sync };
  } finally {
    db.$client.close(false);
  }
}

export function syncStatus(db: Database): SyncStatus {
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
    projectionHead: null,
    projectionLagCommits: null,
    projectionError: null,
  };
}

export function commitSizeWarning(db: Database, commitId: string): string | null {
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
}
