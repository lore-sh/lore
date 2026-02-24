export {
  type ErrorCategory,
  type ErrorCode,
  CodedError,
  httpStatusFromError,
  toHttpProblem,
  type HttpProblem,
} from "./error";

export {
  JsonPrimitive,
  JsonValue,
  JsonObject,
  SqlStorageClass,
  EncodedCell,
  EncodedRow,
  TableSecondaryObject,
  CommitKind,
  MetaTable,
  CommitTable,
  CommitParentTable,
  RefTable,
  ReflogTable,
  OpTable,
  RowEffectTable,
  SchemaEffectTable,
  SnapshotTable,
} from "./schema";

export {
  type Database,
  type SkillPlatform,
  DEFAULT_DB_DIR,
  DEFAULT_DB_NAME,
  DEFAULT_SNAPSHOT_INTERVAL,
  DEFAULT_SNAPSHOT_RETAIN,
  DEFAULT_SYNC_PROTOCOL_VERSION,
  MAIN_REF_NAME,
  META_TABLE,
  COMMIT_TABLE,
  COMMIT_PARENT_TABLE,
  REF_TABLE,
  REFLOG_TABLE,
  OP_TABLE,
  ROW_EFFECT_TABLE,
  SCHEMA_EFFECT_TABLE,
  SNAPSHOT_TABLE,
  LAST_PUSHED_COMMIT_META_KEY,
  LAST_PULLED_COMMIT_META_KEY,
  LAST_SYNC_STATE_META_KEY,
  LAST_SYNC_ERROR_META_KEY,
  SYNC_PROTOCOL_VERSION_META_KEY,
  LAST_MATERIALIZED_COMMIT_META_KEY,
  LAST_MATERIALIZED_AT_META_KEY,
  LAST_MATERIALIZED_ERROR_META_KEY,
  LAST_VERIFIED_AT_META_KEY,
  LAST_VERIFIED_OK_META_KEY,
  isEnoent,
  resolveHomeDir,
  deleteIfExists,
  deleteWithSidecars,
  deleteWalAndShm,
  resolveDbPath,
  openDb,
  initDb,
  tableExists,
  isInitialized,
  assertInitialized,
  runInDeferredTransaction,
  runInSavepoint,
  listUserTables,
  getMetaValue,
  normalizeMetaString,
  setMetaValue,
  query,
} from "./db";

export {
  tableInfo,
  primaryKeys,
  assertPrimaryKey,
  tableDDL,
  describeSchema,
  schemaHash,
  hashSchema,
  stateHash,
  whereClause,
  getRowsByWhere,
  getAllRows,
  pkFromRow,
  getRowByPk,
  countRows,
  isVisible,
  normalizePageSize,
  normalizePage,
  normalizeRow,
  serializeScalar,
} from "./inspect";

export {
  Commit,
  headCommit,
  findCommit,
  listCommits,
  commitOperations,
  commitRowEffects,
  commitSchemaEffects,
  commitSeq,
  readCommit,
  readCommitsAfter,
  replayCommit,
  createCommit,
  computeCommitId,
} from "./commit";

export {
  ColumnDef,
  Operation,
  Plan,
  executeOperation,
  executeReadSql,
  parsePlan,
} from "./operation";

export {
  RowEffect,
  SchemaEffect,
  TableSnapshot,
  captureState,
  diffState,
  applyEffects,
  applyRowEffects,
  readRow,
  rowHash,
  assertForeignKeys,
  isSystemTable,
  dependencyOrder,
  pkWhere,
} from "./effect";

export {
  apply,
  check,
} from "./apply";

export {
  revert,
  detectSchemaConflicts,
  detectSchemaRowConflicts,
  detectRowConflict,
  getLaterEffects,
} from "./revert";

export {
  resolveTable,
  describeDb,
  tableOverview,
  queryTable,
  uniqueColumnNames,
} from "./table";

export {
  history,
  commitSize,
  historySize,
  verify,
} from "./history";

export {
  maybeCreateSnapshot,
  promotePrepared,
  recover,
} from "./snapshot";

export {
  type RemotePlatform,
  connect,
  push,
  pull,
  sync,
  autoSync,
  remoteStatus,
  clone,
  syncStatus,
  syncConfig,
  sizeWarning,
} from "./sync";

export {
  status,
} from "./status";

export {
  parseRemoteDbName,
  classifySyncBoundaryError,
  normalizeToken,
  authTokenForPlatform,
  openRemoteClient,
  detectRemoteReadState,
  ensureRemoteInitialized,
  remoteHead,
  remoteHasCommit,
  remoteCommitSeq,
  materializeToHead,
  projectionStatus,
  pushCommit,
  fetchCommitsAfter,
} from "./remote";
