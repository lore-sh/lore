export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonObject | JsonValue[];
export interface JsonObject {
  [key: string]: JsonValue;
}

export type SqlStorageClass = "null" | "integer" | "real" | "text" | "blob";

export interface EncodedCell {
  storageClass: SqlStorageClass;
  sqlLiteral: string;
}

export interface EncodedRow {
  [column: string]: EncodedCell;
}

export interface TableSecondaryObject {
  type: "index" | "trigger";
  name: string;
  sql: string;
}

export interface ColumnDefinition {
  name: string;
  type: string;
  notNull?: boolean | undefined;
  primaryKey?: boolean | undefined;
  unique?: boolean | undefined;
  default?:
    | {
        kind: "literal";
        value: JsonPrimitive;
      }
    | {
        kind: "sql";
        expr: "CURRENT_TIMESTAMP" | "CURRENT_DATE" | "CURRENT_TIME";
      }
    | undefined;
}

export interface CreateTableOperation {
  type: "create_table";
  table: string;
  columns: ColumnDefinition[];
}

export interface AddColumnOperation {
  type: "add_column";
  table: string;
  column: ColumnDefinition;
}

export interface InsertOperation {
  type: "insert";
  table: string;
  values: Record<string, JsonPrimitive>;
}

export interface DropTableOperation {
  type: "drop_table";
  table: string;
}

export interface DropColumnOperation {
  type: "drop_column";
  table: string;
  column: string;
}

export interface AlterColumnTypeOperation {
  type: "alter_column_type";
  table: string;
  column: string;
  newType: string;
}

export interface AddCheckOperation {
  type: "add_check";
  table: string;
  expression: string;
}

export interface DropCheckOperation {
  type: "drop_check";
  table: string;
  expression: string;
}

export interface RestoreTableOperation {
  type: "restore_table";
  table: string;
  ddlSql: string;
  rows: EncodedRow[] | null;
  secondaryObjects?: TableSecondaryObject[] | undefined;
}

export interface UpdateOperation {
  type: "update";
  table: string;
  values: Record<string, JsonPrimitive>;
  where: Record<string, JsonPrimitive>;
}

export interface DeleteOperation {
  type: "delete";
  table: string;
  where: Record<string, JsonPrimitive>;
}

export type Operation =
  | CreateTableOperation
  | AddColumnOperation
  | InsertOperation
  | DropTableOperation
  | DropColumnOperation
  | AlterColumnTypeOperation
  | AddCheckOperation
  | DropCheckOperation
  | RestoreTableOperation
  | UpdateOperation
  | DeleteOperation;

export interface OperationPlan {
  message: string;
  operations: Operation[];
}

export type CommitKind = "apply" | "revert" | "system";

export interface CommitEntry {
  commitId: string;
  seq: number;
  kind: CommitKind;
  message: string;
  createdAt: number;
  parentIds: string[];
  parentCount: number;
  schemaHashBefore: string;
  schemaHashAfter: string;
  stateHashAfter: string;
  planHash: string;
  inverseReady: boolean;
  revertedTargetId: string | null;
  operations: Operation[];
}

export type RemotePlatform = "turso" | "libsql";

export interface SyncConfig {
  platform: RemotePlatform;
  remoteUrl: string;
  remoteDbName: string | null;
  autoSync: boolean;
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

export interface TossSyncStatus {
  configured: boolean;
  remotePlatform: RemotePlatform | null;
  remoteUrl: string | null;
  remoteDbName: string | null;
  autoSync: boolean;
  state: SyncState;
  lastPushedCommit: string | null;
  lastPulledCommit: string | null;
  pendingCommits: number;
  lastError: string | null;
}

export interface StorageEstimate {
  commitCount: number;
  estimatedHistoryBytes: number;
  latestCommitEstimatedBytes: number | null;
}

export interface StatusTable {
  name: string;
  count: number;
}

export interface TossStatus {
  dbPath: string;
  tableCount: number;
  tables: StatusTable[];
  headCommit: {
    commitId: string;
    seq: number;
    kind: CommitKind;
    message: string;
    createdAt: number;
  } | null;
  snapshotCount: number;
  lastVerifiedAt: string | null;
  lastVerifiedOk: boolean | null;
  lastVerifiedOkAt: string | null;
  sync: TossSyncStatus;
  storage: StorageEstimate;
}

export interface RevertConflict {
  kind: "row" | "schema";
  table: string;
  pk?: Record<string, string> | undefined;
  column?: string | undefined;
  reason: string;
}

export interface RevertSuccessResult {
  ok: true;
  revertCommit: CommitEntry;
}

export interface RevertConflictResult {
  ok: false;
  conflicts: RevertConflict[];
}

export type RevertResult = RevertSuccessResult | RevertConflictResult;

export interface VerifyResult {
  ok: boolean;
  mode: "quick" | "full";
  chainValid: boolean;
  quickCheck: string;
  integrityCheck?: string | undefined;
  issues: string[];
  checkedAt: string;
}

export interface SnapshotEntry {
  commitId: string;
  filePath: string;
  fileSha256: string;
  createdAt: number;
  rowCountHint: number;
}

export type SkillPlatform = "claude" | "cursor" | "codex" | "opencode" | "openclaw";

export interface InitDatabaseOptions {
  dbPath?: string;
  generateSkills?: boolean;
  skillPlatforms?: SkillPlatform[] | undefined;
  openclawHeartbeat?: boolean;
  forceNew?: boolean;
}
