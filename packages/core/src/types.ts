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

export interface SourceInfo {
  planner?: string | undefined;
  skill?: string | undefined;
}

export interface ColumnDefinition {
  name: string;
  type: string;
  notNull?: boolean | undefined;
  primaryKey?: boolean | undefined;
  unique?: boolean | undefined;
  default?: JsonPrimitive | undefined;
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
  | RestoreTableOperation
  | UpdateOperation
  | DeleteOperation;

export interface OperationPlan {
  message: string;
  operations: Operation[];
  source?: SourceInfo | undefined;
}

export type CommitKind = "apply" | "revert" | "system";

export interface CommitEntry {
  commitId: string;
  seq: number;
  kind: CommitKind;
  message: string;
  createdAt: string;
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
    createdAt: string;
  } | null;
  snapshotCount: number;
  lastVerifiedAt: string | null;
  lastVerifiedOk: boolean | null;
  lastVerifiedOkAt: string | null;
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
  createdAt: string;
  rowCountHint: number;
}

export interface DatabaseOptions {
  dbPath?: string;
}

export type SkillPlatform = "claude" | "cursor" | "codex" | "opencode" | "openclaw";

export interface InitDatabaseOptions extends DatabaseOptions {
  generateSkills?: boolean;
  skillPlatforms?: SkillPlatform[] | undefined;
  forceNew?: boolean;
}
