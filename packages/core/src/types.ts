export type JsonPrimitive = string | number | boolean | null;

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
  | UpdateOperation
  | DeleteOperation;

export interface OperationPlan {
  message: string;
  operations: Operation[];
  source?: SourceInfo | undefined;
}

export type LogKind = "apply" | "revert" | "system";

export interface LogEntry {
  rowid: number;
  id: string;
  timestamp: string;
  kind: LogKind;
  message: string;
  operations: Operation[];
  schemaVersion: number;
  checksum: string;
  revertedTargetId: string | null;
}

export interface StatusTable {
  name: string;
  count: number;
}

export interface TossStatus {
  dbPath: string;
  schemaVersion: number;
  tableCount: number;
  tables: StatusTable[];
  latestCommit: {
    id: string;
    timestamp: string;
    kind: LogKind;
    message: string;
  } | null;
}
