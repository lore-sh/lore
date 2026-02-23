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
