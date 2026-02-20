import { index, integer, primaryKey, sqliteTable, text } from "drizzle-orm/sqlite-core";

export const EngineMetaTable = sqliteTable("_toss_engine_meta", {
  key: text("key").primaryKey(),
  value: text("value").notNull(),
});

export const CommitTable = sqliteTable("_toss_commit", {
  commitId: text("commit_id").primaryKey(),
  seq: integer("seq").notNull().unique(),
  kind: text("kind").notNull(),
  message: text("message").notNull(),
  createdAt: text("created_at").notNull(),
  parentCount: integer("parent_count").notNull(),
  schemaHashBefore: text("schema_hash_before").notNull(),
  schemaHashAfter: text("schema_hash_after").notNull(),
  stateHashAfter: text("state_hash_after").notNull(),
  planHash: text("plan_hash").notNull(),
  inverseReady: integer("inverse_ready").notNull(),
  revertedTargetId: text("reverted_target_id"),
});

export const CommitParentTable = sqliteTable(
  "_toss_commit_parent",
  {
    commitId: text("commit_id").notNull(),
    parentCommitId: text("parent_commit_id").notNull(),
    ord: integer("ord").notNull(),
  },
  (table) => ({
    pk: primaryKey({ columns: [table.commitId, table.ord] }),
  }),
);

export const RefTable = sqliteTable("_toss_ref", {
  name: text("name").primaryKey(),
  commitId: text("commit_id"),
  updatedAt: text("updated_at").notNull(),
});

export const ReflogTable = sqliteTable("_toss_reflog", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  refName: text("ref_name").notNull(),
  oldCommitId: text("old_commit_id"),
  newCommitId: text("new_commit_id"),
  reason: text("reason").notNull(),
  createdAt: text("created_at").notNull(),
});

export const OpTable = sqliteTable(
  "_toss_op",
  {
    commitId: text("commit_id").notNull(),
    opIndex: integer("op_index").notNull(),
    opType: text("op_type").notNull(),
    opJson: text("op_json").notNull(),
  },
  (table) => ({
    pk: primaryKey({ columns: [table.commitId, table.opIndex] }),
  }),
);

export const EffectRowTable = sqliteTable(
  "_toss_effect_row",
  {
    commitId: text("commit_id").notNull(),
    effectIndex: integer("effect_index").notNull(),
    tableName: text("table_name").notNull(),
    pkJson: text("pk_json").notNull(),
    opKind: text("op_kind").notNull(),
    beforeRowJson: text("before_row_json"),
    afterRowJson: text("after_row_json"),
    beforeHash: text("before_hash"),
    afterHash: text("after_hash"),
  },
  (table) => ({
    pk: primaryKey({ columns: [table.commitId, table.effectIndex] }),
    byTablePk: index("idx_toss_effect_row_table_pk").on(table.tableName, table.pkJson),
  }),
);

export const EffectSchemaTable = sqliteTable(
  "_toss_effect_schema",
  {
    commitId: text("commit_id").notNull(),
    effectIndex: integer("effect_index").notNull(),
    tableName: text("table_name").notNull(),
    beforeTableJson: text("before_table_json"),
    afterTableJson: text("after_table_json"),
  },
  (table) => ({
    pk: primaryKey({ columns: [table.commitId, table.effectIndex] }),
    byTable: index("idx_toss_effect_schema_table_column").on(table.tableName),
  }),
);

export const SnapshotTable = sqliteTable("_toss_snapshot", {
  commitId: text("commit_id").primaryKey(),
  filePath: text("file_path").notNull(),
  fileSha256: text("file_sha256").notNull(),
  createdAt: text("created_at").notNull(),
  rowCountHint: integer("row_count_hint").notNull(),
});
