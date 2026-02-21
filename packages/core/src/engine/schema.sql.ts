import { sql } from "drizzle-orm";
import { check, foreignKey, index, integer, primaryKey, sqliteTable, text, unique } from "drizzle-orm/sqlite-core";

const nowDefault = sql`(unixepoch() * 1000)`;
const commitKindValues = ["apply", "revert"] as const;
const reflogReasonValues = ["apply", "revert"] as const;
const opTypeValues = [
  "create_table",
  "add_column",
  "insert",
  "drop_table",
  "drop_column",
  "alter_column_type",
  "add_check",
  "drop_check",
  "restore_table",
  "update",
  "delete",
] as const;
const opKindValues = ["insert", "update", "delete"] as const;

export const EngineMetaTable = sqliteTable(
  "_toss_engine_meta",
  {
    key: text("key").primaryKey(),
    value: text("value").notNull(),
  },
  (table) => [
    check("chk_toss_engine_meta_key_non_empty", sql`length(${table.key}) > 0`),
  ],
);

export const CommitTable = sqliteTable(
  "_toss_commit",
  {
    commitId: text("commit_id").primaryKey(),
    seq: integer("seq").notNull().unique(),
    kind: text("kind", { enum: commitKindValues }).notNull(),
    message: text("message").notNull(),
    createdAt: integer("created_at").notNull(),
    parentCount: integer("parent_count").notNull(),
    schemaHashBefore: text("schema_hash_before").notNull(),
    schemaHashAfter: text("schema_hash_after").notNull(),
    stateHashAfter: text("state_hash_after").notNull(),
    planHash: text("plan_hash").notNull(),
    inverseReady: integer("inverse_ready").notNull(),
    revertedTargetId: text("reverted_target_id"),
  },
  (table) => [
    foreignKey({
      columns: [table.revertedTargetId],
      foreignColumns: [table.commitId],
    }).onDelete("set null"),
    check("chk_toss_commit_seq_positive", sql`${table.seq} > 0`),
    check("chk_toss_commit_message_non_empty", sql`length(trim(${table.message})) > 0`),
    check("chk_toss_commit_created_at_non_negative", sql`${table.createdAt} >= 0`),
    check("chk_toss_commit_parent_count_non_negative", sql`${table.parentCount} >= 0`),
    check("chk_toss_commit_inverse_ready_bool", sql`${table.inverseReady} in (0, 1)`),
    check("chk_toss_commit_id_hash_len", sql`length(${table.commitId}) = 64`),
    check("chk_toss_commit_schema_before_hash_len", sql`length(${table.schemaHashBefore}) = 64`),
    check("chk_toss_commit_schema_after_hash_len", sql`length(${table.schemaHashAfter}) = 64`),
    check("chk_toss_commit_state_hash_len", sql`length(${table.stateHashAfter}) = 64`),
    check("chk_toss_commit_plan_hash_len", sql`length(${table.planHash}) = 64`),
    check(
      "chk_toss_commit_reverted_target_hash_len_or_null",
      sql`${table.revertedTargetId} is null or length(${table.revertedTargetId}) = 64`,
    ),
  ],
);

export const CommitParentTable = sqliteTable(
  "_toss_commit_parent",
  {
    commitId: text("commit_id")
      .notNull()
      .references(() => CommitTable.commitId, { onDelete: "cascade" }),
    parentCommitId: text("parent_commit_id")
      .notNull()
      .references(() => CommitTable.commitId, { onDelete: "restrict" }),
    ord: integer("ord").notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.commitId, table.ord] }),
    unique("uq_toss_commit_parent_commit_parent").on(table.commitId, table.parentCommitId),
    index("idx_toss_commit_parent_parent").on(table.parentCommitId),
    check("chk_toss_commit_parent_ord_non_negative", sql`${table.ord} >= 0`),
  ],
);

export const RefTable = sqliteTable(
  "_toss_ref",
  {
    name: text("name").primaryKey(),
    commitId: text("commit_id").references(() => CommitTable.commitId, { onDelete: "set null" }),
    updatedAt: integer("updated_at").notNull().default(nowDefault),
  },
  (table) => [
    check("chk_toss_ref_name_non_empty", sql`length(trim(${table.name})) > 0`),
    check("chk_toss_ref_updated_at_non_negative", sql`${table.updatedAt} >= 0`),
    check("chk_toss_ref_commit_id_hash_len_or_null", sql`${table.commitId} is null or length(${table.commitId}) = 64`),
  ],
);

export const ReflogTable = sqliteTable(
  "_toss_reflog",
  {
    id: integer("id").primaryKey(),
    refName: text("ref_name")
      .notNull()
      .references(() => RefTable.name, { onDelete: "cascade" }),
    oldCommitId: text("old_commit_id").references(() => CommitTable.commitId, {
      onDelete: "set null",
    }),
    newCommitId: text("new_commit_id").references(() => CommitTable.commitId, {
      onDelete: "set null",
    }),
    reason: text("reason", { enum: reflogReasonValues }).notNull(),
    createdAt: integer("created_at").notNull().default(nowDefault),
  },
  (table) => [
    index("idx_toss_reflog_ref_id").on(table.refName, table.id),
    check("chk_toss_reflog_created_at_non_negative", sql`${table.createdAt} >= 0`),
    check("chk_toss_reflog_old_commit_id_hash_len_or_null", sql`${table.oldCommitId} is null or length(${table.oldCommitId}) = 64`),
    check("chk_toss_reflog_new_commit_id_hash_len_or_null", sql`${table.newCommitId} is null or length(${table.newCommitId}) = 64`),
  ],
);

export const OpTable = sqliteTable(
  "_toss_op",
  {
    commitId: text("commit_id")
      .notNull()
      .references(() => CommitTable.commitId, { onDelete: "cascade" }),
    opIndex: integer("op_index").notNull(),
    opType: text("op_type", { enum: opTypeValues }).notNull(),
    opJson: text("op_json").notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.commitId, table.opIndex] }),
    check("chk_toss_op_op_index_non_negative", sql`${table.opIndex} >= 0`),
    check("chk_toss_op_json_valid", sql`json_valid(${table.opJson})`),
  ],
);

export const EffectRowTable = sqliteTable(
  "_toss_effect_row",
  {
    commitId: text("commit_id")
      .notNull()
      .references(() => CommitTable.commitId, { onDelete: "cascade" }),
    effectIndex: integer("effect_index").notNull(),
    tableName: text("table_name").notNull(),
    pkJson: text("pk_json").notNull(),
    opKind: text("op_kind", { enum: opKindValues }).notNull(),
    beforeRowJson: text("before_row_json"),
    afterRowJson: text("after_row_json"),
    beforeHash: text("before_hash"),
    afterHash: text("after_hash"),
  },
  (table) => [
    primaryKey({ columns: [table.commitId, table.effectIndex] }),
    index("idx_toss_effect_row_table_pk").on(table.tableName, table.pkJson),
    check("chk_toss_effect_row_effect_index_non_negative", sql`${table.effectIndex} >= 0`),
    check("chk_toss_effect_row_table_name_non_empty", sql`length(trim(${table.tableName})) > 0`),
    check("chk_toss_effect_row_pk_json_valid", sql`json_valid(${table.pkJson})`),
    check(
      "chk_toss_effect_row_before_json_valid_or_null",
      sql`${table.beforeRowJson} is null or json_valid(${table.beforeRowJson})`,
    ),
    check(
      "chk_toss_effect_row_after_json_valid_or_null",
      sql`${table.afterRowJson} is null or json_valid(${table.afterRowJson})`,
    ),
    check(
      "chk_toss_effect_row_before_hash_len_or_null",
      sql`${table.beforeHash} is null or length(${table.beforeHash}) = 64`,
    ),
    check(
      "chk_toss_effect_row_after_hash_len_or_null",
      sql`${table.afterHash} is null or length(${table.afterHash}) = 64`,
    ),
    check(
      "chk_toss_effect_row_before_hash_pairing",
      sql`(${table.beforeHash} is null) = (${table.beforeRowJson} is null)`,
    ),
    check(
      "chk_toss_effect_row_after_hash_pairing",
      sql`(${table.afterHash} is null) = (${table.afterRowJson} is null)`,
    ),
    check(
      "chk_toss_effect_row_op_shape",
      sql`(${table.opKind} = 'insert' and ${table.beforeRowJson} is null and ${table.afterRowJson} is not null)
        or (${table.opKind} = 'update' and ${table.beforeRowJson} is not null and ${table.afterRowJson} is not null)
        or (${table.opKind} = 'delete' and ${table.beforeRowJson} is not null and ${table.afterRowJson} is null)`,
    ),
  ],
);

export const EffectSchemaTable = sqliteTable(
  "_toss_effect_schema",
  {
    commitId: text("commit_id")
      .notNull()
      .references(() => CommitTable.commitId, { onDelete: "cascade" }),
    effectIndex: integer("effect_index").notNull(),
    tableName: text("table_name").notNull(),
    beforeTableJson: text("before_table_json"),
    afterTableJson: text("after_table_json"),
  },
  (table) => [
    primaryKey({ columns: [table.commitId, table.effectIndex] }),
    index("idx_toss_effect_schema_table_column").on(table.tableName),
    check("chk_toss_effect_schema_effect_index_non_negative", sql`${table.effectIndex} >= 0`),
    check("chk_toss_effect_schema_table_name_non_empty", sql`length(trim(${table.tableName})) > 0`),
    check(
      "chk_toss_effect_schema_before_json_valid_or_null",
      sql`${table.beforeTableJson} is null or json_valid(${table.beforeTableJson})`,
    ),
    check(
      "chk_toss_effect_schema_after_json_valid_or_null",
      sql`${table.afterTableJson} is null or json_valid(${table.afterTableJson})`,
    ),
    check(
      "chk_toss_effect_schema_some_side_exists",
      sql`${table.beforeTableJson} is not null or ${table.afterTableJson} is not null`,
    ),
  ],
);

export const SnapshotTable = sqliteTable(
  "_toss_snapshot",
  {
    commitId: text("commit_id")
      .primaryKey()
      .references(() => CommitTable.commitId, { onDelete: "cascade" }),
    filePath: text("file_path").notNull(),
    fileSha256: text("file_sha256").notNull(),
    createdAt: integer("created_at").notNull().default(nowDefault),
    rowCountHint: integer("row_count_hint").notNull(),
  },
  (table) => [
    index("idx_toss_snapshot_created_at").on(table.createdAt),
    check("chk_toss_snapshot_created_at_non_negative", sql`${table.createdAt} >= 0`),
    check("chk_toss_snapshot_row_count_non_negative", sql`${table.rowCountHint} >= 0`),
    check("chk_toss_snapshot_sha256_len", sql`length(${table.fileSha256}) = 64`),
  ],
);
