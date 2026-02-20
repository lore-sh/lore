CREATE TABLE `_toss_commit_parent` (
	`commit_id` text NOT NULL,
	`parent_commit_id` text NOT NULL,
	`ord` integer NOT NULL,
	PRIMARY KEY(`commit_id`, `ord`),
	FOREIGN KEY (`commit_id`) REFERENCES `_toss_commit`(`commit_id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`parent_commit_id`) REFERENCES `_toss_commit`(`commit_id`) ON UPDATE no action ON DELETE restrict,
	CONSTRAINT "chk_toss_commit_parent_ord_non_negative" CHECK("_toss_commit_parent"."ord" >= 0)
);
--> statement-breakpoint
CREATE INDEX `idx_toss_commit_parent_parent` ON `_toss_commit_parent` (`parent_commit_id`);--> statement-breakpoint
CREATE UNIQUE INDEX `uq_toss_commit_parent_commit_parent` ON `_toss_commit_parent` (`commit_id`,`parent_commit_id`);--> statement-breakpoint
CREATE TABLE `_toss_commit` (
	`commit_id` text PRIMARY KEY NOT NULL,
	`seq` integer NOT NULL,
	`kind` text NOT NULL,
	`message` text NOT NULL,
	`created_at` integer NOT NULL,
	`parent_count` integer NOT NULL,
	`schema_hash_before` text NOT NULL,
	`schema_hash_after` text NOT NULL,
	`state_hash_after` text NOT NULL,
	`plan_hash` text NOT NULL,
	`inverse_ready` integer NOT NULL,
	`reverted_target_id` text,
	FOREIGN KEY (`reverted_target_id`) REFERENCES `_toss_commit`(`commit_id`) ON UPDATE no action ON DELETE set null,
	CONSTRAINT "chk_toss_commit_seq_positive" CHECK("_toss_commit"."seq" > 0),
	CONSTRAINT "chk_toss_commit_message_non_empty" CHECK(length(trim("_toss_commit"."message")) > 0),
	CONSTRAINT "chk_toss_commit_created_at_non_negative" CHECK("_toss_commit"."created_at" >= 0),
	CONSTRAINT "chk_toss_commit_parent_count_non_negative" CHECK("_toss_commit"."parent_count" >= 0),
	CONSTRAINT "chk_toss_commit_inverse_ready_bool" CHECK("_toss_commit"."inverse_ready" in (0, 1)),
	CONSTRAINT "chk_toss_commit_id_hash_len" CHECK(length("_toss_commit"."commit_id") = 64),
	CONSTRAINT "chk_toss_commit_schema_before_hash_len" CHECK(length("_toss_commit"."schema_hash_before") = 64),
	CONSTRAINT "chk_toss_commit_schema_after_hash_len" CHECK(length("_toss_commit"."schema_hash_after") = 64),
	CONSTRAINT "chk_toss_commit_state_hash_len" CHECK(length("_toss_commit"."state_hash_after") = 64),
	CONSTRAINT "chk_toss_commit_plan_hash_len" CHECK(length("_toss_commit"."plan_hash") = 64),
	CONSTRAINT "chk_toss_commit_reverted_target_hash_len_or_null" CHECK("_toss_commit"."reverted_target_id" is null or length("_toss_commit"."reverted_target_id") = 64)
);
--> statement-breakpoint
CREATE UNIQUE INDEX `_toss_commit_seq_unique` ON `_toss_commit` (`seq`);--> statement-breakpoint
CREATE TABLE `_toss_effect_row` (
	`commit_id` text NOT NULL,
	`effect_index` integer NOT NULL,
	`table_name` text NOT NULL,
	`pk_json` text NOT NULL,
	`op_kind` text NOT NULL,
	`before_row_json` text,
	`after_row_json` text,
	`before_hash` text,
	`after_hash` text,
	PRIMARY KEY(`commit_id`, `effect_index`),
	FOREIGN KEY (`commit_id`) REFERENCES `_toss_commit`(`commit_id`) ON UPDATE no action ON DELETE cascade,
	CONSTRAINT "chk_toss_effect_row_effect_index_non_negative" CHECK("_toss_effect_row"."effect_index" >= 0),
	CONSTRAINT "chk_toss_effect_row_table_name_non_empty" CHECK(length(trim("_toss_effect_row"."table_name")) > 0),
	CONSTRAINT "chk_toss_effect_row_pk_json_valid" CHECK(json_valid("_toss_effect_row"."pk_json")),
	CONSTRAINT "chk_toss_effect_row_before_json_valid_or_null" CHECK("_toss_effect_row"."before_row_json" is null or json_valid("_toss_effect_row"."before_row_json")),
	CONSTRAINT "chk_toss_effect_row_after_json_valid_or_null" CHECK("_toss_effect_row"."after_row_json" is null or json_valid("_toss_effect_row"."after_row_json")),
	CONSTRAINT "chk_toss_effect_row_before_hash_len_or_null" CHECK("_toss_effect_row"."before_hash" is null or length("_toss_effect_row"."before_hash") = 64),
	CONSTRAINT "chk_toss_effect_row_after_hash_len_or_null" CHECK("_toss_effect_row"."after_hash" is null or length("_toss_effect_row"."after_hash") = 64),
	CONSTRAINT "chk_toss_effect_row_before_hash_pairing" CHECK(("_toss_effect_row"."before_hash" is null) = ("_toss_effect_row"."before_row_json" is null)),
	CONSTRAINT "chk_toss_effect_row_after_hash_pairing" CHECK(("_toss_effect_row"."after_hash" is null) = ("_toss_effect_row"."after_row_json" is null)),
	CONSTRAINT "chk_toss_effect_row_op_shape" CHECK(("_toss_effect_row"."op_kind" = 'insert' and "_toss_effect_row"."before_row_json" is null and "_toss_effect_row"."after_row_json" is not null)
        or ("_toss_effect_row"."op_kind" = 'update' and "_toss_effect_row"."before_row_json" is not null and "_toss_effect_row"."after_row_json" is not null)
        or ("_toss_effect_row"."op_kind" = 'delete' and "_toss_effect_row"."before_row_json" is not null and "_toss_effect_row"."after_row_json" is null))
);
--> statement-breakpoint
CREATE INDEX `idx_toss_effect_row_table_pk` ON `_toss_effect_row` (`table_name`,`pk_json`);--> statement-breakpoint
CREATE TABLE `_toss_effect_schema` (
	`commit_id` text NOT NULL,
	`effect_index` integer NOT NULL,
	`table_name` text NOT NULL,
	`before_table_json` text,
	`after_table_json` text,
	PRIMARY KEY(`commit_id`, `effect_index`),
	FOREIGN KEY (`commit_id`) REFERENCES `_toss_commit`(`commit_id`) ON UPDATE no action ON DELETE cascade,
	CONSTRAINT "chk_toss_effect_schema_effect_index_non_negative" CHECK("_toss_effect_schema"."effect_index" >= 0),
	CONSTRAINT "chk_toss_effect_schema_table_name_non_empty" CHECK(length(trim("_toss_effect_schema"."table_name")) > 0),
	CONSTRAINT "chk_toss_effect_schema_before_json_valid_or_null" CHECK("_toss_effect_schema"."before_table_json" is null or json_valid("_toss_effect_schema"."before_table_json")),
	CONSTRAINT "chk_toss_effect_schema_after_json_valid_or_null" CHECK("_toss_effect_schema"."after_table_json" is null or json_valid("_toss_effect_schema"."after_table_json")),
	CONSTRAINT "chk_toss_effect_schema_some_side_exists" CHECK("_toss_effect_schema"."before_table_json" is not null or "_toss_effect_schema"."after_table_json" is not null)
);
--> statement-breakpoint
CREATE INDEX `idx_toss_effect_schema_table_column` ON `_toss_effect_schema` (`table_name`);--> statement-breakpoint
CREATE TABLE `_toss_engine_meta` (
	`key` text PRIMARY KEY NOT NULL,
	`value` text NOT NULL,
	CONSTRAINT "chk_toss_engine_meta_key_non_empty" CHECK(length("_toss_engine_meta"."key") > 0)
);
--> statement-breakpoint
CREATE TABLE `_toss_op` (
	`commit_id` text NOT NULL,
	`op_index` integer NOT NULL,
	`op_type` text NOT NULL,
	`op_json` text NOT NULL,
	PRIMARY KEY(`commit_id`, `op_index`),
	FOREIGN KEY (`commit_id`) REFERENCES `_toss_commit`(`commit_id`) ON UPDATE no action ON DELETE cascade,
	CONSTRAINT "chk_toss_op_op_index_non_negative" CHECK("_toss_op"."op_index" >= 0),
	CONSTRAINT "chk_toss_op_json_valid" CHECK(json_valid("_toss_op"."op_json"))
);
--> statement-breakpoint
CREATE TABLE `_toss_ref` (
	`name` text PRIMARY KEY NOT NULL,
	`commit_id` text,
	`updated_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	FOREIGN KEY (`commit_id`) REFERENCES `_toss_commit`(`commit_id`) ON UPDATE no action ON DELETE set null,
	CONSTRAINT "chk_toss_ref_name_non_empty" CHECK(length(trim("_toss_ref"."name")) > 0),
	CONSTRAINT "chk_toss_ref_updated_at_non_negative" CHECK("_toss_ref"."updated_at" >= 0),
	CONSTRAINT "chk_toss_ref_commit_id_hash_len_or_null" CHECK("_toss_ref"."commit_id" is null or length("_toss_ref"."commit_id") = 64)
);
--> statement-breakpoint
CREATE TABLE `_toss_reflog` (
	`id` integer PRIMARY KEY NOT NULL,
	`ref_name` text NOT NULL,
	`old_commit_id` text,
	`new_commit_id` text,
	`reason` text NOT NULL,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	FOREIGN KEY (`ref_name`) REFERENCES `_toss_ref`(`name`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`old_commit_id`) REFERENCES `_toss_commit`(`commit_id`) ON UPDATE no action ON DELETE set null,
	FOREIGN KEY (`new_commit_id`) REFERENCES `_toss_commit`(`commit_id`) ON UPDATE no action ON DELETE set null,
	CONSTRAINT "chk_toss_reflog_created_at_non_negative" CHECK("_toss_reflog"."created_at" >= 0),
	CONSTRAINT "chk_toss_reflog_old_commit_id_hash_len_or_null" CHECK("_toss_reflog"."old_commit_id" is null or length("_toss_reflog"."old_commit_id") = 64),
	CONSTRAINT "chk_toss_reflog_new_commit_id_hash_len_or_null" CHECK("_toss_reflog"."new_commit_id" is null or length("_toss_reflog"."new_commit_id") = 64)
);
--> statement-breakpoint
CREATE INDEX `idx_toss_reflog_ref_id` ON `_toss_reflog` (`ref_name`,`id`);--> statement-breakpoint
CREATE TABLE `_toss_snapshot` (
	`commit_id` text PRIMARY KEY NOT NULL,
	`file_path` text NOT NULL,
	`file_sha256` text NOT NULL,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	`row_count_hint` integer NOT NULL,
	FOREIGN KEY (`commit_id`) REFERENCES `_toss_commit`(`commit_id`) ON UPDATE no action ON DELETE cascade,
	CONSTRAINT "chk_toss_snapshot_created_at_non_negative" CHECK("_toss_snapshot"."created_at" >= 0),
	CONSTRAINT "chk_toss_snapshot_row_count_non_negative" CHECK("_toss_snapshot"."row_count_hint" >= 0),
	CONSTRAINT "chk_toss_snapshot_sha256_len" CHECK(length("_toss_snapshot"."file_sha256") = 64)
);
--> statement-breakpoint
CREATE INDEX `idx_toss_snapshot_created_at` ON `_toss_snapshot` (`created_at`);