CREATE TABLE `_lore_commit_parent` (
	`commit_id` text NOT NULL,
	`parent_commit_id` text NOT NULL,
	`ord` integer NOT NULL,
	PRIMARY KEY(`commit_id`, `ord`),
	FOREIGN KEY (`commit_id`) REFERENCES `_lore_commit`(`commit_id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`parent_commit_id`) REFERENCES `_lore_commit`(`commit_id`) ON UPDATE no action ON DELETE restrict,
	CONSTRAINT "chk_lore_commit_parent_ord_non_negative" CHECK("_lore_commit_parent"."ord" >= 0)
);
--> statement-breakpoint
CREATE INDEX `idx_lore_commit_parent_parent` ON `_lore_commit_parent` (`parent_commit_id`);--> statement-breakpoint
CREATE UNIQUE INDEX `uq_lore_commit_parent_commit_parent` ON `_lore_commit_parent` (`commit_id`,`parent_commit_id`);--> statement-breakpoint
CREATE TABLE `_lore_commit` (
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
	`revertible` integer NOT NULL,
	`revert_target_id` text,
	FOREIGN KEY (`revert_target_id`) REFERENCES `_lore_commit`(`commit_id`) ON UPDATE no action ON DELETE set null,
	CONSTRAINT "chk_lore_commit_seq_positive" CHECK("_lore_commit"."seq" > 0),
	CONSTRAINT "chk_lore_commit_message_non_empty" CHECK(length(trim("_lore_commit"."message")) > 0),
	CONSTRAINT "chk_lore_commit_created_at_non_negative" CHECK("_lore_commit"."created_at" >= 0),
	CONSTRAINT "chk_lore_commit_parent_count_non_negative" CHECK("_lore_commit"."parent_count" >= 0),
	CONSTRAINT "chk_lore_commit_revertible_bool" CHECK("_lore_commit"."revertible" in (0, 1)),
	CONSTRAINT "chk_lore_commit_id_hash_len" CHECK(length("_lore_commit"."commit_id") = 64),
	CONSTRAINT "chk_lore_commit_schema_before_hash_len" CHECK(length("_lore_commit"."schema_hash_before") = 64),
	CONSTRAINT "chk_lore_commit_schema_after_hash_len" CHECK(length("_lore_commit"."schema_hash_after") = 64),
	CONSTRAINT "chk_lore_commit_state_hash_len" CHECK(length("_lore_commit"."state_hash_after") = 64),
	CONSTRAINT "chk_lore_commit_plan_hash_len" CHECK(length("_lore_commit"."plan_hash") = 64),
	CONSTRAINT "chk_lore_commit_revert_target_hash_len_or_null" CHECK("_lore_commit"."revert_target_id" is null or length("_lore_commit"."revert_target_id") = 64)
);
--> statement-breakpoint
CREATE UNIQUE INDEX `_lore_commit_seq_unique` ON `_lore_commit` (`seq`);--> statement-breakpoint
CREATE TABLE `_lore_meta` (
	`key` text PRIMARY KEY NOT NULL,
	`value` text NOT NULL,
	CONSTRAINT "chk_lore_meta_key_non_empty" CHECK(length("_lore_meta"."key") > 0)
);
--> statement-breakpoint
CREATE TABLE `_lore_op` (
	`commit_id` text NOT NULL,
	`op_index` integer NOT NULL,
	`op_type` text NOT NULL,
	`op_json` text NOT NULL,
	PRIMARY KEY(`commit_id`, `op_index`),
	FOREIGN KEY (`commit_id`) REFERENCES `_lore_commit`(`commit_id`) ON UPDATE no action ON DELETE cascade,
	CONSTRAINT "chk_lore_op_op_index_non_negative" CHECK("_lore_op"."op_index" >= 0),
	CONSTRAINT "chk_lore_op_json_valid" CHECK(json_valid("_lore_op"."op_json"))
);
--> statement-breakpoint
CREATE TABLE `_lore_ref` (
	`name` text PRIMARY KEY NOT NULL,
	`commit_id` text,
	`updated_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	FOREIGN KEY (`commit_id`) REFERENCES `_lore_commit`(`commit_id`) ON UPDATE no action ON DELETE set null,
	CONSTRAINT "chk_lore_ref_name_non_empty" CHECK(length(trim("_lore_ref"."name")) > 0),
	CONSTRAINT "chk_lore_ref_updated_at_non_negative" CHECK("_lore_ref"."updated_at" >= 0),
	CONSTRAINT "chk_lore_ref_commit_id_hash_len_or_null" CHECK("_lore_ref"."commit_id" is null or length("_lore_ref"."commit_id") = 64)
);
--> statement-breakpoint
CREATE TABLE `_lore_reflog` (
	`id` integer PRIMARY KEY NOT NULL,
	`ref_name` text NOT NULL,
	`old_commit_id` text,
	`new_commit_id` text,
	`reason` text NOT NULL,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	FOREIGN KEY (`ref_name`) REFERENCES `_lore_ref`(`name`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`old_commit_id`) REFERENCES `_lore_commit`(`commit_id`) ON UPDATE no action ON DELETE set null,
	FOREIGN KEY (`new_commit_id`) REFERENCES `_lore_commit`(`commit_id`) ON UPDATE no action ON DELETE set null,
	CONSTRAINT "chk_lore_reflog_created_at_non_negative" CHECK("_lore_reflog"."created_at" >= 0),
	CONSTRAINT "chk_lore_reflog_old_commit_id_hash_len_or_null" CHECK("_lore_reflog"."old_commit_id" is null or length("_lore_reflog"."old_commit_id") = 64),
	CONSTRAINT "chk_lore_reflog_new_commit_id_hash_len_or_null" CHECK("_lore_reflog"."new_commit_id" is null or length("_lore_reflog"."new_commit_id") = 64)
);
--> statement-breakpoint
CREATE INDEX `idx_lore_reflog_ref_name_id` ON `_lore_reflog` (`ref_name`,`id`);--> statement-breakpoint
CREATE TABLE `_lore_row_effect` (
	`commit_id` text NOT NULL,
	`effect_index` integer NOT NULL,
	`table_name` text NOT NULL,
	`pk_json` text NOT NULL,
	`op_kind` text NOT NULL,
	`before_json` text,
	`after_json` text,
	`before_hash` text,
	`after_hash` text,
	PRIMARY KEY(`commit_id`, `effect_index`),
	FOREIGN KEY (`commit_id`) REFERENCES `_lore_commit`(`commit_id`) ON UPDATE no action ON DELETE cascade,
	CONSTRAINT "chk_lore_row_effect_effect_index_non_negative" CHECK("_lore_row_effect"."effect_index" >= 0),
	CONSTRAINT "chk_lore_row_effect_table_name_non_empty" CHECK(length(trim("_lore_row_effect"."table_name")) > 0),
	CONSTRAINT "chk_lore_row_effect_pk_json_valid" CHECK(json_valid("_lore_row_effect"."pk_json")),
	CONSTRAINT "chk_lore_row_effect_before_json_valid_or_null" CHECK("_lore_row_effect"."before_json" is null or json_valid("_lore_row_effect"."before_json")),
	CONSTRAINT "chk_lore_row_effect_after_json_valid_or_null" CHECK("_lore_row_effect"."after_json" is null or json_valid("_lore_row_effect"."after_json")),
	CONSTRAINT "chk_lore_row_effect_before_hash_len_or_null" CHECK("_lore_row_effect"."before_hash" is null or length("_lore_row_effect"."before_hash") = 64),
	CONSTRAINT "chk_lore_row_effect_after_hash_len_or_null" CHECK("_lore_row_effect"."after_hash" is null or length("_lore_row_effect"."after_hash") = 64),
	CONSTRAINT "chk_lore_row_effect_before_hash_pairing" CHECK(("_lore_row_effect"."before_hash" is null) = ("_lore_row_effect"."before_json" is null)),
	CONSTRAINT "chk_lore_row_effect_after_hash_pairing" CHECK(("_lore_row_effect"."after_hash" is null) = ("_lore_row_effect"."after_json" is null)),
	CONSTRAINT "chk_lore_row_effect_op_shape" CHECK(("_lore_row_effect"."op_kind" = 'insert' and "_lore_row_effect"."before_json" is null and "_lore_row_effect"."after_json" is not null)
        or ("_lore_row_effect"."op_kind" = 'update' and "_lore_row_effect"."before_json" is not null and "_lore_row_effect"."after_json" is not null)
        or ("_lore_row_effect"."op_kind" = 'delete' and "_lore_row_effect"."before_json" is not null and "_lore_row_effect"."after_json" is null))
);
--> statement-breakpoint
CREATE INDEX `idx_lore_row_effect_table_pk` ON `_lore_row_effect` (`table_name`,`pk_json`);--> statement-breakpoint
CREATE TABLE `_lore_schema_effect` (
	`commit_id` text NOT NULL,
	`effect_index` integer NOT NULL,
	`table_name` text NOT NULL,
	`before_json` text,
	`after_json` text,
	PRIMARY KEY(`commit_id`, `effect_index`),
	FOREIGN KEY (`commit_id`) REFERENCES `_lore_commit`(`commit_id`) ON UPDATE no action ON DELETE cascade,
	CONSTRAINT "chk_lore_schema_effect_effect_index_non_negative" CHECK("_lore_schema_effect"."effect_index" >= 0),
	CONSTRAINT "chk_lore_schema_effect_table_name_non_empty" CHECK(length(trim("_lore_schema_effect"."table_name")) > 0),
	CONSTRAINT "chk_lore_schema_effect_before_json_valid_or_null" CHECK("_lore_schema_effect"."before_json" is null or json_valid("_lore_schema_effect"."before_json")),
	CONSTRAINT "chk_lore_schema_effect_after_json_valid_or_null" CHECK("_lore_schema_effect"."after_json" is null or json_valid("_lore_schema_effect"."after_json")),
	CONSTRAINT "chk_lore_schema_effect_some_side_exists" CHECK("_lore_schema_effect"."before_json" is not null or "_lore_schema_effect"."after_json" is not null)
);
--> statement-breakpoint
CREATE INDEX `idx_lore_schema_effect_table_name` ON `_lore_schema_effect` (`table_name`);--> statement-breakpoint
CREATE TABLE `_lore_snapshot` (
	`commit_id` text PRIMARY KEY NOT NULL,
	`file_path` text NOT NULL,
	`file_sha256` text NOT NULL,
	`created_at` integer DEFAULT (unixepoch() * 1000) NOT NULL,
	`row_count_hint` integer NOT NULL,
	FOREIGN KEY (`commit_id`) REFERENCES `_lore_commit`(`commit_id`) ON UPDATE no action ON DELETE cascade,
	CONSTRAINT "chk_lore_snapshot_created_at_non_negative" CHECK("_lore_snapshot"."created_at" >= 0),
	CONSTRAINT "chk_lore_snapshot_row_count_non_negative" CHECK("_lore_snapshot"."row_count_hint" >= 0),
	CONSTRAINT "chk_lore_snapshot_sha256_len" CHECK(length("_lore_snapshot"."file_sha256") = 64)
);
--> statement-breakpoint
CREATE INDEX `idx_lore_snapshot_created_at` ON `_lore_snapshot` (`created_at`);