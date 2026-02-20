CREATE TABLE IF NOT EXISTS `_toss_engine_meta` (
  `key` TEXT PRIMARY KEY,
  `value` TEXT NOT NULL
);

--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `_toss_commit` (
  `commit_id` TEXT PRIMARY KEY,
  `seq` INTEGER NOT NULL UNIQUE,
  `kind` TEXT NOT NULL,
  `message` TEXT NOT NULL,
  `created_at` TEXT NOT NULL,
  `parent_count` INTEGER NOT NULL,
  `schema_hash_before` TEXT NOT NULL,
  `schema_hash_after` TEXT NOT NULL,
  `state_hash_after` TEXT NOT NULL,
  `plan_hash` TEXT NOT NULL,
  `inverse_ready` INTEGER NOT NULL,
  `reverted_target_id` TEXT
);

--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `_toss_commit_parent` (
  `commit_id` TEXT NOT NULL,
  `parent_commit_id` TEXT NOT NULL,
  `ord` INTEGER NOT NULL,
  PRIMARY KEY (`commit_id`, `ord`)
);

--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `_toss_ref` (
  `name` TEXT PRIMARY KEY,
  `commit_id` TEXT,
  `updated_at` TEXT NOT NULL
);

--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `_toss_reflog` (
  `id` INTEGER PRIMARY KEY AUTOINCREMENT,
  `ref_name` TEXT NOT NULL,
  `old_commit_id` TEXT,
  `new_commit_id` TEXT,
  `reason` TEXT NOT NULL,
  `created_at` TEXT NOT NULL
);

--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `_toss_op` (
  `commit_id` TEXT NOT NULL,
  `op_index` INTEGER NOT NULL,
  `op_type` TEXT NOT NULL,
  `op_json` TEXT NOT NULL,
  PRIMARY KEY (`commit_id`, `op_index`)
);

--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `_toss_effect_row` (
  `commit_id` TEXT NOT NULL,
  `effect_index` INTEGER NOT NULL,
  `table_name` TEXT NOT NULL,
  `pk_json` TEXT NOT NULL,
  `op_kind` TEXT NOT NULL,
  `before_row_json` TEXT,
  `after_row_json` TEXT,
  `before_hash` TEXT,
  `after_hash` TEXT,
  PRIMARY KEY (`commit_id`, `effect_index`)
);

--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_toss_effect_row_table_pk`
ON `_toss_effect_row` (`table_name`, `pk_json`);

--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `_toss_effect_schema` (
  `commit_id` TEXT NOT NULL,
  `effect_index` INTEGER NOT NULL,
  `table_name` TEXT NOT NULL,
  `before_table_json` TEXT,
  `after_table_json` TEXT,
  PRIMARY KEY (`commit_id`, `effect_index`)
);

--> statement-breakpoint
CREATE INDEX IF NOT EXISTS `idx_toss_effect_schema_table_column`
ON `_toss_effect_schema` (`table_name`);

--> statement-breakpoint
CREATE TABLE IF NOT EXISTS `_toss_snapshot` (
  `commit_id` TEXT PRIMARY KEY,
  `file_path` TEXT NOT NULL,
  `file_sha256` TEXT NOT NULL,
  `created_at` TEXT NOT NULL,
  `row_count_hint` INTEGER NOT NULL
);

--> statement-breakpoint
INSERT INTO `_toss_engine_meta` (`key`, `value`)
VALUES ('schema_fingerprint', 'toss-canonical-observed-2026-02-19')
ON CONFLICT(`key`) DO UPDATE SET `value` = excluded.`value`;

--> statement-breakpoint
INSERT INTO `_toss_engine_meta` (`key`, `value`)
VALUES ('snapshot_interval', '100')
ON CONFLICT(`key`) DO UPDATE SET `value` = excluded.`value`;

--> statement-breakpoint
INSERT INTO `_toss_engine_meta` (`key`, `value`)
VALUES ('snapshot_retain', '20')
ON CONFLICT(`key`) DO UPDATE SET `value` = excluded.`value`;

--> statement-breakpoint
INSERT INTO `_toss_ref` (`name`, `commit_id`, `updated_at`)
VALUES ('main', NULL, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
ON CONFLICT(`name`) DO NOTHING;
