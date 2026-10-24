-- fathomry
-- Copyright (C) 2026  Frost Leo
-- SPDX-License-Identifier: GPL-3.0-or-later
--
-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation, either version 3 of the License, or
-- (at your option) any later version.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with this program. If not, see <http://www.gnu.org/licenses/>.

DO $catalog_table$
BEGIN
    CREATE TABLE {{target}}.fathomry_schema_history (
        family TEXT COLLATE pg_catalog."C" NOT NULL,
        migration_revision BIGINT NOT NULL,
        step_ordinal INTEGER NOT NULL,
        manifest_sha256 BYTEA NOT NULL,
        step_id TEXT COLLATE pg_catalog."C" NOT NULL,
        step_sha256 BYTEA NOT NULL,
        state TEXT COLLATE pg_catalog."C" NOT NULL,
        created_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        acknowledged_at TIMESTAMPTZ(6),
        CONSTRAINT pk_fathomry_schema_history PRIMARY KEY (migration_revision, step_ordinal),
        CONSTRAINT ck_schema_history_family CHECK (OCTET_LENGTH(family) BETWEEN 1 AND 64),
        CONSTRAINT ck_schema_history_revision CHECK (migration_revision > 0),
        CONSTRAINT ck_schema_history_ordinal CHECK (step_ordinal BETWEEN 0 AND 128),
        CONSTRAINT ck_schema_history_manifest CHECK (OCTET_LENGTH(manifest_sha256) = 32),
        CONSTRAINT ck_schema_history_step CHECK (OCTET_LENGTH(step_id) BETWEEN 1 AND 128),
        CONSTRAINT ck_schema_history_digest CHECK (OCTET_LENGTH(step_sha256) = 32),
        CONSTRAINT ck_schema_history_state CHECK (
            (step_ordinal = 0 AND state IN ('intent', 'complete'))
            OR (step_ordinal > 0 AND state IN ('intent', 'applied'))
        ),
        CONSTRAINT ck_schema_history_acknowledgement CHECK (
            (state = 'intent' AND acknowledged_at IS NULL)
            OR (state <> 'intent' AND acknowledged_at IS NOT NULL)
        )
    );

    COMMENT ON TABLE {{target}}.fathomry_schema_history IS 'en: Bounded installation intent and acknowledgement journal; not proof of authenticated origin; zh-CN: 有界安装意图与确认日志；不证明来源已经认证';
    COMMENT ON COLUMN {{target}}.fathomry_schema_history.family IS 'en: Schema family identity, independent of package and server versions; zh-CN: Schema 家族标识，独立于包版本和数据库版本';
    COMMENT ON COLUMN {{target}}.fathomry_schema_history.migration_revision IS 'en: Positive logical migration revision; zh-CN: 正整数逻辑迁移修订号';
    COMMENT ON COLUMN {{target}}.fathomry_schema_history.step_ordinal IS 'en: Zero identifies the migration header; positive values identify ordered statements; zh-CN: 零表示迁移头记录；正整数表示有序语句';
    COMMENT ON COLUMN {{target}}.fathomry_schema_history.manifest_sha256 IS 'en: Exact 32-byte digest of the selected embedded manifest; zh-CN: 所选嵌入式 manifest 的精确 32 字节摘要';
    COMMENT ON COLUMN {{target}}.fathomry_schema_history.step_id IS 'en: Immutable manifest step identity, or migration name for the header; zh-CN: 不可变 manifest 步骤标识，头记录使用迁移名称';
    COMMENT ON COLUMN {{target}}.fathomry_schema_history.step_sha256 IS 'en: Exact 32-byte original SQL asset digest; the header binds bootstrap SQL; zh-CN: 原始 SQL 资产的精确 32 字节摘要；头记录绑定引导 SQL';
    COMMENT ON COLUMN {{target}}.fathomry_schema_history.state IS 'en: Intent is not proof of effect; applied acknowledges a step; complete terminates the migration; zh-CN: intent 不证明实际效果；applied 表示步骤已确认；complete 表示迁移终结';
    COMMENT ON COLUMN {{target}}.fathomry_schema_history.created_at IS 'en: Immutable journal creation time, UTC microsecond precision; zh-CN: 不可变日志创建时间，UTC 微秒精度';
    COMMENT ON COLUMN {{target}}.fathomry_schema_history.acknowledged_at IS 'en: Acknowledgement time, UTC microsecond precision; NULL means no recorded acknowledgement; zh-CN: 确认时间，UTC 微秒精度；NULL 表示没有已记录的确认';
END;
$catalog_table$;
