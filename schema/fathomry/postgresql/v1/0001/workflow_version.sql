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
    CREATE TABLE {{target}}.workflow_version (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        created_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        row_revision BIGINT NOT NULL DEFAULT 1,
        workflow_definition_id BIGINT NOT NULL,
        version_code TEXT COLLATE pg_catalog."C" NOT NULL,
        spec_format TEXT COLLATE pg_catalog."C" NOT NULL,
        fingerprint_profile TEXT COLLATE pg_catalog."C" NOT NULL,
        spec_sha256 BYTEA NOT NULL,
        spec_document BYTEA NOT NULL,
        enabled BOOLEAN NOT NULL DEFAULT FALSE,
        default_mode_id BIGINT,
        CONSTRAINT uq_workflow_version_identity UNIQUE (workflow_definition_id, version_code),
        CONSTRAINT uq_workflow_version_owner_id UNIQUE (workflow_definition_id, id),
        CONSTRAINT fk_workflow_version_definition FOREIGN KEY (workflow_definition_id)
            REFERENCES {{target}}.workflow_definition (id) ON UPDATE RESTRICT ON DELETE RESTRICT,
        CONSTRAINT ck_workflow_version_revision CHECK (row_revision > 0),
        CONSTRAINT ck_workflow_version_code CHECK (OCTET_LENGTH(version_code) BETWEEN 1 AND 128),
        CONSTRAINT ck_workflow_version_format CHECK (OCTET_LENGTH(spec_format) BETWEEN 1 AND 64),
        CONSTRAINT ck_workflow_version_profile CHECK (OCTET_LENGTH(fingerprint_profile) BETWEEN 1 AND 64),
        CONSTRAINT ck_workflow_version_digest CHECK (OCTET_LENGTH(spec_sha256) = 32),
        CONSTRAINT ck_workflow_version_document CHECK (OCTET_LENGTH(spec_document) BETWEEN 1 AND 4194304)
    );

    COMMENT ON TABLE {{target}}.workflow_version IS 'en: Immutable executable declaration with separately mutable admission and default mode; zh-CN: 不可变执行声明，以及独立可变的准入状态和默认模式';
    COMMENT ON COLUMN {{target}}.workflow_version.id IS 'en: Environment-local immutable row identity; zh-CN: 环境内不可变行标识';
    COMMENT ON COLUMN {{target}}.workflow_version.created_at IS 'en: Immutable creation time, UTC microsecond precision; zh-CN: 不可变创建时间，UTC 微秒精度';
    COMMENT ON COLUMN {{target}}.workflow_version.updated_at IS 'en: Time of effective management change, UTC microsecond precision; zh-CN: 有效管理变更时间，UTC 微秒精度';
    COMMENT ON COLUMN {{target}}.workflow_version.row_revision IS 'en: Positive optimistic management revision, not a workflow or mode version; zh-CN: 正整数管理并发修订号，不是工作流版本或模式版本';
    COMMENT ON COLUMN {{target}}.workflow_version.workflow_definition_id IS 'en: Immutable owning stable workflow definition; zh-CN: 不可变的所属稳定工作流定义';
    COMMENT ON COLUMN {{target}}.workflow_version.version_code IS 'en: Immutable explicit version label, at most 128 bytes; never inferred from a hash; zh-CN: 不可变的显式版本标签，最多 128 字节；不由哈希推导';
    COMMENT ON COLUMN {{target}}.workflow_version.spec_format IS 'en: Immutable executable-document format identifier, at most 64 bytes; zh-CN: 不可变执行文档格式标识，最多 64 字节';
    COMMENT ON COLUMN {{target}}.workflow_version.fingerprint_profile IS 'en: Immutable canonicalization and conflict-comparison profile, at most 64 bytes; zh-CN: 不可变规范化及冲突比较协议标识，最多 64 字节';
    COMMENT ON COLUMN {{target}}.workflow_version.spec_sha256 IS 'en: Immutable 32-byte comparison digest; deliberately not unique; zh-CN: 不可变的 32 字节比较摘要；刻意不设唯一约束';
    COMMENT ON COLUMN {{target}}.workflow_version.spec_document IS 'en: Immutable exact canonical executable JSON bytes, 1 byte to 4 MiB; zh-CN: 不可变规范化执行 JSON 精确字节，1 字节至 4 MiB';
    COMMENT ON COLUMN {{target}}.workflow_version.enabled IS 'en: Allows new selection of this version; does not prove deployment or govern old-run recovery; zh-CN: 是否允许新选择此版本；不证明已部署，也不控制旧执行恢复';
    COMMENT ON COLUMN {{target}}.workflow_version.default_mode_id IS 'en: Default mode owned by this version; NULL means unconfigured, never latest; zh-CN: 本版本拥有的默认模式；NULL 表示未配置，绝不表示最新模式';
END;
$catalog_table$;
