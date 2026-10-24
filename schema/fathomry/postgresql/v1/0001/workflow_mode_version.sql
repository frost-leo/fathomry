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
    CREATE TABLE {{target}}.workflow_mode_version (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        created_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        row_revision BIGINT NOT NULL DEFAULT 1,
        workflow_mode_id BIGINT NOT NULL,
        version_code TEXT COLLATE pg_catalog."C" NOT NULL,
        config_format TEXT COLLATE pg_catalog."C" NOT NULL,
        fingerprint_profile TEXT COLLATE pg_catalog."C" NOT NULL,
        config_sha256 BYTEA NOT NULL,
        config_document BYTEA NOT NULL,
        enabled BOOLEAN NOT NULL DEFAULT FALSE,
        CONSTRAINT uq_workflow_mode_version_identity UNIQUE (workflow_mode_id, version_code),
        CONSTRAINT uq_workflow_mode_version_owner_id UNIQUE (workflow_mode_id, id),
        CONSTRAINT fk_workflow_mode_version_mode FOREIGN KEY (workflow_mode_id)
            REFERENCES {{target}}.workflow_mode (id) ON UPDATE RESTRICT ON DELETE RESTRICT,
        CONSTRAINT ck_workflow_mode_version_revision CHECK (row_revision > 0),
        CONSTRAINT ck_workflow_mode_version_code CHECK (OCTET_LENGTH(version_code) BETWEEN 1 AND 128),
        CONSTRAINT ck_workflow_mode_version_format CHECK (OCTET_LENGTH(config_format) BETWEEN 1 AND 64),
        CONSTRAINT ck_workflow_mode_version_profile CHECK (OCTET_LENGTH(fingerprint_profile) BETWEEN 1 AND 64),
        CONSTRAINT ck_workflow_mode_version_digest CHECK (OCTET_LENGTH(config_sha256) = 32),
        CONSTRAINT ck_workflow_mode_version_document CHECK (OCTET_LENGTH(config_document) BETWEEN 1 AND 1048576)
    );

    COMMENT ON TABLE {{target}}.workflow_mode_version IS 'en: Immutable complete control values for one named mode and explicit revision; zh-CN: 一个命名模式及显式修订版本的不可变完整控制参数';
    COMMENT ON COLUMN {{target}}.workflow_mode_version.id IS 'en: Environment-local immutable row identity; zh-CN: 环境内不可变行标识';
    COMMENT ON COLUMN {{target}}.workflow_mode_version.created_at IS 'en: Immutable creation time, UTC microsecond precision; zh-CN: 不可变创建时间，UTC 微秒精度';
    COMMENT ON COLUMN {{target}}.workflow_mode_version.updated_at IS 'en: Time of effective management change, UTC microsecond precision; zh-CN: 有效管理变更时间，UTC 微秒精度';
    COMMENT ON COLUMN {{target}}.workflow_mode_version.row_revision IS 'en: Positive optimistic management revision, not a workflow or mode version; zh-CN: 正整数管理并发修订号，不是工作流版本或模式版本';
    COMMENT ON COLUMN {{target}}.workflow_mode_version.workflow_mode_id IS 'en: Immutable owning version-scoped named mode; zh-CN: 不可变的所属版本内命名模式';
    COMMENT ON COLUMN {{target}}.workflow_mode_version.version_code IS 'en: Immutable explicit control revision, at most 128 bytes; not row_revision; zh-CN: 不可变显式控制参数修订标签，最多 128 字节；不是 row_revision';
    COMMENT ON COLUMN {{target}}.workflow_mode_version.config_format IS 'en: Immutable control-value document format identifier, at most 64 bytes; zh-CN: 不可变控制参数文档格式标识，最多 64 字节';
    COMMENT ON COLUMN {{target}}.workflow_mode_version.fingerprint_profile IS 'en: Immutable control canonicalization and comparison profile, at most 64 bytes; zh-CN: 不可变控制参数规范化及比较协议标识，最多 64 字节';
    COMMENT ON COLUMN {{target}}.workflow_mode_version.config_sha256 IS 'en: Immutable 32-byte comparison digest; deliberately not unique; zh-CN: 不可变的 32 字节比较摘要；刻意不设唯一约束';
    COMMENT ON COLUMN {{target}}.workflow_mode_version.config_document IS 'en: Immutable canonical complete control JSON bytes, 1 byte to 1 MiB; no runtime handles; zh-CN: 不可变规范化完整控制 JSON 字节，1 字节至 1 MiB；不包含运行时句柄';
    COMMENT ON COLUMN {{target}}.workflow_mode_version.enabled IS 'en: Allows new selection of this exact control revision; initially disabled; zh-CN: 是否允许新选择此确定控制参数修订版本；初始禁用';
END;
$catalog_table$;
