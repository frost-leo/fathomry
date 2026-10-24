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
    CREATE TABLE {{target}}.workflow_definition (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        created_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        row_revision BIGINT NOT NULL DEFAULT 1,
        namespace TEXT COLLATE pg_catalog."C" NOT NULL,
        code TEXT COLLATE pg_catalog."C" NOT NULL,
        category_id BIGINT,
        enabled BOOLEAN NOT NULL DEFAULT FALSE,
        default_version_id BIGINT,
        CONSTRAINT uq_workflow_definition_identity UNIQUE (namespace, code),
        CONSTRAINT fk_workflow_definition_category FOREIGN KEY (namespace, category_id)
            REFERENCES {{target}}.workflow_category (namespace, id) ON UPDATE RESTRICT ON DELETE RESTRICT,
        CONSTRAINT ck_workflow_definition_revision CHECK (row_revision > 0),
        CONSTRAINT ck_workflow_definition_namespace CHECK (OCTET_LENGTH(namespace) BETWEEN 1 AND 255),
        CONSTRAINT ck_workflow_definition_code CHECK (OCTET_LENGTH(code) BETWEEN 1 AND 128)
    );

    COMMENT ON TABLE {{target}}.workflow_definition IS 'en: Stable workflow identity and management intent, not runtime or deployment state; zh-CN: 稳定工作流身份及管理意图，不表示运行或部署状态';
    COMMENT ON COLUMN {{target}}.workflow_definition.id IS 'en: Environment-local immutable row identity; zh-CN: 环境内不可变行标识';
    COMMENT ON COLUMN {{target}}.workflow_definition.created_at IS 'en: Immutable creation time, UTC microsecond precision; zh-CN: 不可变创建时间，UTC 微秒精度';
    COMMENT ON COLUMN {{target}}.workflow_definition.updated_at IS 'en: Time of effective management change, UTC microsecond precision; zh-CN: 有效管理变更时间，UTC 微秒精度';
    COMMENT ON COLUMN {{target}}.workflow_definition.row_revision IS 'en: Positive optimistic management revision, not a workflow or mode version; zh-CN: 正整数管理并发修订号，不是工作流版本或模式版本';
    COMMENT ON COLUMN {{target}}.workflow_definition.namespace IS 'en: Native Temporal Namespace; exact UTF-8 identity, at most 255 bytes; zh-CN: 原生 Temporal 命名空间；精确 UTF-8 身份，最多 255 字节';
    COMMENT ON COLUMN {{target}}.workflow_definition.code IS 'en: Immutable workflow code, at most 128 bytes; not a display name; zh-CN: 不可变工作流编码，最多 128 字节；不是展示名称';
    COMMENT ON COLUMN {{target}}.workflow_definition.category_id IS 'en: Optional primary category in the same Namespace; NULL means unclassified; zh-CN: 同一命名空间内的可选主分类；NULL 表示未分类';
    COMMENT ON COLUMN {{target}}.workflow_definition.enabled IS 'en: New-run admission intent; initially disabled, not Worker readiness; zh-CN: 新执行准入意图；初始禁用，不代表 Worker 就绪';
    COMMENT ON COLUMN {{target}}.workflow_definition.default_version_id IS 'en: Default version owned by this definition; NULL means unconfigured, never latest; zh-CN: 本定义拥有的默认版本；NULL 表示未配置，绝不表示最新版本';
END;
$catalog_table$;
