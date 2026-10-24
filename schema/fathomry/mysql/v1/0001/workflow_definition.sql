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

CREATE TABLE {{target}}.workflow_definition (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'en: Environment-local immutable row identity; zh-CN: 环境内不可变行标识',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'en: Immutable creation time, UTC microsecond precision; zh-CN: 不可变创建时间，UTC 微秒精度',
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'en: Time of effective management change, UTC microsecond precision; zh-CN: 有效管理变更时间，UTC 微秒精度',
    row_revision BIGINT NOT NULL DEFAULT 1 COMMENT 'en: Positive optimistic management revision, not a workflow or mode version; zh-CN: 正整数管理并发修订号，不是工作流版本或模式版本',
    namespace VARBINARY(255) NOT NULL COMMENT 'en: Native Temporal Namespace; exact UTF-8 identity, at most 255 bytes; zh-CN: 原生 Temporal 命名空间；精确 UTF-8 身份，最多 255 字节',
    code VARBINARY(128) NOT NULL COMMENT 'en: Immutable workflow code, at most 128 bytes; not a display name; zh-CN: 不可变工作流编码，最多 128 字节；不是展示名称',
    category_id BIGINT COMMENT 'en: Optional primary category in the same Namespace; NULL means unclassified; zh-CN: 同一命名空间内的可选主分类；NULL 表示未分类',
    enabled BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'en: New-run admission intent; initially disabled, not Worker readiness; zh-CN: 新执行准入意图；初始禁用，不代表 Worker 就绪',
    default_version_id BIGINT COMMENT 'en: Default version owned by this definition; NULL means unconfigured, never latest; zh-CN: 本定义拥有的默认版本；NULL 表示未配置，绝不表示最新版本',
    CONSTRAINT uq_workflow_definition_identity UNIQUE (namespace, code),
    CONSTRAINT fk_workflow_definition_category FOREIGN KEY (namespace, category_id)
        REFERENCES {{target}}.workflow_category (namespace, id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT ck_workflow_definition_revision CHECK (row_revision > 0),
    CONSTRAINT ck_workflow_definition_namespace CHECK (OCTET_LENGTH(namespace) BETWEEN 1 AND 255),
    CONSTRAINT ck_workflow_definition_code CHECK (OCTET_LENGTH(code) BETWEEN 1 AND 128),
    CONSTRAINT ck_workflow_definition_enabled CHECK (enabled IN (0, 1))
) ENGINE = InnoDB ROW_FORMAT = DYNAMIC DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin COMMENT = 'en: Stable workflow identity and management intent, not runtime or deployment state; zh-CN: 稳定工作流身份及管理意图，不表示运行或部署状态';
