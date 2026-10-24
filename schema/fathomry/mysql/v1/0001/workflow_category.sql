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

CREATE TABLE {{target}}.workflow_category (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'en: Environment-local immutable row identity; zh-CN: 环境内不可变行标识',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'en: Immutable creation time, UTC microsecond precision; zh-CN: 不可变创建时间，UTC 微秒精度',
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'en: Time of effective management change, UTC microsecond precision; zh-CN: 有效管理变更时间，UTC 微秒精度',
    row_revision BIGINT NOT NULL DEFAULT 1 COMMENT 'en: Positive optimistic management revision, not a workflow or mode version; zh-CN: 正整数管理并发修订号，不是工作流版本或模式版本',
    namespace VARBINARY(255) NOT NULL COMMENT 'en: Native Temporal Namespace; exact UTF-8 identity, at most 255 bytes; zh-CN: 原生 Temporal 命名空间；精确 UTF-8 身份，最多 255 字节',
    code VARBINARY(128) NOT NULL COMMENT 'en: Immutable category code, at most 128 bytes; zh-CN: 不可变分类编码，最多 128 字节',
    enabled BOOLEAN NOT NULL DEFAULT TRUE COMMENT 'en: Allows new category associations; disabling preserves existing associations; zh-CN: 是否允许新的分类关联；禁用后保留已有关系',
    sort_order INTEGER NOT NULL DEFAULT 0 COMMENT 'en: Nonnegative category display order; zh-CN: 非负分类展示顺序',
    CONSTRAINT uq_workflow_category_identity UNIQUE (namespace, code),
    CONSTRAINT uq_workflow_category_namespace_id UNIQUE (namespace, id),
    CONSTRAINT ck_workflow_category_revision CHECK (row_revision > 0),
    CONSTRAINT ck_workflow_category_namespace CHECK (OCTET_LENGTH(namespace) BETWEEN 1 AND 255),
    CONSTRAINT ck_workflow_category_code CHECK (OCTET_LENGTH(code) BETWEEN 1 AND 128),
    CONSTRAINT ck_workflow_category_enabled CHECK (enabled IN (0, 1)),
    CONSTRAINT ck_workflow_category_sort CHECK (sort_order >= 0)
) ENGINE = InnoDB ROW_FORMAT = DYNAMIC DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin COMMENT = 'en: Flat Namespace-local category directory, not a tenant or Workflow Type; zh-CN: 命名空间内的平面分类目录，不是租户或工作流类型';
