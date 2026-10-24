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

CREATE TABLE {{target}}.i18n_resource (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'en: Environment-local immutable row identity; zh-CN: 环境内不可变行标识',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'en: Immutable creation time, UTC microsecond precision; zh-CN: 不可变创建时间，UTC 微秒精度',
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'en: Time of effective management change, UTC microsecond precision; zh-CN: 有效管理变更时间，UTC 微秒精度',
    row_revision BIGINT NOT NULL DEFAULT 1 COMMENT 'en: Positive optimistic management revision, not a workflow or mode version; zh-CN: 正整数管理并发修订号，不是工作流版本或模式版本',
    scope VARBINARY(64) NOT NULL COMMENT 'en: Immutable resource group, at most 64 bytes; not a Temporal Namespace; zh-CN: 不可变资源分组，最多 64 字节；不是 Temporal 命名空间',
    code VARBINARY(1024) NOT NULL COMMENT 'en: Immutable owner-derived resource key, at most 1024 bytes; zh-CN: 由所属对象生成的不可变资源键，最多 1024 字节',
    translator_note TEXT COMMENT 'en: Optional publisher-owned English translation context, at most 1024 UTF-8 bytes; zh-CN: 发布方维护的可选英文翻译上下文，最多 1024 个 UTF-8 字节',
    CONSTRAINT uq_i18n_resource_address UNIQUE (scope, code),
    CONSTRAINT ck_i18n_resource_revision CHECK (row_revision > 0),
    CONSTRAINT ck_i18n_resource_scope CHECK (OCTET_LENGTH(scope) BETWEEN 1 AND 64),
    CONSTRAINT ck_i18n_resource_code CHECK (OCTET_LENGTH(code) BETWEEN 1 AND 1024),
    CONSTRAINT ck_i18n_resource_note CHECK (OCTET_LENGTH(translator_note) <= 1024)
) ENGINE = InnoDB ROW_FORMAT = DYNAMIC DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin COMMENT = 'en: Stable declared display-resource identities; separate from localized text; zh-CN: 已声明展示资源的稳定身份；与各语言文本分离';
