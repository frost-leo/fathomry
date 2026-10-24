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

CREATE TABLE {{target}}.workflow_mode (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'en: Environment-local immutable row identity; zh-CN: 环境内不可变行标识',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'en: Immutable creation time, UTC microsecond precision; zh-CN: 不可变创建时间，UTC 微秒精度',
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'en: Time of effective management change, UTC microsecond precision; zh-CN: 有效管理变更时间，UTC 微秒精度',
    row_revision BIGINT NOT NULL DEFAULT 1 COMMENT 'en: Positive optimistic management revision, not a workflow or mode version; zh-CN: 正整数管理并发修订号，不是工作流版本或模式版本',
    workflow_version_id BIGINT NOT NULL COMMENT 'en: Immutable workflow version that interprets these controls; zh-CN: 不可变的、负责解释这些控制参数的工作流版本',
    code VARBINARY(128) NOT NULL COMMENT 'en: Immutable preset code within its workflow version, at most 128 bytes; zh-CN: 工作流版本内不可变的预设编码，最多 128 字节',
    enabled BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'en: Allows new selection of this named mode; initially disabled; zh-CN: 是否允许新选择此命名模式；初始禁用',
    default_mode_version_id BIGINT COMMENT 'en: Default revision owned by this mode; NULL means unconfigured, never latest; zh-CN: 本模式拥有的默认修订版本；NULL 表示未配置，绝不表示最新修订',
    CONSTRAINT uq_workflow_mode_identity UNIQUE (workflow_version_id, code),
    CONSTRAINT uq_workflow_mode_owner_id UNIQUE (workflow_version_id, id),
    CONSTRAINT fk_workflow_mode_version FOREIGN KEY (workflow_version_id)
        REFERENCES {{target}}.workflow_version (id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT ck_workflow_mode_revision CHECK (row_revision > 0),
    CONSTRAINT ck_workflow_mode_code CHECK (OCTET_LENGTH(code) BETWEEN 1 AND 128),
    CONSTRAINT ck_workflow_mode_enabled CHECK (enabled IN (0, 1))
) ENGINE = InnoDB ROW_FORMAT = DYNAMIC DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin COMMENT = 'en: Named control preset owned by one exact workflow version; zh-CN: 由一个确定工作流版本拥有的命名控制预设';
