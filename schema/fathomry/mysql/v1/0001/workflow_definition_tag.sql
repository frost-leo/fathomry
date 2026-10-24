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

CREATE TABLE {{target}}.workflow_definition_tag (
    workflow_definition_id BIGINT NOT NULL COMMENT 'en: Immutable owning workflow definition; tag writes use its management revision; zh-CN: 不可变的所属工作流定义；标签写入使用该定义的管理修订号',
    tag_key VARBINARY(128) NOT NULL COMMENT 'en: Immutable raw metadata key, 1 to 128 UTF-8 bytes; reserved keys are rejected; zh-CN: 不可变原始元数据键，1 至 128 个 UTF-8 字节；拒绝保留键',
    tag_value VARBINARY(256) NOT NULL COMMENT 'en: Raw metadata value, at most 256 UTF-8 bytes; empty is present; no credentials; zh-CN: 原始元数据值，最多 256 个 UTF-8 字节；空值表示存在；不得保存凭据',
    PRIMARY KEY (workflow_definition_id, tag_key),
    CONSTRAINT fk_workflow_definition_tag_definition FOREIGN KEY (workflow_definition_id)
        REFERENCES {{target}}.workflow_definition (id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT ck_workflow_definition_tag_key CHECK (OCTET_LENGTH(tag_key) BETWEEN 1 AND 128),
    CONSTRAINT ck_workflow_definition_tag_value CHECK (OCTET_LENGTH(tag_value) <= 256)
) ENGINE = InnoDB ROW_FORMAT = DYNAMIC DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin COMMENT = 'en: Definition-owned bounded raw key/value metadata; at most 64 tags per definition; zh-CN: 定义拥有的有界原始键值元数据；每个定义最多 64 项';
