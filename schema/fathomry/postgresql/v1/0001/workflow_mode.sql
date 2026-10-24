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
    CREATE TABLE {{target}}.workflow_mode (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        created_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        row_revision BIGINT NOT NULL DEFAULT 1,
        workflow_version_id BIGINT NOT NULL,
        code TEXT COLLATE pg_catalog."C" NOT NULL,
        enabled BOOLEAN NOT NULL DEFAULT FALSE,
        default_mode_version_id BIGINT,
        CONSTRAINT uq_workflow_mode_identity UNIQUE (workflow_version_id, code),
        CONSTRAINT uq_workflow_mode_owner_id UNIQUE (workflow_version_id, id),
        CONSTRAINT fk_workflow_mode_version FOREIGN KEY (workflow_version_id)
            REFERENCES {{target}}.workflow_version (id) ON UPDATE RESTRICT ON DELETE RESTRICT,
        CONSTRAINT ck_workflow_mode_revision CHECK (row_revision > 0),
        CONSTRAINT ck_workflow_mode_code CHECK (OCTET_LENGTH(code) BETWEEN 1 AND 128)
    );

    COMMENT ON TABLE {{target}}.workflow_mode IS 'en: Named control preset owned by one exact workflow version; zh-CN: 由一个确定工作流版本拥有的命名控制预设';
    COMMENT ON COLUMN {{target}}.workflow_mode.id IS 'en: Environment-local immutable row identity; zh-CN: 环境内不可变行标识';
    COMMENT ON COLUMN {{target}}.workflow_mode.created_at IS 'en: Immutable creation time, UTC microsecond precision; zh-CN: 不可变创建时间，UTC 微秒精度';
    COMMENT ON COLUMN {{target}}.workflow_mode.updated_at IS 'en: Time of effective management change, UTC microsecond precision; zh-CN: 有效管理变更时间，UTC 微秒精度';
    COMMENT ON COLUMN {{target}}.workflow_mode.row_revision IS 'en: Positive optimistic management revision, not a workflow or mode version; zh-CN: 正整数管理并发修订号，不是工作流版本或模式版本';
    COMMENT ON COLUMN {{target}}.workflow_mode.workflow_version_id IS 'en: Immutable workflow version that interprets these controls; zh-CN: 不可变的、负责解释这些控制参数的工作流版本';
    COMMENT ON COLUMN {{target}}.workflow_mode.code IS 'en: Immutable preset code within its workflow version, at most 128 bytes; zh-CN: 工作流版本内不可变的预设编码，最多 128 字节';
    COMMENT ON COLUMN {{target}}.workflow_mode.enabled IS 'en: Allows new selection of this named mode; initially disabled; zh-CN: 是否允许新选择此命名模式；初始禁用';
    COMMENT ON COLUMN {{target}}.workflow_mode.default_mode_version_id IS 'en: Default revision owned by this mode; NULL means unconfigured, never latest; zh-CN: 本模式拥有的默认修订版本；NULL 表示未配置，绝不表示最新修订';
END;
$catalog_table$;
