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
    CREATE TABLE {{target}}.i18n_text (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        created_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        row_revision BIGINT NOT NULL DEFAULT 1,
        resource_id BIGINT NOT NULL,
        locale_id BIGINT NOT NULL,
        value TEXT NOT NULL,
        CONSTRAINT uq_i18n_text_resource_locale UNIQUE (resource_id, locale_id),
        CONSTRAINT fk_i18n_text_resource FOREIGN KEY (resource_id)
            REFERENCES {{target}}.i18n_resource (id) ON UPDATE RESTRICT ON DELETE RESTRICT,
        CONSTRAINT fk_i18n_text_locale FOREIGN KEY (locale_id)
            REFERENCES {{target}}.i18n_locale (id) ON UPDATE RESTRICT ON DELETE RESTRICT,
        CONSTRAINT ck_i18n_text_revision CHECK (row_revision > 0),
        CONSTRAINT ck_i18n_text_value CHECK (OCTET_LENGTH(value) <= 16384)
    );

    COMMENT ON TABLE {{target}}.i18n_text IS 'en: Independent text per resource and locale; missing rows use English fallback; zh-CN: 每个资源和语言独立保存的文本；缺失行使用英语回退';
    COMMENT ON COLUMN {{target}}.i18n_text.id IS 'en: Environment-local immutable row identity; zh-CN: 环境内不可变行标识';
    COMMENT ON COLUMN {{target}}.i18n_text.created_at IS 'en: Immutable creation time, UTC microsecond precision; zh-CN: 不可变创建时间，UTC 微秒精度';
    COMMENT ON COLUMN {{target}}.i18n_text.updated_at IS 'en: Time of effective management change, UTC microsecond precision; zh-CN: 有效管理变更时间，UTC 微秒精度';
    COMMENT ON COLUMN {{target}}.i18n_text.row_revision IS 'en: Positive optimistic management revision, not a workflow or mode version; zh-CN: 正整数管理并发修订号，不是工作流版本或模式版本';
    COMMENT ON COLUMN {{target}}.i18n_text.resource_id IS 'en: Immutable owning resource identity; zh-CN: 不可变的所属资源标识';
    COMMENT ON COLUMN {{target}}.i18n_text.locale_id IS 'en: Immutable display-locale identity; zh-CN: 不可变的展示语言标识';
    COMMENT ON COLUMN {{target}}.i18n_text.value IS 'en: UTF-8 display text, at most 16 KiB; permitted empty text is intentional, not missing; zh-CN: UTF-8 展示文本，最多 16 KiB；字段允许的空文本表示有意留空，不代表缺失';
END;
$catalog_table$;
