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

CREATE TABLE {{target}}.i18n_text (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'en: Environment-local immutable row identity; zh-CN: 环境内不可变行标识',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'en: Immutable creation time, UTC microsecond precision; zh-CN: 不可变创建时间，UTC 微秒精度',
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'en: Time of effective management change, UTC microsecond precision; zh-CN: 有效管理变更时间，UTC 微秒精度',
    row_revision BIGINT NOT NULL DEFAULT 1 COMMENT 'en: Positive optimistic management revision, not a workflow or mode version; zh-CN: 正整数管理并发修订号，不是工作流版本或模式版本',
    resource_id BIGINT NOT NULL COMMENT 'en: Immutable owning resource identity; zh-CN: 不可变的所属资源标识',
    locale_id BIGINT NOT NULL COMMENT 'en: Immutable display-locale identity; zh-CN: 不可变的展示语言标识',
    `value` TEXT NOT NULL COMMENT 'en: UTF-8 display text, at most 16 KiB; permitted empty text is intentional, not missing; zh-CN: UTF-8 展示文本，最多 16 KiB；字段允许的空文本表示有意留空，不代表缺失',
    CONSTRAINT uq_i18n_text_resource_locale UNIQUE (resource_id, locale_id),
    CONSTRAINT fk_i18n_text_resource FOREIGN KEY (resource_id)
        REFERENCES {{target}}.i18n_resource (id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_i18n_text_locale FOREIGN KEY (locale_id)
        REFERENCES {{target}}.i18n_locale (id) ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT ck_i18n_text_revision CHECK (row_revision > 0),
    CONSTRAINT ck_i18n_text_value CHECK (OCTET_LENGTH(`value`) <= 16384)
) ENGINE = InnoDB ROW_FORMAT = DYNAMIC DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin COMMENT = 'en: Independent text per resource and locale; missing rows use English fallback; zh-CN: 每个资源和语言独立保存的文本；缺失行使用英语回退';
