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

CREATE TABLE {{target}}.i18n_locale (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'en: Environment-local immutable row identity; zh-CN: 环境内不可变行标识',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'en: Immutable creation time, UTC microsecond precision; zh-CN: 不可变创建时间，UTC 微秒精度',
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'en: Time of effective management change, UTC microsecond precision; zh-CN: 有效管理变更时间，UTC 微秒精度',
    row_revision BIGINT NOT NULL DEFAULT 1 COMMENT 'en: Positive optimistic management revision, not a workflow or mode version; zh-CN: 正整数管理并发修订号，不是工作流版本或模式版本',
    code VARCHAR(128) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL COMMENT 'en: Canonical locale tag; case-insensitive identity, at most 128 bytes; zh-CN: 规范化语言标签；身份不区分大小写，最多 128 字节',
    native_name VARCHAR(128) NOT NULL COMMENT 'en: Language autonym, 1 to 128 Unicode characters; zh-CN: 语言自称，1 至 128 个 Unicode 字符',
    text_direction VARBINARY(3) NOT NULL DEFAULT 'ltr' COMMENT 'en: Text direction: ltr or rtl; zh-CN: 文字方向：ltr 或 rtl',
    enabled BOOLEAN NOT NULL DEFAULT TRUE COMMENT 'en: Available for display selection; English must remain enabled; zh-CN: 是否可选择用于展示；英语必须保持启用',
    sort_order INTEGER NOT NULL DEFAULT 0 COMMENT 'en: Nonnegative display-selection order; zh-CN: 非负展示选择顺序',
    CONSTRAINT uq_i18n_locale_code UNIQUE (code),
    CONSTRAINT ck_i18n_locale_revision CHECK (row_revision > 0),
    CONSTRAINT ck_i18n_locale_code CHECK (OCTET_LENGTH(code) BETWEEN 1 AND 128),
    CONSTRAINT ck_i18n_locale_name CHECK (CHAR_LENGTH(native_name) BETWEEN 1 AND 128),
    CONSTRAINT ck_i18n_locale_direction CHECK (text_direction IN ('ltr', 'rtl')),
    CONSTRAINT ck_i18n_locale_enabled CHECK (enabled IN (0, 1)),
    CONSTRAINT ck_i18n_locale_sort CHECK (sort_order >= 0),
    CONSTRAINT ck_i18n_locale_english CHECK (code <> 'en' OR enabled = 1)
) ENGINE = InnoDB ROW_FORMAT = DYNAMIC DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin COMMENT = 'en: Display locale directory; availability does not imply translation coverage; zh-CN: 展示语言目录；可用状态不代表翻译覆盖完整';
