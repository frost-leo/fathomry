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

CREATE TRIGGER immutable_workflow_category
BEFORE UPDATE ON {{target}}.workflow_category
FOR EACH ROW WHEN (
    OLD.id IS DISTINCT FROM NEW.id
    OR OLD.created_at IS DISTINCT FROM NEW.created_at
    OR OLD.namespace IS DISTINCT FROM NEW.namespace
    OR OLD.code IS DISTINCT FROM NEW.code
)
EXECUTE FUNCTION {{target}}.reject_catalog_change();
