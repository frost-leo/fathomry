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

CREATE TRIGGER immutable_workflow_version
BEFORE UPDATE ON {{target}}.workflow_version
FOR EACH ROW WHEN (
    OLD.id IS DISTINCT FROM NEW.id
    OR OLD.created_at IS DISTINCT FROM NEW.created_at
    OR OLD.workflow_definition_id IS DISTINCT FROM NEW.workflow_definition_id
    OR OLD.version_code IS DISTINCT FROM NEW.version_code
    OR OLD.spec_format IS DISTINCT FROM NEW.spec_format
    OR OLD.fingerprint_profile IS DISTINCT FROM NEW.fingerprint_profile
    OR OLD.spec_sha256 IS DISTINCT FROM NEW.spec_sha256
    OR OLD.spec_document IS DISTINCT FROM NEW.spec_document
)
EXECUTE FUNCTION {{target}}.reject_catalog_change();
