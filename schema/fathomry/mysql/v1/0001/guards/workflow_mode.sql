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

CREATE TRIGGER {{target}}.immutable_workflow_mode
BEFORE UPDATE ON {{target}}.workflow_mode
FOR EACH ROW
BEGIN
    IF NOT (OLD.id <=> NEW.id)
       OR NOT (OLD.created_at <=> NEW.created_at)
       OR NOT (OLD.workflow_version_id <=> NEW.workflow_version_id)
       OR NOT (OLD.code <=> NEW.code) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'immutable catalog field';
    END IF;
END;
