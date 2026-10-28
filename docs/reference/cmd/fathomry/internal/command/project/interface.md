<!--
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
-->

# Project command group

[Documentation](../../../../../../README.md) / Private command reference

**Audience:** official CLI maintainers.
**Status:** implemented project group with one offline creation leaf.
**Package:** `github.com/frost-leo/fathomry/cmd/fathomry/internal/command/project`.

The group owns explicit command composition and its offline localized help.
`New` constructs the group and adds only the implemented
[create leaf](create/interface.md), exposed as `project new`.
`Components` gathers group and leaf metadata without filesystem or service work.

The official app selects this group. The public `cmd/fathomry/v1` entry does not
import it and remains the minimal shell consumed by generated projects.
No check/init placeholder, callback registry or Framework project runtime exists.

The group does not own creation inputs, templates, writes or their effect reports;
those responsibilities remain in the create subpackage. It does not import a
configuration loader, database capability or business declaration.
