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

# Root-entry configuration consumer

This hand-authored fixture exercises the API slice of #167. It is not generated
output and does not qualify the new template or published-module distribution.

The parent test copies the project to an independent temporary module, points it
at the candidate through an explicit local replacement, and checks root execution,
project-owned schema behavior, readonly builds and a relocated standalone binary.
The actual generator remains unchanged until its new API dependency is published.

The root entry supplies signals/streams, internal/bootstrap supplies project
declarations, internal/setting owns pure data/validation, and the Fathomry command
implementation uses the existing Resource scope and CLI engine. No proxy, private
framework import or copied resource/evidence loop is included.
