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

# Browser contracts

[Documentation](../../../../README.md) / Public Adapter reference

**Audience:** Browser Provider authors and explicit composition callers.
**Status:** implemented shared budget, observation and browser-host path vocabulary.
**Package:** `github.com/frost-leo/fathomry/adapters/browser/v1`.

This category contains no browser engine, universal client, selector implementation
or lifecycle runtime. Concrete providers use `adapters/v1` and `resource/v1`.

`Budget` separates retained context work, source work and independent evidence.
`Policy` includes the source once. Sum policies for overlapping source generations.
These logical budgets are not browser RSS, transport allocation or network limits.

`Observation` keeps return, external-effect uncertainty, context disposal,
borrowed-use detach, actual process exit and responsibility transfer separate.
A load event does not prove HTTP success. A canceled call does not disprove effects.
`Attribution` names the actual retained public generation, not the latest Follow value.

`HostPath` explicitly names a path on the browser's host. It is not evidence that
the Go caller can read that path, and does not implement remote file transfer.

See the [chromedp contract](../chromedp/v1/interface.md) for implemented routes.
