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

# Local configuration Adapter

[Documentation](../../../../../README.md) / Public package reference

**Audience:** independent Go projects selecting explicit files.
**Status:** implemented and tested with stable regular files/atomic replacement on Linux.
**Package:** `github.com/frost-leo/fathomry/adapters/configsource/viper/v1`.

`Select(Settings)` accepts one non-secret source name and 1–16 uniquely named
`File` slots. Each file uses an explicit literal absolute `Path` (at most 4096
bytes) and `Encoding` of `yaml` or `json`. No CWD/HOME search, environment
expansion, implicit bindings or extension inference is performed. Bootstrap paths
and labels collectively fit the private integration's 64-KiB budget.

Settings/File are normal exact-tagged sensitive DTOs suitable inside project T.
Select validates and copies their data without stat/open. It returns the common
[Selection contract](../../v1/interface.md), not native Viper state.

Capture uses a narrow bounded raw seam in the existing private integration.
Original UTF-8 bytes survive even for present-empty/whitespace or invalid syntax.
Encoding is an admitted selection hint, not proof of native parsing.
No Viper AllSettings normalization, query map or ErrEmpty reconstruction is used.
The existing private parsed Load behavior is unchanged.

`ReconcileInterval` is in nanoseconds. Zero selects Capture-only; Observe requires
an explicit 1 second through 5 minutes. Observe owns initial acquisition and paced
periodic literal-path reads. All later attempts, including failures, wait at least
that interval after the preceding capture; there is no catch-up concurrency.
No fsnotify event driver, Viper WatchConfig or Framework poller is involved.

Subsequent reads observe delete/recreate, parent/target replacement and normal OS
symlink resolution. Coherent writer publication requires stable regular files or
qualified same-filesystem atomic replacement. In-place writes can expose valid
intermediate data; a read/tick is not writer completion. No symlink sandbox,
cross-file transaction, Windows or network-filesystem qualification is implied.
Blocked native stat/open/read/close cannot be forcibly interrupted.

Read/close errors retain source, known file-slot alias and public operation phase
through the common AcquisitionFailure contract; these are not native paths.
Read/close errors are Adapter conditions. Deliberate `errors.As` can expose the
first native `*os.PathError`; context cancellation/deadline and caller cancellation
causes retain intentional matching. These cause paths may disclose sensitive
selectors; ordinary diagnostics never print them. Parser/private fault graphs
are not exported. Positive OS absence becomes Missing, not a read error.

Static `Definition`, `Sources`, `Bindings`, `Module` functions participate in
layer catalogs without constructing a source. The public module namespace is
`fathomry.adapters.configsource.viper`; it is independent of instance names.

Executable controls are in the unrelated-module
[finite tests](../../../../../../framework/configuration/v1/testdata/consumer/local_test.go)
and [live/raw tests](../../../../../../framework/configuration/v1/testdata/consumer/local_live_test.go).
These test the actual public API, not only the historical private integration.
