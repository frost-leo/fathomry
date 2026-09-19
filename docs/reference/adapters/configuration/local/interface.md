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

# Local configuration Provider adapter

[Documentation](../../../../README.md) / Public adapter reference

**Audience:** projects selecting local settings and adapter maintainers.
**Status:** implemented file-only Provider adapter; no remote source or reload.
**Package:** `github.com/frost-leo/fathomry/adapters/configuration/local`.

## Declaration and loading

`New(Options)` freezes the selected files without I/O and returns a concurrent
Provider. Pass it to the framework's [Load](../../../framework/configuration/interface.md).
The adapter uses the private Viper integration for bounded acquisition and hands
original bytes, not native normalized settings, to framework preparation.

Options and File are ordinary Go API structs, not Viper options or persistent
DTOs. Their evolution follows framework module compatibility and existing
consumer behavior, rather than independent type-version suffixes. SchemaVersion
declares the project's data format and defaults to 1. This declaration is separate
from the Go API, Viper v1.21.0 and the framework module version.

Root is an absolute, already-clean native path <=4096 bytes. It is not expanded,
discovered or checked for existence by the constructor. Requiring a clean root
avoids silently changing a path containing symlink/parent navigation. File paths
are explicit relative slash paths without dot/parent components or backslashes;
the joined native path is <=4096 bytes and must be local to the lexical root.
No scanning, globbing, home expansion or environment substitution occurs.

At most three files are permitted, with unique public names and distinct Base,
Environment or Local layers. Files are acquired in layer order. Encoding defaults
to yaml (also accepting JSON syntax); json explicitly selects native JSON parsing.
Names are public non-secret labels, not filenames. An empty file list explicitly
selects defaults/environment-only preparation and acquires nothing.

## Failure and resource boundaries

Each selected file must be stable and regular during acquisition. Root and child
symlinks follow ordinary OS semantics; lexical validation is not a filesystem
sandbox or proof of physical containment. Changing/replacing files concurrently
can invalidate the supported input conditions.

Optional permits only missing input. Permission denial, directories, malformed
content, oversized documents and other failures are not treated as absence.
Every failed acquisition returns zero Input, never a usable earlier prefix.
Every opened file is closed by the underlying finite acquisition before return.
There is no Provider.Close method, retained reader/client, watcher or background
cleanup worker.

Context cancellation is checked between synchronous phases and reads; it cannot
force a blocked filesystem operation or parser to stop. Native paths and parser
text are withheld. Public failure diagnostics name only the adapter and the
declared source; safe fs.ErrNotExist/fs.ErrPermission and caller cancellation
causes remain inspectable. These are facts, not automatic retry instructions.

Per-document limits are 1 MiB, and source count limits aggregate retained document
input to at most 3 MiB. The underlying reader checks an additional witness byte
rather than treating truncation as success. Parser, copying, Go/native memory and
caller-retained buffers are not a hard process-memory ceiling.

## Project ownership and compatibility

The project declares locations and business schema meaning. Selecting this public
adapter is not writing SDK/resource assembly: it requires no native client,
Viper object, resource factory, lease or evidence inbox. The framework owns
preparation and the adapter owns its technical reads.

The supplied declared schema version is not inferred from file bytes and is not
an authenticity check. Changing a business schema requires an explicit matching
declaration or conversion; library input-contract evolution does not silently
change the old default or parsing behavior. Unknown layers/encodings are
refused, not interpreted as newly supported options.

Options, files and Provider handles use restricted formatting and refuse JSON
persistence/reconstruction. Original returned documents intentionally contain
private data; the ordinary application path uses the framework's prepared value
and safe description.

[Native file tests](../../../../../adapters/configuration/local/local_test.go)
cover selection, copies, optional/malformed/oversized sources, raw byte fidelity,
cancellation and concurrent public loads.
[Linux tests](../../../../../adapters/configuration/local/local_linux_test.go)
cover permission refusal and nonregular-file rejection. These are local
filesystem checks, not remote-service or hostile-filesystem qualification.
