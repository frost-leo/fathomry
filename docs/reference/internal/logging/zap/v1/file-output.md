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

# Zap local file output

[Documentation](../../../../../README.md) / [Zap interface](interface.md)

**Audience:** composition maintainers and local-output operators.
**Status:** implemented Linux local-filesystem profile; not a crash-recovery ledger.

## Ownership and supported filesystems

Each file destination receives one existing absolute directory. The package
creates no directories and reads no ambient file-output configuration.
The directory and its existing files must belong to the effective user and have
no group/other permission bits. New files use creation mode `0600`, subject to
the process umask; keep owner read/write permissions available. Prepare directories
with `0700`. Do not mix unrelated files into them.

A directory-descriptor `flock` is held until owned descriptors close. It excludes
cooperating owners, including separate sources, processes and pathname aliases.
All file operations are relative to that pinned descriptor. Final symlinks and
existing hard-linked/nonregular files are rejected. Archive publication uses
Linux `renameat2(RENAME_NOREPLACE)`, never replacement rename.

Only local filesystems supporting those operations and file/directory Sync are
within this profile. Remote filesystems, competing external rotators, hostile
same-user mutation, directory replacement and unsupported OSes are not supported.
Locks are advisory, not a security sandbox. The directory and namespace must
remain exclusively managed by this sink for its lifetime.

## Rotation, compression and retention

- `current.log` is append-only between rotations. A record is never split between
  active and archive files. Rotation occurs before a write would exceed the
  configured `MaxFileBytes`; exact fit does not rotate.
- Archives use monotonically increasing 20-digit sequence numbers, recovered
  from the existing archive names on clean startup. No timestamp decides names,
  so rapid rotations or wall-clock changes cannot overwrite a backup.
- The active file is synchronized and closed, then renamed without replacement
  to `archive-<sequence>.log`. A conflict is an error, not a reason to overwrite.
- With compression enabled, gzip BestSpeed writes a new exclusive
  `archive-<sequence>.log.gz.tmp`. Compression, file Sync and descriptor Close must
  succeed before no-replace publication as `.log.gz`. The original raw file is
  removed only after successful publication and directory Sync.
- Only after successful archive creation are the oldest archives beyond
  `MaxBackups` removed. The next active file is created exclusively.
- Directory namespace changes are synchronized. There is no maintenance worker,
  wall-clock scheduler, background queue or compression continuing after Close.

Size is in bytes, not rounded megabytes. Bootstrap defaults are 10 MiB per file,
five backups and compression off. Supported sizes are the selected
`MaxEntryBytes` through 64 MiB, with 1–64 backups. Compression is an explicit
per-output boolean. There is no time trigger, age retention, zstd or automatic
policy migration. Different directories can select different policies.

## Space and memory accounting

Let `M = MaxFileBytes`, `N = MaxBackups` and
`C = M + floor(M / 100) + 65536`. Both compression input and output are bounded;
the compressed file cannot exceed `C` even for incompressible data.

Without compression, managed logical file bytes are bounded by `(N + 1) * M`.
With compression, steady state is bounded by `N * C + M` and maintenance by
`(N + 1) * C + M`. These conservative limits include the temporary gzip output
and original raw archive. A failed maintenance cycle stops further file writes,
rather than starting another accumulation cycle.

These are logical-content ceilings under exclusive ownership, not free-space
reservation, filesystem allocation/metadata bounds, disk quotas or filesystem
snapshot accounting. Composition adds the envelopes of its selected directories
and must provision free space and external retention accordingly.

Compression streams through bounded input/output with a 32 KiB copy buffer;
it never reads a whole maximum-size file into memory. Native gzip/JSON buffers,
allocator behavior and retained error graphs are not an RSS guarantee. Gzip is
synchronous and can increase log-call and shutdown latency. No hard wall-clock
bound is claimed for filesystem operations.

## Failure, Close and restart

Any native write, rotation, gzip, publication, retention or file Sync failure
seals that file sink. The actual failure is retained in the operation's branch
result and later Close. Other returning sinks can still receive the event.
There is no automatic retry or fallback destination.

A short/failed write may have emitted a prefix. A canceled caller may have
already written a complete record. Neither case proves no effect. Failed
compression keeps the original raw archive and any partial temporary output;
it does not delete the evidence or claim successful maintenance.

Close is once-only and seals before cleanup. It synchronizes/closes owned
descriptors, releases the directory lock and retains earlier errors. Repeated
Close cannot reopen a file, append beyond rotation limits or convert an earlier
failure into success. It does not roll back created files or accepted records.

Clean restart validates a bounded directory listing, names, counts, permissions,
sizes and the active file's final newline before opening append output.
Unknown files, unfinished `.tmp` or raw gzip-input archives, excess archives,
invalid sequences, oversized files, links and a truncated active record cause
initialization failure without repair or deletion. Changed policies that reject
existing files fail rather than silently reinterpreting them.

After interrupted maintenance or failure, an operator must preserve and inspect
the affected files, recover/archive them outside the managed directory as
appropriate, and restore a valid directory before restarting. Do not blindly
delete leftovers or replay a failed event: earlier destinations may have accepted
it. Automatic crash repair, power-loss certification, full existing-archive
integrity validation and durable execution evidence are not supplied.

## Why not simply adopt a rotation companion?

The issue's pinned companion observations include Lumberjack maintenance that
outlives Close and suppresses errors, and Timberjack archive collisions and
maintenance errors invisible to Write/Close. A positive gzip example does not
negate those counterexamples. No candidate was treated as unconditionally safe.

This package instead uses a small synchronous, fail-closed file owner with
standard gzip and existing Linux primitives. It is Zap-specific, not a shared
rotation framework or a third logging engine. The cost is explicit platform,
blocking and manual-recovery restrictions, rather than a claim that an upstream
writer's Close proves more than it actually does.

The [native file tests](../../../../../../internal/logging/zap/v1/file_linux_test.go)
verify retention/gzip read-back, rapid rotation preservation, no-clobber conflicts,
exclusive ownership, partial construction, unsafe-restart refusal and an actual
gzip output failure under a child-process file-size limit. The latter must return
the native error, preserve raw data and retain the error across Close; merely
reproducing silent failure would fail this acceptance test.
