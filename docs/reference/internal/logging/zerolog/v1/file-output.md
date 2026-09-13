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

# Zerolog local file output

[Documentation](../../../../../README.md) / [Zerolog interface](interface.md)

**Audience:** composition and operational maintainers selecting local output.
**Status:** implemented bounded synchronous filesystem profile; no automatic
crash repair or power-loss certification.

## Directory and ownership contract

A file sink selects an **existing private dedicated directory** by absolute,
clean path, at most 4096 UTF-8 bytes. It creates no directories and applies no
ambient home/current-directory discovery. The directory and existing managed
regular files must have no group/other permissions. New files use mode 0600.
No other process or external tool may modify the directory while it is owned.

An `os.Root` anchors operations. Exclusive creation of `.fathomry.lock` refuses
another cooperating owner, including aliases of the same directory. The marker
is not automatically stolen, expired or cleared based on a PID/time heuristic.
Existing active/archive symlinks, unrelated entries, temporary artifacts,
incompatible compression suffixes, excess backups and over-bound files cause
construction refusal, not deletion. Existing active content must end in a newline.
This is structural admission, not an integrity audit of historical archives.

The directory is a trusted deployment boundary, not a sandbox for adversarial
filesystem mutation or hard-link insertion. Use a local filesystem with regular
files and exclusive hard-link publication support. Other filesystems/platforms
are unverified and can refuse operations.

## Selected policy and bounds

| Option | Meaning |
| --- | --- |
| `MaxBytes` | 8 MiB default; at least the effective `MaxRecordBytes`, at most 64 MiB |
| `Backups` | 4 default; 1–128 retained archives |
| `Compress` | false default; true selects synchronous gzip BestSpeed |

`current.jsonl` holds active JSON lines. Rotation occurs **before** an entire
record would exceed `MaxBytes`, or on explicit `Rotate`. An exactly full file is
valid. Empty files are not archived. No interval/daily/age policy is selected.

Archives use monotonic sequence names `log-00000000000000000001.jsonl`, optionally
with `.gz`. Restart derives the next sequence from retained archives. Sequence
exhaustion is an explicit failure, not wraparound. No timestamp precision or
sleep is used to avoid collisions.

Retention deletes the oldest known archive only after publishing a complete new
archive and opening a replacement active file. Count, not elapsed age, is the
retention policy. Clean restart with the same supported policy preserves existing
active content and continues numbering. Changing compression or lowering limits
can require explicit migration; existing evidence is not silently reinterpreted.

An active/uncompressed archive is at most `MaxBytes`. A compressed destination
is explicitly capped at `2 * MaxBytes + 64 KiB`. At a transition/failure there
may be one extra archive and one temporary file. A conservative managed disk bound
is `MaxBytes + (Backups + 2) * archiveLimit` plus directory/lock/filesystem
overhead, where `archiveLimit` is `MaxBytes` without gzip and the compressed cap
with gzip. External files, filesystem allocation overhead and caller modifications
are outside that envelope. Directory admission reads at most 132 entries before
refusal. There is no unbounded directory walk.

## Publication and failure evidence

Uncompressed rotation creates a non-replacing hard link before removing the
active name. Gzip streams through a bounded context-checked writer into an
exclusively created temporary file. Compression finalization, input close, output
sync and output close are checked before exclusive publication under the archive
name. An existing destination is never overwritten. Temporary cleanup failures
are retained; an unowned obstruction is never removed.

There is no maintenance worker or global callback. Write, compression, sync,
publication, retention and cleanup errors remain inspectable through the owning
call or assembly report, without raw stderr diagnostics. Compression cost is on
the synchronous caller; subsequent sinks wait for it. Context is checked before
each sink and between gzip reads/writes. Once a short filesystem transition or
file close begins, it finishes synchronously; its individual syscalls are not
forcibly timed out. No check implies that kernel I/O can be interrupted.

A failure stops further file output. Original data and any partial archive effects
are preserved for reconciliation; no retry, overwrite, record truncation or
automatic deletion is used to manufacture success. Other eligible sinks continue
and retain their own evidence. A failed maintenance operation can leave both
active and archive names, or an empty new active file, without accepting the
current log record.

Successful `Log` means the active file accepted the bytes, not that they survived
power loss. `Sync` acts on active files; close also checks their sync/close.
Directory metadata is not fsynced as a crash-consistency protocol. No atomicity
across sinks, transaction, post-crash exactly-once delivery or durable execution
history follows from these calls.

## Close and explicit recovery

Normal close seals access through the shared resource mechanism, closes all owned
handles, and removes the lock marker. All selected files are attempted even if
an earlier close reports an error. Borrowed byte/structured handles remain
caller-owned.

A failed file writer or unsuccessful sync/close leaves the lock name as
**recovery evidence**, while closing its local handles. The assembly may report
`Quiescent` and `Released` with an error: those flags confirm local resource use
ended, not successful disk cleanup or absence of residual artifacts. A crash also
leaves the marker. Subsequent construction refuses it.

An operator must first establish that no live owner exists, inspect preserved
active/archive/temporary files and the recorded failure, and decide how to retain
or reconcile evidence before removing the marker. There is intentionally no
automatic unlock/recovery API. Never remove a marker just because a deadline
expired. If cleanup itself is waiting on kernel I/O, the operation remains owned.

The existing resource manager retains historical cleanup causes even after a
later close finishes. Composition still budgets explicit cleanup continuations
and retained reports; file output adds no independent retry-history mechanism.

## Selection rationale and evidence

Native companion probes demonstrated small successful rotations but also archive
overwrite, unjoined maintenance or missing maintenance errors. This implementation
therefore uses standard-library file/gzip operations in the zerolog-specific
package rather than certifying a candidate with those counterexamples. It does
not select a Zap policy or establish a reusable cross-logger rotation API.

[File tests](../../../../../../internal/logging/zerolog/v1/file_test.go) separately
verify successful JSON/gzip read-back, bounded retention and restart, six
same-timestamp rotations, existing-destination refusal, compression/retention
failure, post-close refusal, ownership collisions, partial construction and
continued cleanup after an earlier error. These are integration acceptance
assertions, not probes whose PASS merely confirms a native defect.

Maintained regressions also exercise cancellation after gzip begins, interrupted
multi-file cleanup with a continuation, and Linux
[real regular-file short writes](../../../../../../internal/logging/zerolog/v1/file_linux_test.go)
using a child-process-only file-size limit. Partial bytes, original causes,
healthy secondary output and recovery markers have independent assertions.

See [the package contract](interface.md) and
[issue #31](https://github.com/frost-leo/fathomry/issues/31) for scope, SDK evidence
and exclusions. No production filesystem, power-cut or broad platform qualification
is claimed by these local tests.
