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

# Typed results and independent evidence

[Documentation](../../../README.md) / Internal package reference

**Audience:** invocation integration authors.
**Status:** implemented local contract; native guarantees remain integration-specific.
**Package:** `github.com/frost-leo/fathomry/internal/invocation`.

Start with the [invocation interface](interface.md). This is one part of the same
accepted-call responsibility, not another resource owner.

## Typed results and necessary evidence

`Outcome[T]` preserves typed partial data together with separate Primary and
Cleanup errors. `Present=true` distinguishes explicit empty output from absent
data. The capability owns T's meanings: accepted/committed/visible/unknown effects,
per-Item versus aggregate granularity, native references and completeness. The
shared mechanism never expands an aggregate ACK into per-Item success.

Transferred T must be immutable and bounded. Result metadata is copied; generic
values and original native errors are **borrowed**, not automatically deep-copied.
Do not mutate a map/slice behind a transferred result or call its projection a
deep snapshot. Returning a copy of the outer struct does not clone its payload.
Providers must define these ownership/concurrency rules and budget native causes.

Errors use private `fault`, retain `errors.Is/As`, and add actual source/technical
context without printing native causes. `Result.Err` aggregates inspectable
primary/cleanup failures. Nil means no reported error, not proof of an effect,
complete empty output or business success. Retry, toleration, reconciliation,
localization and Item/Run disposition remain separate owners. Standard Go `error`
wrapping can retain every original cause at a meaningful future boundary. The
fixtures use local sentinels with `errors.Join`/`fmt.Errorf`, not a replacement
public error system. Frozen higher-level attribution stays in their typed evidence
envelope. Handling an error does not discharge independent evidence responsibility.

`Inbox[T]` is the framework's independent process-local evidence boundary:

- A positive count and declared byte capacity are reserved **before** SDK work.
  Full capacity rejects admission. A rejected Begin returns an error but no accepted
  operation receipt; no SDK work was entered through that call.
- `Next` receives an accepted operation's live receipt, potentially before results.
  Pending, queued, received-but-unreleased and finished deliveries remain charged.
- Complete/Resolve/Finish never invoke receiver code or await queue space. A serial
  SDK promise cannot become stuck behind an exporter or evidence callback.
  [franz-go promise contract](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Client.Produce).
- `DeliveryRecord.Release` refuses while local use remains, then idempotently
  relinquishes the slot. The framework must handle necessary facts first. Release
  itself performs no durable write and is not a storage acknowledgement.
- Reading a call receipt cannot release the independent delivery. Handling an
  error does not remove evidence. A recording failure leaves capacity occupied.
- Long sessions drain bounded child results incrementally, retaining only their
  current aggregate/session responsibility. Receivers must not serialize an endless
  session wait ahead of every child delivery. The library owns no receiver workers.

There is no shared unbounded completed-call history. Inbox backlog is explicit and
bounded; receipts retained after local release are caller-owned memory. A lost
completion or missing cleanup keeps a finite reservation occupied rather than
manufacturing success. Resource/evidence ownership must be handed to an accountable
receiver before an adapter abandons its caller wait.

This is an in-process handoff, not the full durable evidence protocol. Process
crashes, authoritative storage acknowledgement, duplicate/late evidence across
processes, archival and advancement after persistence failure remain framework
integration obligations. Do not use this queue alone to claim reliable recovery.

## Executable evidence

[Call tests](../../../../internal/invocation/calls_test.go) and [evidence tests](../../../../internal/invocation/evidence_test.go) exercise the contract. See [SDK integration](../../../development/sdk-integration.md) and [testing](../../../development/testing.md).
