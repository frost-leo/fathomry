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

# Framework common runtime composition

[Documentation](../../../README.md) / Public package reference

**Audience:** supported Framework scenarios and independent application assemblers.
**Status:** implemented public composition, required-evidence reception and safe
presentation/log binding. The separate
[configuration scenario](../configuration/v1/interface.md) supplies loading/Watch;
this package is not an application host or another configuration engine.
**Package:** `github.com/frost-leo/fathomry/framework/v1`.

## Compose existing public owners

New(ctx, Options) creates exactly one public operation Runtime and one public
resource Scope. Options composes their data-only ceilings; it neither selects SDKs
nor creates service clients. Operations and Resources expose those existing
mechanisms for explicit typed declarations. No DI registry, global settings
installation, source discovery or duplicate ownership engine is introduced.

Close seals/cancels both, joins actual operations before final resource cleanup,
and retains both error graphs. An expired wait leaves the same reachable Runtime;
retry Close. Completed native cleanup can retain errors. Borrowed sinks and
separately created evidence receivers are not silently owned or discarded.

The separately constructed configuration scenarios own their own operation runtime
and source. This Runtime is not their parent, shared admission limit or application
host. Passing Resources to a configuration dependency borrows an adoption scope;
it does not transfer configuration Watch ownership. Generated Boot uses the
configuration scenario API without this advanced assembly machinery.

## Receive required evidence without blocking behind live owners

StartReceiver[T] borrows an Inbox and a bounded, non-panicking sink. It uses
[Inbox.NextReleased](../../adapters/v1/interface.md), not a new custody queue.
Completed finite records can pass an earlier live Open/Watch record without
releasing that owner's evidence reservation. There is one sink invocation at a
time, not one worker per record.

Success acknowledges custody. Failure retains/requeues the same fact and retries
delivery after RetryDelay: zero=100ms, admitted 1ms..1min. A failed sink may already
have external effects, so it must tolerate redelivery according to its own evidence
identity. This is neither exactly-once delivery nor permission to retry SDK work.
Repeated failure eventually fills the Inbox and refuses new operations before
dispatch; no unbounded queue or adaptive retry controller is added.

Status reports saturating delivered/failure counts and the last historical failure.
Close cancels and joins this receiver's actual callback stack without sealing or
discarding the Inbox. If a sink ignores cancellation, timeout retains the owner.

After stopping producers, Finish seals new reservations and waits for accepted
records. Live/unreleased records remain owned. A separate consumer's outstanding
claim causes ErrPending even if this receiver has reached EOF. Finish does not
cancel producers or certify another sink's success.

## Bind presentation once

NewErrorLog borrows an explicit slog.Logger and Presenter. A nil sink deliberately
disables emission, not presentation. WithResource optionally selects a public
Ref[Presenter] per emission, using the explicit fallback if borrowing fails.
The lease lasts through presentation; the resulting presentation is frozen.

A Fixed Presenter instance with a supplied live settings.Reader still follows that
reader's accepted locale. Instance replacement policy and preference capture are
different mechanisms. No operation repeatedly fetches a global locale or catalog.

Emit returns the operational error/presentation, separate presentation/borrowing
Issue and a recognition flag. Codes and original causes remain inspectable.
Unknown native errors are retained but never passed to default formatting: the
log uses a static unclassified message. Registered failures use the existing safe
presentation projection. Caller-supplied projectors and slog handlers must be
bounded. An emission attempt does not prove durable logging; required evidence
has its separate reception path.

CoreComponents explicitly gathers failure/settings/i18n/resource/operation/assembly,
not concrete providers. Applications append only selected component bundles.
Assembly errors use the core capability facility 0x006, not a Framework-layer band.

## Executable contracts

- [Runtime timeout/cleanup ordering](../../../../framework/v1/runtime_test.go).
- [Receiver retry, blocked callback and other-consumer custody](../../../../framework/v1/receiver_test.go).
- [Fixed resource/live locale, offline atlas and unknown-error privacy](../../../../framework/v1/log_test.go).
- [Independent consumer with an actual public file observation and finite read](../../../../framework/v1/testdata/consumer/consumer_test.go).

Framework code imports public capabilities, not root Internal. These process-local
owners, watchers and logging callbacks do not belong in Workflow execution.
