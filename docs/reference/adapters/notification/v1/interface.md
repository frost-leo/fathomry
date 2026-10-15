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

# Notification contracts

[Documentation](../../../../README.md) / Public package reference

**Audience:** notification consumers and provider maintainers.

**Status:** implemented SDK-independent data contracts.

**Package:** `github.com/frost-leo/fathomry/adapters/notification/v1`.

## Responsibilities

`Budget` and `Policy` describe declared operation/evidence and resident-source
envelopes. `Info` contains frozen source provenance; `Attribution` identifies
the operation and actual borrowed generation. `Info.Clone` copies provenance.
A zero generation also covers direct use or failed acquisition; it does not
prove that no source exists.
Budget.AdmissionTimeout is the declared outer admission wait bound, in nanoseconds;
it does not forcibly terminate a callback or replace provider execution budgets.

Observation names distinguish SMTP relay acceptance, platform acknowledgement,
callback authentication, acknowledgement writing, local process success and an
explicit recipient-read observation. Providers report only facts they actually
observe. Absence is not a negative guarantee; none of these names implies a
Run/Item business terminal state. Mail never reports recipient reading.

This package contains no SDK, client, universal Notifier, Report, registry,
scheduler or retry engine. Concrete adapters reuse [operations](../../v1/interface.md)
and [resource generations](../../../resource/v1/interface.md). The
[Mail adapter](../mail/v1/interface.md) is one consumer; choosing it does not
import Lark.

## Bounds and ownership

Byte figures are declared local envelopes, not measured RSS, kernel memory or
cross-worker quotas. Include every overlapping source, including retired
generations until actual cleanup. An adapter's native-aware preparation is the
authority for its recommendation; this category supplies no default formulas.

Metadata is process-local and intentionally refuses JSON serialization.
Normal formatting is redacted; deliberate field access can reveal private
attribution. No error facility is allocated to this data-only package.

See the [source](../../../../../adapters/notification/v1/doc.go) and
[copy/distinction tests](../../../../../adapters/notification/v1/metadata_test.go).
