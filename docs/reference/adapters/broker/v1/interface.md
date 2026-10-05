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

# Broker contracts

[Documentation](../../../../README.md) / Public Adapter reference

**Audience:** application and concrete broker-Adapter authors.
**Status:** implemented public data contracts; no universal broker interface.
**Package:** `github.com/frost-leo/fathomry/adapters/broker/v1`.

This category package holds `Budget`, `Policy`, `Info`, `Attribution` and
`Attempts`. It depends only on public operation contracts and the standard
library. It does not construct clients, wrap Internal, select a provider or own
shutdown. Kafka-specific types remain in the [Kafka Adapter](../kafka/v1/interface.md).

A concrete provider validates settings and derives a policy. The caller creates
its operation Runtime and evidence Inbox explicitly. Source-instance allowances
include overlapping Fixed/Follow generations; another resource holder does not
create more budget. Reservations describe ownership, not measured RSS.

Source configuration revisions, public operation sequence/correlation and actual
borrowed generation are different identities. Inexact zero attempts is not proof
of no request or effect. Runtime metadata is deliberately inspectable but
redacted in ordinary formatting/logging and refuses implicit JSON serialization.

See the Kafka [policy tests](../../../../../adapters/broker/kafka/v1/option_test.go)
and [public-only executable](../../../../../adapters/broker/kafka/v1/testdata/consumer/main.go).
