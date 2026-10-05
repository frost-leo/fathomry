/**
 * fathomry
 * Copyright (C) 2026  Frost Leo
 * SPDX-License-Identifier: GPL-3.0-or-later
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

// Package franz provides a bounded franz-go integration for process-local
// Activity I/O: acknowledged production, bounded Kafka-only atomic batches and
// exact historical record reads. It is not a public broker interface or the
// framework's data/reference, Item/Run or recovery protocol.
//
// Composition calls Select, resource.Assemble and Bind with an independently
// owned invocation.Inbox. Source owns native clients; Client cannot close or
// reconfigure them. Required evidence is independent of receipt waiting and
// optional lossy observations. Assembly.Close retains incomplete cleanup.
//
// Explicit endpoints/topics, all-ISR idempotent writes, manual/keyed routing and
// read-committed bounded decoding support none/gzip/snappy/lz4/zstd. Static SASL
// requires verified TLS. Consumer retains a direct cursor; OffsetGroup stores
// non-member checkpoints. ConsumeGroup separately owns classic cooperative-sticky
// membership and whole-page commit tokens. Native access, group EOS and
// administration are absent. See the package interface in
// docs/reference/internal/broker/franz/v1/interface.md.
package franz
