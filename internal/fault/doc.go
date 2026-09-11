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

// Package fault preserves technical error kinds, bounded context and original
// causes for the internal foundation. It has no framework attribution, public
// error dependency, retry policy, registry, localization or wire protocol.
//
// Declare code-owned [Kind] values and construct occurrences with [Kind.New].
// [Context] supplies technical location and optional [Correlation]; validation
// bounds their representation, not their authenticity or safety as metric labels.
// Callers must keep credentials, payloads and business identity out of these labels.
//
// [Error] copies context and cause-slice storage while retaining the original
// native error objects. Standard errors.Is and errors.As can inspect those causes;
// their lifetime and concurrent-use guarantees remain those of their owners.
// [Error.Diagnostic] and ordinary formatting omit native error text and graphs.
// Deliberately traversing causes is not a redacted diagnostic operation.
//
// A kind identifies a technical classification, not retryability, remote effects,
// successful cleanup or a business outcome. Invalid kind/context input becomes
// [Invalid] without discarding the supplied causes. These in-process errors have
// no approved durable JSON or Temporal failure representation.
package fault
