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

// Package nacos exposes the supported Nacos configuration profile through public
// operation/evidence and resource-ownership contracts. Validate admits loadable
// Settings without I/O. Open returns an Owner, not readiness. Owner.Client supplies
// direct operations; Using binds a resource.Ref[Handle] without exposing native
// ownership or Close authority through borrowed resource values.
//
// Reads, invalidations, complete raw observations, Publish/CAS/Delete and bounded
// native v1/v3 Search remain independently usable, without Framework. No native
// cache fallback, extra retry/refetch loop, naming service or plugin is added.
//
// Owner.Close and subscription Close retain responsibility after wait expiry.
// Resource-backed subscriptions retain their selected generation until actual
// cleanup. Evidence admission precedes dispatch and its redelivery never repeats
// mutations. InspectError deliberately exposes native evidence; ordinary formatting
// never prints server text or configuration payloads. No operation chooses locale,
// process-default settings or a global instance. This is not Workflow code.
package nacos
