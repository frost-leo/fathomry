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

// Package catalog owns workflow publication and catalog business contracts, not
// database creation, migrations, resource acquisition or Worker execution.
//
// Prepare consumes one verified
// declaration Bundle plus separately authored English and first-creation metadata.
// It validates owner references, limits and derived resource addresses and returns
// an immutable in-process snapshot. Names are required for the definition and each
// selected version-owned mode; omitted optional metadata is not a delete.
//
// Category declarations/references and tags remain distinct first-creation inputs.
// Prepared data does not enable workflows, assign defaults or claim database/schema
// readiness. PostgreSQL/MySQL construct an offline Catalog over a caller-owned
// Resource-backed Adapter client. Register admits current schema on the retained
// transaction before create/compare. Publication separates effect, acknowledgement
// and local cleanup; unknown commit is never automatically retried.
//
// Copies share operation ownership and calls refuse overlap. Retain the Catalog
// when cleanup is incomplete, Close with a fresh context and use LastPublication
// for the latest captured facts. Close never owns client/source shutdown. Callers
// reserve WorkBytes, prepared-input and provider evidence/copy capacity and drain
// provider evidence incrementally. List and Read use an actual read-only transaction
// and one anchor SELECT for mutable metadata/localization. Requested immutable
// documents are fetched by anchored identities/digests and verified through the
// declaration authority. Pages are bounded independent views, not a retained
// multi-page snapshot. Read cleanup never rewrites LastPublication.
// Compare uses the same read owner and one selected metadata anchor, validates
// stored canonical documents rather than trusting recorded fingerprints, and
// reports missing/different units without mutation or acknowledgement inference.
// No live client, pool, resource scope or runtime is created here.
//
// Authoring input is borrowed during Prepare; returned state retains no mutable
// caller-owned data. Declaration validation/canonical bytes use the existing
// declaration authority. Invalid input returns no partial prepared result.
// Ordinary formatting/logging is redacted; explicit input data can be sensitive.
package catalog
