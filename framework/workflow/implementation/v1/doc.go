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

// Package implementation binds verified workflow declarations to explicit Go
// Activity functions and projects bounded implementation coverage.
//
// Activity is inert: it retains a function and its types without calling user
// code. Prepare validates an opted-in workflow version and owns copied metadata;
// callable captures and receivers remain borrowed with caller-defined lifetime
// and concurrent-use semantics. A Registry is not a resource owner or executor.
// Concurrent metadata reads are supported; do not overwrite shared handles.
//
// Required value ports use the identified ordinary-JSON Go ABI. Matching validates
// static shapes, not future values, effects, retries, codecs or business behavior.
// No user-supplied serializer, factory, handler, Boot or service runs during preparation.
// Unsupported optional/batch/custom-codec shapes remain valid declaration features.
//
// Manifests are detached data, not executable authority. ValidateManifest checks
// them against the same verified declaration; decoding one cannot reconstruct
// callables. Formatting runtime handles hides content and JSON encoding refuses.
// Artifact, deployment and execution qualification remain separate boundaries.
package implementation
