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

// Package minio integrates the MinIO Go v7 SDK at the private object-storage
// boundary. Select, resource.WithLimits, resource.Assemble and Bind establish an
// explicitly configured, bucket/prefix-scoped source. Clients are concurrent
// non-owning capabilities; only the assembly releases their transport.
//
// Supported operations are bounded reads/downloads, stat, serial multipart and
// single uploads, single-source server-side copy, bounded object/version listing,
// incomplete-upload inspection/abort, per-target removal, and optional tags.
// Static V4 credentials, a fixed region, literal-IP endpoint, path addressing and
// explicit TLS roots (or explicitly allowed HTTP) exclude ambient discovery.
// Native retries, automatic multipart, ComposeObject, lazy Object handles,
// privileged bucket controls, file helpers, presigning and native/RDMA modes
// are not exposed. The package contract documents the remaining feature census.
//
// Accepted calls reserve independent invocation evidence before native work.
// Caller readers/writers are borrowed until receipt release, never closed here;
// their cooperation is required for cancellation. Network bodies are owned and
// joined, including close failures. Cancellation never proves a remote rollback.
// This is not a Run input, archive, manifest or business publication protocol.
package minio
