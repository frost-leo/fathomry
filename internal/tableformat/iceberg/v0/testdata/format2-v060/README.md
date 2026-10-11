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

# Iceberg Go v0.6.0 format-2 fixture

This immutable synthetic fixture was produced by the unmodified official
Iceberg Go v0.6.0 (`350ae7270d7c44578638ef55e7e0392227745e8a`) and Arrow
v18.6.0 under Fathomry's preparation-baseline dependency graph. It was generated
during issue #137 implementation, not copied from historical service data.

The 65 rows have IDs 0 through 64. `value` is NULL when `id % 7 == 0`, otherwise
`historical-<id>`. Field IDs are 1 and 2. All metadata, manifest lists, manifests
and Parquet files are unchanged writer output; their URIs intentionally use the
synthetic S3 root `s3://fixture/owned/historical/`. `fixture.json` identifies
the selected metadata file.

`TestManagedReadsImmutableV060Format2Fixture` loads these bytes through the
managed REST/S3 boundary, verifies exact values/nulls/field IDs and rejects
implicit metadata mutation. This is a reader compatibility fixture, not a live
service test or a promise that v0.6.0 can read new format-3 features.

The generation source, pinned baseline mod/sum reference, build-info observation
and SHA-256 manifest are preserved in the issue #137 local reference packet under
`artifacts/implementation-20261011/format2-fixture/`. Do not regenerate this
fixture with the currently selected writer.
