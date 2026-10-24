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

# SQL endpoint policy interface

[Documentation](../../../../README.md) / Internal package reference

**Audience:** PostgreSQL/MySQL technical integration maintainers.
**Status:** private hostname admission and socket dial policy; not a connection pool.
**Package:** `github.com/frost-leo/fathomry/internal/database/endpoint`.

ValidHost admits non-unspecified, unzoned literal IPs. With explicit DNS permission,
it additionally admits ASCII letter/digit/hyphen labels of 1–63 bytes, no leading
or trailing hyphen, at most 253 bytes plus one terminal dot. It rejects URLs, ports,
paths, empty labels, Unicode names and unapproved resolver discovery. Use explicit
ASCII IDNA spelling when needed; this helper performs no Unicode normalization.

DialContext uses a per-call standard net.Dialer and Go-preferred Resolver. It does
not change process globals, cache addresses, select credentials or own a pool.
Deployment resolver/search-domain/hosts settings are authorized inputs when DNS is
enabled. The existing caller-owned setup deadline covers resolution and connection;
a socket's subsequent lifetime remains its caller's responsibility.
The dialer's socket control checks every numeric candidate and rejects unspecified
or zoned addresses before connect. Multiple candidate addresses may be attempted
by the standard dialer before success; this is not SQL retry or failover after
connection. No arbitrary callbacks or resolver objects are public input.

PostgreSQL's wire scope pins the selected peer for native cancellation. Once
connected, the original hostname is no longer a permitted cancellation destination,
so a DNS change cannot redirect its backend identifier/secret. MySQL retains its
same original physical socket and existing transport/session ownership.

[Tests](../../../../../internal/database/endpoint/endpoint_test.go) exercise strict
admission, controlled actual DNS responses, specified/unspecified targets and
canceled lookup. Provider DNS tests separately check authenticated TLS connections
and PostgreSQL connected-peer cancellation. These do not certify arbitrary cluster
routing, a real Kubernetes deployment or hard wall-clock cleanup completion.
