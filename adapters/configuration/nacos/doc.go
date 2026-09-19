/*
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

// Package nacos supplies finite remote configuration acquisition through the
// existing internal Nacos integration. New freezes explicit bootstrap and source
// declarations; pass the resulting Provider to framework/configuration.Load.
//
// The adapter owns authentication, transient sessions and synchronous cleanup.
// Loading starts no watch, cache, failover file or background resource manager.
// The common framework owns environment capture, strict interpretation and
// immutable settings; business projects never receive native clients.
//
// Request deadlines are cooperative, not hard cleanup deadlines. Outstanding
// native DNS or caller trace hooks are joined before return. Plaintext requires
// explicit opt-in; secure operation verifies both HTTPS and gRPC TLS. No endpoint,
// key, namespace or credential discovery, management writes or service readiness
// claim is implied. Results are not atomic multi-key/multi-server snapshots.
package nacos
