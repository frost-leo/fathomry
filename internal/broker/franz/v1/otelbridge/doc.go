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

// Package otelbridge translates explicit W3C propagation between Kafka's ordered
// byte headers and Fathomry's bounded OpenTelemetry context contract. It installs
// no SDK hook, logger, exporter or global provider, and owns no connection.
//
// Call Inject before producer submission or Extract after an exact/direct read.
// Both are explicit trust decisions; baggage is opt-in. Duplicate reserved names
// reject rather than silently changing context or losing ordinary Kafka headers.
package otelbridge
