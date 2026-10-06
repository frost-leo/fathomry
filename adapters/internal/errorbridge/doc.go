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

// Package errorbridge provides bounded, process-local error-boundary helpers
// shared by public adapters. Providers own native classifications and definitions;
// this package owns neither new error codes nor retry/effect policy or any runtime.
//
// Inspect walks immutable borrowed cause graphs once, preserving native priority
// and already-public subtrees. Forward hides wrapper text without replacing the
// public core or original graph; forwarded aggregates are authoritative too.
// Contains performs bounded identity checks without foreign matching callbacks.
// Consumers cannot import this private package.
package errorbridge
