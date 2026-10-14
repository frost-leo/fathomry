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

// Package chromedp provides managed chromedp operations with retained contexts.
// Closed actions cover navigation, page-element interaction and rendered content
// extraction. Fallback selection and business retries belong to the caller.
// Prepare is offline. Open owns a source; Retain owns a context and its pages.
// Drain the independent evidence inbox. Close retained contexts before the source.
// Native Target, Browser and allocator handles never enter application callbacks.
// Typed subscriptions and Console retain bounded native event producers. Control
// leases pre-admit Fetch/auth/dialog/file-chooser and screencast replies with an
// independent terminal ledger. Object/value custody, origin storage and cache
// grants retain their original context/source. Trace is browser-owner-only.
// Fetch response bodies use their original pause, as bounded data or managed IO.
// Network request grants track native event order, redirects and source identity.
// Canceled body producers retain cleanup custody until positive native teardown.
// Storage partitions come from native frame keys, not reconstructed origin URLs.
// IndexedDB remote values retain object-group custody. Service-worker controls
// preserve observed identity and explicit context-wide native mutation semantics.
// KeepOpen explicitly transfers process/profile cleanup to a separate owner;
// source shutdown after transfer does not mean the kept process has exited.
// BindFunc reserves callback evidence before decoding and joins actual handler,
// serializer and reply work. Page-visible callback failures use fixed text.
// Cancellation requests shutdown; active iterator callbacks must actually return.
package chromedp
