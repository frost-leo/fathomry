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

// Package chromedp provides independently configured, controlled browser sessions
// using chromedp and CDP. It is an internal integration, not an HTTP Do client.
//
// Select prepares one named source without starting Chrome. Assemble owns its
// process (local mode) or connection (borrowed-browser mode); Bind combines shared
// admission with an independently owned evidence inbox. Run owns one isolated
// BrowserContext until disposal or explicit transfer to assembly cleanup.
//
// Native executor-only Actions and typed CDP commands retain their target-scoped
// expressiveness, without exposing chromedp contexts, allocators or browser handles.
// BrowserContext concurrency and retained output limits are not HTTP rate limits,
// complete network counts, a browser RSS limit, or an untrusted-code sandbox.
// These operations belong in process-local callers, never Temporal Workflow code.
package chromedp
