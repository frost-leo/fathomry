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

// Package viper integrates the Viper v1 SDK with bounded, isolated reads and live
// queries, captured decoding and owned file observation. It does not assign
// application layers or validate business settings. Callers select inputs
// explicitly; no discovery, merge, writeback,
// SDK handle, managed resource, or process-global configuration is exposed.
//
// # Explicit acquisition and preparation
//
// [Load] validates the complete [LoadInput] batch and creates one private Viper
// instance per input. Each input supplies versioned [OptionsV1] and exactly one
// absolute file path or borrowed reader. Files opened by Load are closed before
// return; caller readers are never closed. Failure returns no usable new prefix.
// [RawFile] reuses bounded owned file reading without requiring successful native
// syntax parsing; it preserves UTF-8 present-empty bytes and positive OS absence.
//
// [Document.RawCopy] preserves original bytes for the existing resource preparation
// boundary. Composition chooses the authorized layer identities and precedence;
// rebuilding a document from native normalized values would lose original casing,
// numeric representation, null semantics or other preparation evidence.
//
// # Queries and lifetime
//
// [Document.ValueCopy] preserves the selected native query semantics and copies
// returned collections. Explicit bindings and opt-in AutomaticEnv are read live,
// not frozen during Load. A native query is therefore neither a preparation result
// nor a common-time snapshot across sources and environment values. [Document.Capture]
// freezes selected native values; [Decode] additionally discovers struct field keys
// so environment-only fields reach native Unmarshal. Decode uses native weak
// conversions, not resource.Prepare's strict YAML/JSON contract. TOML retains native
// scalar/date types; dotenv/env parsing never populates the process environment
// and refuses variable expansion while preserving literal/escaped dollars.
//
// [Watch] periodically reconciles explicitly selected file contents. Callers own
// [Subscription.Close], and reload/validate on [Change] invalidation or resync.
// It does not observe environment changes or promise every intermediate file write.
//
// Input, structure and copy bounds are enforced by this package; cancellation is
// checked between synchronous phases and cannot force an arbitrary reader, file
// operation or parser to terminate. Load/Decode create no persistent worker;
// a Watch Close timeout retains responsibility for its one worker. Runtime formatting
// is restricted; explicit raw/native queries
// deliberately expose data and are not diagnostic projections.
package viper
