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
// queries. It does not assign application layers or prepare business settings.
// Callers select static inputs explicitly; no discovery, merge, watcher, writeback,
// SDK handle, managed resource, or process-global configuration is exposed.
//
// # Explicit acquisition and preparation
//
// [Load] validates the complete [LoadInput] batch and creates one private Viper
// instance per input. Each input supplies versioned [OptionsV1] and exactly one
// absolute file path or borrowed reader. Files opened by Load are closed before
// return; caller readers are never closed. Failure returns no usable new prefix.
//
// [Document.RawCopy] preserves original bytes for the existing resource preparation
// boundary. Composition chooses the authorized layer identities and precedence;
// rebuilding a document from native normalized values would lose original casing,
// numeric representation, null semantics or other preparation evidence.
//
// # Queries and lifetime
//
// [Document.ValueCopy] preserves the selected native query semantics and copies
// returned collections. Explicit environment bindings are read live at query time,
// not frozen during Load. A native query is therefore neither a preparation result
// nor a common-time snapshot across sources and environment values.
//
// Input, structure and copy bounds are enforced by this package; cancellation is
// checked between synchronous phases and cannot force an arbitrary reader, file
// operation or parser to terminate. No persistent worker or Close operation is
// introduced. Runtime formatting is restricted; explicit raw/native queries
// deliberately expose data and are not diagnostic projections.
package viper
