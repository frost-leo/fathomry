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

// Package dotenv reads an explicitly selected dotenv file into isolated values.
// ReadFile never discovers files, changes the process environment, evaluates
// shell syntax, expands variables or opens a remote connection. Values.Lookup
// reads only the frozen file; LookupWithProcess gives explicitly present process
// values precedence, including empty values, without enumerating the environment.
//
// Only UTF-8 KEY=VALUE assignments, optional export, comments and single-line
// quoted values are supported. Single quotes are literal; double quotes use Go
// string escapes. Dollar signs are literal in every form. Multiline values use
// double-quoted escapes, not multiline source syntax. Duplicate names, malformed
// lines and byte/entry limit violations reject the whole file, even when unused.
//
// ReadFile requires a stable regular file at a literal absolute clean path.
// Symlinks follow ordinary OS behavior, allowing explicitly selected secret
// mounts. Paths and native diagnostics are withheld. Cancellation is cooperative;
// arbitrary blocked filesystem calls cannot be forcibly interrupted. There is
// no watcher, goroutine, persistent handle or common-time file/process snapshot.
package dotenv
