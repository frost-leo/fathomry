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

// Package local supplies the local-file Provider adapter for framework
// configuration loading. Projects declare literal file locations and layers;
// the adapter owns acquisition and cleanup through the private Viper integration.
// It exposes no Viper instance, resource factory, watcher or native error graph.
//
// New validates and freezes declarations without I/O. The resulting Provider
// supports independent concurrent loads. Only explicitly optional missing files
// are skipped; malformed, inaccessible and oversized files are errors. Paths,
// file contents and process variable names are not diagnostics.
//
// Root resolves portable relative file names; it is not a filesystem sandbox.
// Symlinks follow normal OS behavior and selected files must remain stable during
// acquisition. Cancellation cannot force a blocked filesystem or parser to stop.
// No project discovery, environment expansion, directory scan or reload occurs.
package local
