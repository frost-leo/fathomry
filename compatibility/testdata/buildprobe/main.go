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

// This executable is a test fixture. Its deliberately selected JSON projection is
// test transport, not a public compatibility serialization or deployment protocol.
package main

import (
	"encoding/json"
	"os"

	"github.com/frost-leo/fathomry/compatibility"
)

var nativeAttempts int

type moduleProjection struct {
	Path, Version, Sum, Replacement, ReplacementPath, ReplacementVersion, ReplacementSum string
	VersionKind, Revision, Modified                                                      string
	Present, Main                                                                        bool
}

func project(module compatibility.Module) moduleProjection {
	return moduleProjection{
		Path: module.Path.Value, Version: module.Version.Value, Sum: module.Sum.Value, Present: module.Present, Main: module.Main,
		VersionKind: string(module.Version.Kind), Replacement: string(module.Replacement.Kind),
		ReplacementPath: module.Replacement.Path.Value, ReplacementVersion: module.Replacement.Version.Value,
		ReplacementSum: module.Replacement.Sum.Value, Revision: module.VCS.Revision.Value, Modified: module.VCS.Modified.Value,
	}
}

func main() {
	build, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: os.Args[1:], DisclosePaths: []string{"example.org/consumer"}})
	if err != nil {
		panic("invalid fixture build request")
	}
	modules := make([]moduleProjection, len(build.SDKs))
	for index, module := range build.SDKs {
		modules[index] = project(module)
	}
	_, serialization := json.Marshal(build)
	output := struct {
		Go                   string
		Main, Framework      moduleProjection
		SDKs                 []moduleProjection
		Attempts             int
		SerializationRefused bool
	}{build.Go.Value, project(build.Main), project(build.Framework), modules, nativeAttempts, serialization != nil}
	if err := json.NewEncoder(os.Stdout).Encode(output); err != nil {
		panic("fixture output failed")
	}
}
