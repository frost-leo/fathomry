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

package trino

import (
	"github.com/frost-leo/fathomry/adapters/sqlengine/v1"
	"github.com/frost-leo/fathomry/internal/compatibility"
)

// ModuleInfo reports only selected SDK build facts, never local replacement paths.
type ModuleInfo struct {
	private
	Present                                             bool
	Path, Version, Sum                                  sqlengine.Fact
	ReplacementKind                                     string
	ReplacementPath, ReplacementVersion, ReplacementSum sqlengine.Fact
}

// BuildInfo separates executing-binary facts from coordinator/connector evidence.
type BuildInfo struct {
	private
	Metadata string
	Go       sqlengine.Fact
	SDKs     []ModuleInfo
	Platform []sqlengine.Option
}

// Build performs no readiness I/O and never inventories arbitrary dependencies.
func Build() (BuildInfo, error) {
	observed, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{"github.com/trinodb/trino-go-client"}})
	if err != nil {
		return BuildInfo{}, translate(err, "build")
	}
	fact := func(value compatibility.Fact) sqlengine.Fact {
		return sqlengine.Fact{Kind: string(value.Kind), Value: value.Value}
	}
	result := BuildInfo{Metadata: string(observed.Metadata), Go: fact(observed.Go)}
	for _, module := range observed.SDKs {
		result.SDKs = append(result.SDKs, ModuleInfo{Present: module.Present, Path: fact(module.Path), Version: fact(module.Version),
			Sum: fact(module.Sum), ReplacementKind: string(module.Replacement.Kind), ReplacementPath: fact(module.Replacement.Path),
			ReplacementVersion: fact(module.Replacement.Version), ReplacementSum: fact(module.Replacement.Sum)})
	}
	for _, option := range observed.Platform {
		result.Platform = append(result.Platform, sqlengine.Option{Name: option.Name, Value: option.Value})
	}
	return result, nil
}
