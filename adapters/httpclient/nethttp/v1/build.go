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

package nethttp

import native "github.com/frost-leo/fathomry/internal/httpclient/nethttp/v1"

// ModuleInfo separates selected and replaced module identities; missing facts
// remain unknown. Local replacement paths never escape.
type ModuleInfo struct {
	private
	Present                                             bool
	Path, Version, Sum                                  Fact
	ReplacementKind                                     string
	ReplacementPath, ReplacementVersion, ReplacementSum Fact
}

// BuildInfo describes the actual executing binary. Go identifies net/http;
// there is no independently versioned standard-library SDK module. SDKs records
// the selected x/net helper module used for owned SOCKS/IDNA behavior.
type BuildInfo struct {
	private
	Metadata  string
	Go        Fact
	Framework ModuleInfo
	SDKs      []ModuleInfo
	Platform  []Option
}

// Build performs local executable metadata inspection only.
func Build() (BuildInfo, error) {
	value, err := native.Build()
	if err != nil {
		return BuildInfo{}, translate(err, "build")
	}
	module := value.Framework
	result := BuildInfo{Metadata: string(value.Metadata), Go: fact(value.Go),
		Framework: ModuleInfo{Present: module.Present, Path: fact(module.Path), Version: fact(module.Version), Sum: fact(module.Sum),
			ReplacementKind: string(module.Replacement.Kind), ReplacementPath: fact(module.Replacement.Path),
			ReplacementVersion: fact(module.Replacement.Version), ReplacementSum: fact(module.Replacement.Sum)}}
	for _, module := range value.SDKs {
		result.SDKs = append(result.SDKs, ModuleInfo{Present: module.Present, Path: fact(module.Path), Version: fact(module.Version), Sum: fact(module.Sum),
			ReplacementKind: string(module.Replacement.Kind), ReplacementPath: fact(module.Replacement.Path),
			ReplacementVersion: fact(module.Replacement.Version), ReplacementSum: fact(module.Replacement.Sum)})
	}
	for _, option := range value.Platform {
		result.Platform = append(result.Platform, Option{Name: option.Name, Value: option.Value})
	}
	return result, nil
}
