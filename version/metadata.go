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

package version

import (
	"runtime/debug"
	"strings"

	"github.com/frost-leo/fathomry/internal/compatibility"
)

func validateMetadata(info *debug.BuildInfo, request Request) error {
	if _, err := compatibility.FromBuildInfo(nil, compatibility.BuildRequest{
		SDKModules: request.Dependencies, DisclosePaths: request.DisclosePaths,
	}); err != nil {
		return problem(InvalidRequest)
	}
	if info == nil {
		return nil
	}
	if len(info.Deps) > MaxModules || len(info.Settings) > MaxSettings {
		return problem(LimitExceeded)
	}
	remaining := MaxMetadataBytes
	bounded := func(values ...string) bool {
		for _, value := range values {
			if len(value) > remaining {
				return false
			}
			remaining -= len(value)
		}
		return true
	}
	if !bounded(info.GoVersion, info.Path) {
		return problem(LimitExceeded)
	}
	selected := map[string]bool{FrameworkModule: true, info.Main.Path: true}
	for _, path := range request.Dependencies {
		selected[path] = true
	}
	seenModules := make(map[string]bool)
	modules := append([]*debug.Module{&info.Main}, info.Deps...)
	for _, module := range modules {
		if module == nil {
			continue
		}
		if !bounded(module.Path, module.Version, module.Sum) {
			return problem(LimitExceeded)
		}
		if replacement := module.Replace; replacement != nil {
			if !bounded(replacement.Path, replacement.Version, replacement.Sum) {
				return problem(LimitExceeded)
			}
			if replacement.Replace != nil {
				return problem(InvalidMetadata)
			}
		}
		if !selected[module.Path] || module.Path == "" {
			continue
		}
		if seenModules[module.Path] {
			return problem(Conflict)
		}
		seenModules[module.Path] = true
		if _, err := Parse(module.Version); err != nil {
			return problem(InvalidMetadata)
		}
		if module.Replace != nil {
			if _, err := Parse(module.Replace.Version); err != nil {
				return problem(InvalidMetadata)
			}
		}
	}
	seenSettings := make(map[string]bool)
	for _, setting := range info.Settings {
		if !bounded(setting.Key, setting.Value) {
			return problem(LimitExceeded)
		}
		if !relevantSetting(setting.Key) {
			continue
		}
		if seenSettings[setting.Key] {
			return problem(Conflict)
		}
		seenSettings[setting.Key] = true
		if setting.Value == "" {
			continue
		}
		switch setting.Key {
		case "vcs", "vcs.revision", "vcs.modified", "vcs.time":
			if info.Main.Path == "" {
				return problem(InvalidMetadata)
			}
		case "-race", "-asan", "-msan", "-cover", "-trimpath":
			if setting.Value != "true" && setting.Value != "false" {
				return problem(InvalidMetadata)
			}
		case "CGO_ENABLED":
			if setting.Value != "0" && setting.Value != "1" {
				return problem(InvalidMetadata)
			}
		default:
			if len(setting.Value) > 128 || strings.Contains(setting.Value, ",") && setting.Key != "GOARM" && setting.Key != "GOARM64" {
				return problem(InvalidMetadata)
			}
			for _, char := range setting.Value {
				if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' ||
					char >= '0' && char <= '9' || strings.ContainsRune("-._+,", char)) {
					return problem(InvalidMetadata)
				}
			}
		}
	}
	return nil
}

func relevantSetting(key string) bool {
	switch key {
	case "vcs", "vcs.revision", "vcs.modified", "vcs.time",
		"GOOS", "GOARCH", "GOAMD64", "GOARM", "GOARM64", "GO386", "GOMIPS", "GOMIPS64",
		"GOPPC64", "GORISCV64", "CGO_ENABLED", "GOFIPS140", "-compiler", "-race",
		"-buildmode", "-asan", "-msan", "-cover", "-trimpath":
		return true
	}
	return false
}

func sourceFacts(settings []debug.BuildSetting) (Source, error) {
	values := make(map[string]string, 4)
	for _, setting := range settings {
		switch setting.Key {
		case "vcs", "vcs.revision", "vcs.modified", "vcs.time":
			values[setting.Key] = setting.Value
		}
	}
	result := Source{}
	system := values["vcs"]
	switch system {
	case "":
		if values["vcs.revision"] != "" || values["vcs.modified"] != "" || values["vcs.time"] != "" {
			return Source{}, problem(InvalidMetadata)
		}
		return result, nil
	case "git", "hg", "svn", "bzr", "fossil":
		result.VCS = Fact{Value: strings.Clone(system), Evidence: Reported}
	default:
		return Source{}, problem(InvalidMetadata)
	}
	if revision := values["vcs.revision"]; revision != "" {
		if system == "git" {
			if !gitObjectID(revision) {
				return Source{}, problem(InvalidMetadata)
			}
			result.Revision = Fact{Value: strings.Clone(revision), Evidence: Reported}
		} else {
			result.Revision = Fact{Evidence: Redacted}
		}
	}
	switch values["vcs.modified"] {
	case "":
	case "true":
		result.Tree = Dirty
	case "false":
		result.Tree = Clean
	default:
		return Source{}, problem(InvalidMetadata)
	}
	timestamp, err := ParseTimestamp(values["vcs.time"])
	if err != nil {
		return Source{}, problem(InvalidMetadata)
	}
	result.CommitTime = timestamp
	return result, nil
}
