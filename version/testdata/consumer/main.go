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

package main

import (
	"encoding/json"
	"os"
	"reflect"

	_ "example.org/hidden"
	"example.org/selected"
	"github.com/frost-leo/fathomry/i18n"
	"github.com/frost-leo/fathomry/version"
	"github.com/frost-leo/fathomry/version/presentation"
)

type component struct {
	Path, Version, Replacement, ReplacementPath, ReplacementVersion string
	Present, Main                                                   bool
}

type report struct {
	Code, CheckCode, Go, GOOS, GOARCH                     string
	Main, Framework                                       component
	Dependencies                                          []component
	NativeRevision, NativeTree, CommitTime                string
	Release, Revision, Tree, BuildTime, DeclarationOrigin string
	English, Chinese, Fallback                            string
	Behavior                                              int
}

func output(value report) {
	if err := json.NewEncoder(os.Stdout).Encode(value); err != nil {
		panic(err)
	}
}

func main() {
	build, err := version.Inspect(version.Request{
		Dependencies:  []string{"example.org/selected", "example.org/absent"},
		DisclosePaths: []string{"example.org/version-consumer", "example.org/fork"},
	})
	if err != nil {
		output(report{Code: err.Error()})
		return
	}
	snapshot := build.Snapshot()
	project := func(module version.Module) component {
		return component{
			Path: module.Path.Value, Version: module.Version.Value, Present: module.Present, Main: module.Main,
			Replacement: string(module.Replacement.Kind), ReplacementPath: module.Replacement.Path.Value,
			ReplacementVersion: module.Replacement.Version.Value,
		}
	}
	result := report{
		Go: snapshot.Go.Value, Main: project(snapshot.Main), Framework: project(snapshot.Framework),
		NativeRevision: snapshot.Source.Revision.Value, NativeTree: string(snapshot.Source.Tree),
		CommitTime: snapshot.Source.CommitTime.String(),
		Release:    snapshot.Declaration.Release.String(), Revision: snapshot.Declaration.GitRevision,
		Tree: string(snapshot.Declaration.Tree), BuildTime: snapshot.Declaration.BuildTime.String(),
		DeclarationOrigin: string(snapshot.Declaration.Origin), Behavior: selected.Value(),
	}
	for _, module := range snapshot.Dependencies {
		result.Dependencies = append(result.Dependencies, project(module))
	}
	for _, setting := range snapshot.Settings {
		switch setting.Name {
		case "GOOS":
			result.GOOS = setting.Value
		case "GOARCH":
			result.GOARCH = setting.Value
		}
	}
	if len(os.Args) == 5 {
		err := build.CheckDeclaration(version.Declaration{
			Release: os.Args[1], GitRevision: os.Args[2], Tree: version.TreeState(os.Args[3]), BuildTime: os.Args[4],
		})
		if err != nil {
			result.CheckCode = err.Error()
		}
	}
	catalog, err := i18n.Prepare(presentation.Resources())
	if err != nil || len(catalog.Snapshot().Stale) != 0 {
		panic("invalid resources")
	}
	english, err := presentation.Summary(catalog, "en", build)
	if err != nil {
		panic(err)
	}
	chinese, err := presentation.Summary(catalog, "zh-Hans", build)
	if err != nil {
		panic(err)
	}
	fallback, err := presentation.Summary(catalog, "de", build)
	if err != nil || fallback.Text != english.Text || fallback.Fallback != i18n.UnsupportedLocale ||
		chinese.ResourceLocale != "zh-Hans" || chinese.Text == english.Text || !reflect.DeepEqual(snapshot, build.Snapshot()) {
		panic("presentation boundary failed")
	}
	result.English, result.Chinese, result.Fallback = english.Text, chinese.Text, string(fallback.Fallback)
	output(result)
}
