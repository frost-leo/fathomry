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
	"embed"
	"encoding/json"
	"fmt"
	"os"
	"runtime/debug"

	"github.com/frost-leo/fathomry/i18n"
)

//go:embed en.json zh.json golden.json
var files embed.FS

func main() {
	var resources []i18n.Resource
	for _, name := range []string{"en.json", "zh.json"} {
		data, err := files.ReadFile(name)
		if err != nil {
			panic(err)
		}
		resources = append(resources, i18n.Resource{Name: name, Data: data})
	}
	catalog, err := i18n.Prepare(resources)
	if err != nil {
		panic(err)
	}
	var golden map[string]json.RawMessage
	data, err := files.ReadFile("golden.json")
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(data, &golden); err != nil {
		panic(err)
	}
	for _, pair := range [][2]string{{"en", "en"}, {"zh-CN", "zh"}} {
		result, err := catalog.Render(pair[0], "business.receipt", i18n.Arguments{"Count": i18n.Number("2")})
		if err != nil {
			panic(err)
		}
		var expected string
		if err := json.Unmarshal(golden[pair[1]], &expected); err != nil {
			panic(err)
		}
		if result.Text != expected || result.Fallback != i18n.NoFallback {
			panic("consumer.render_mismatch")
		}
	}
	build, ok := debug.ReadBuildInfo()
	if !ok {
		panic("consumer.build_info_missing")
	}
	versions := make(map[string]string)
	for _, dependency := range build.Deps {
		if dependency.Replace != nil {
			panic("consumer.unexpected_replacement")
		}
		versions[dependency.Path] = dependency.Version
	}
	if versions["github.com/frost-leo/fathomry"] == "" || versions["github.com/nicksnyder/go-i18n/v2"] == "" ||
		versions["golang.org/x/text"] == "" {
		panic("consumer.dependency_provenance_missing")
	}
	fmt.Println(catalog.Snapshot().ID)
	if err := json.NewEncoder(os.Stdout).Encode(versions); err != nil {
		panic(err)
	}
}
