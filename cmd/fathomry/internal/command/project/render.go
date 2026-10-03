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

package project

import (
	"bytes"
	"embed"
	"go/format"
	"io/fs"
	"slices"
	"strconv"
	"strings"
	"text/template"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"golang.org/x/mod/modfile"
)

//go:embed templates/*
var templates embed.FS

type templateValues struct{ Name, Module, Mode, Provider, Encoding, Environment, GoVersion, Version, Source string }

func render(tree plan) ([]projectFile, error) {
	values := templateValues{Name: tree.name, Module: tree.module, Mode: tree.mode, Provider: tree.provider, Encoding: tree.encoding, GoVersion: tree.dependency.goVersion, Version: tree.dependency.version, Source: tree.dependency.source}
	entries := []struct{ template, path string }{
		{"gitignore.tmpl", ".gitignore"}, {"dotenv.tmpl", ".env.example"}, {"readme.tmpl", "README.md"},
		{"main.go.tmpl", "cmd/" + tree.name + "/main.go"}, {"main_test.go.tmpl", "cmd/" + tree.name + "/main_test.go"},
		{"boot.go.tmpl", "internal/bootstrap/boot.go"}, {"boot_" + tree.mode + "_test.go.tmpl", "internal/bootstrap/boot_test.go"},
		{"options_" + tree.mode + ".go.tmpl", "internal/bootstrap/options.go"}, {"options_" + tree.mode + "_test.go.tmpl", "internal/bootstrap/options_test.go"},
		{"inputs.go.tmpl", "internal/bootstrap/inputs.go"}, {"inputs_test.go.tmpl", "internal/bootstrap/inputs_test.go"},
		{"setting.go.tmpl", "internal/setting/setting.go"}, {"setting_test.go.tmpl", "internal/setting/setting_test.go"},
		{"application.go.tmpl", "internal/setting/application.go"}, {"application_test.go.tmpl", "internal/setting/application_test.go"},
		{"base." + tree.encoding + ".tmpl", "configs/base." + tree.encoding},
	}
	if tree.mode == "local" {
		entries = append(entries, struct{ template, path string }{"override." + tree.encoding + ".tmpl", "configs/local/development.example." + tree.encoding})
	} else {
		entries = append(entries, struct{ template, path string }{"remote." + tree.encoding + ".tmpl", "bootstrap.example." + tree.encoding})
	}
	var result []projectFile
	for _, entry := range entries {
		data, err := renderTemplate(entry.template, values)
		if err != nil {
			return nil, err
		}
		if strings.HasSuffix(entry.path, ".go") {
			data, err = format.Source(data)
			if err != nil {
				return nil, command.Fail(command.ErrExecution, err)
			}
		}
		result = append(result, projectFile{entry.path, data})
	}
	for _, environment := range []string{"development", "testing", "production"} {
		values.Environment = environment
		data, err := renderTemplate("environment."+tree.encoding+".tmpl", values)
		if err != nil {
			return nil, err
		}
		result = append(result, projectFile{"configs/environments/" + environment + "." + tree.encoding, data})
	}
	notice, err := templates.ReadFile("templates/module.tmpl")
	if err != nil {
		return nil, command.Fail(command.ErrExecution, err)
	}
	mod, err := modfile.Parse("go.mod", notice, nil)
	if err != nil {
		return nil, command.Fail(command.ErrExecution, err)
	}
	if err = mod.AddModuleStmt(tree.module); err == nil {
		err = mod.AddGoStmt(tree.dependency.goVersion)
	}
	if err == nil {
		err = mod.AddRequire(frameworkModule, tree.dependency.version)
	}
	if err == nil && tree.dependency.source != "" {
		err = mod.AddReplace(frameworkModule, "", tree.dependency.source, "")
	}
	for _, entry := range tree.dependency.replacements {
		if err == nil {
			err = mod.AddReplace(entry.old.Path, entry.old.Version, entry.new.Path, entry.new.Version)
		}
	}
	if err != nil {
		return nil, command.Fail(command.ErrExecution, err)
	}
	module, err := mod.Format()
	if err != nil {
		return nil, command.Fail(command.ErrExecution, err)
	}
	result = append(result, projectFile{"go.mod", module})
	slices.SortFunc(result, func(left, right projectFile) int { return strings.Compare(left.name, right.name) })
	total := 0
	for _, file := range result {
		total += len(file.content)
		if !fs.ValidPath(file.name) || total > 1<<20 || len(result) > 32 {
			return nil, command.Fail(command.ErrLimit)
		}
	}
	return result, nil
}
func renderTemplate(name string, values templateValues) ([]byte, error) {
	raw, err := templates.ReadFile("templates/" + name)
	if err != nil {
		return nil, command.Fail(command.ErrExecution, err)
	}
	parsed, err := template.New(name).Funcs(template.FuncMap{"quote": strconv.Quote}).Option("missingkey=error").Parse(string(raw))
	if err != nil {
		return nil, command.Fail(command.ErrExecution, err)
	}
	var buffer bytes.Buffer
	if err := parsed.Execute(&buffer, values); err != nil {
		return nil, command.Fail(command.ErrExecution, err)
	}
	return buffer.Bytes(), nil
}
