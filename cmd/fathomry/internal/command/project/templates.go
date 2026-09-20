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

package project

import (
	"bytes"
	"embed"
	"go/format"
	"strings"
	"text/template"
)

//go:embed templates/*
var templates embed.FS

type templateData struct {
	Name              string
	Module            string
	FrameworkVersion  string
	Prefix            string
	DefaultLocale     string
	Local             bool
	Remote            bool
	Environments      []environmentSource
	Environment       string
	RemoteEnvironment bool
	GoNotice          string
	TextNotice        string
	YAMLNotice        string
}

func renderProject(options Options) (map[string][]byte, error) {
	plan, err := selectSources(options)
	if err != nil {
		return nil, err
	}
	locale := options.DefaultLocale
	if locale == "" {
		locale = "en"
	}
	data := templateData{Name: options.Name, Module: options.Module, FrameworkVersion: options.FrameworkVersion,
		Prefix: strings.ToUpper(strings.ReplaceAll(options.Name, "-", "_")), DefaultLocale: locale,
		Local: plan.Local, Remote: plan.Remote, Environments: plan.Environments,
		GoNotice: goNotice, TextNotice: textNotice, YAMLNotice: yamlNotice}
	mapping := map[string]string{
		"main.go.tmpl":               "cmd/" + options.Name + "/main.go",
		"configuration.go.tmpl":      "internal/configuration/configuration.go",
		"resource.go.tmpl":           "internal/resource/configuration.go",
		"resource_test.go.tmpl":      "internal/resource/configuration_test.go",
		"bindings.go.tmpl":           "internal/configuration/bindings.go",
		"sources.go.tmpl":            "internal/configuration/sources.go",
		"configuration_test.go.tmpl": "internal/configuration/configuration_test.go",
		"localization.go.tmpl":       "internal/localization/localization.go",
		"readme.md.tmpl":             "README.md", "gitignore.tmpl": ".gitignore",
		"base.yaml.tmpl":  "configs/base.yaml",
		"local.yaml.tmpl": "configs/local/development.example.yaml",
	}
	if plan.Local {
		mapping["local.go.tmpl"] = "internal/configuration/sources_local.go"
	}
	if plan.Remote {
		mapping["nacos.go.tmpl"] = "internal/configuration/sources_nacos.go"
	}
	files := make(map[string][]byte)
	for source, target := range mapping {
		content, err := renderTemplate(source, data)
		if err != nil {
			return nil, err
		}
		files[target] = content
	}
	for _, environment := range plan.Environments {
		files["configs/environments/"+environment.Name+".yaml"] = []byte(yamlNotice +
			"# Override only this environment's settings; other fields inherit base.\n{}\n")
		selected := data
		selected.Environment = environment.Name
		selected.RemoteEnvironment = environment.Mode == "remote"
		content, err := renderTemplate("dotenv.tmpl", selected)
		if err != nil {
			return nil, err
		}
		files[".env."+environment.Name+".example"] = content
		if environment.Name == "development" {
			files[".env.example"] = content
		}
	}
	license, err := templates.ReadFile("templates/COPYING")
	if err != nil {
		return nil, err
	}
	files["LICENSE"] = license
	for _, locale := range []string{"en", "zh-Hans"} {
		content, err := templates.ReadFile("templates/" + locale + ".json")
		if err != nil {
			return nil, err
		}
		files["internal/localization/locales/"+locale+".json"] = content
	}
	return files, nil
}

func renderTemplate(name string, data templateData) ([]byte, error) {
	raw, err := templates.ReadFile("templates/" + name)
	if err != nil {
		return nil, err
	}
	parsed, err := template.New(name).Funcs(template.FuncMap{"upper": strings.ToUpper}).Delims("[[", "]]").Option("missingkey=error").Parse(string(raw))
	if err != nil {
		return nil, err
	}
	var output bytes.Buffer
	if err := parsed.Execute(&output, data); err != nil {
		return nil, err
	}
	if strings.HasSuffix(name, ".go.tmpl") {
		return format.Source(output.Bytes())
	}
	return output.Bytes(), nil
}
