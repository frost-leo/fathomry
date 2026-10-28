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

package create

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"go/format"
	"io/fs"
	"strings"
	"text/template"

	"golang.org/x/mod/modfile"
)

//go:embed templates/go.mod.tmpl templates/README.md.tmpl templates/.gitignore.tmpl
//go:embed templates/cmd/PROJECT_NAME/main.go.tmpl templates/cmd/PROJECT_NAME/main_test.go.tmpl
//go:embed templates/config/README.md.tmpl
var templates embed.FS

type renderedFile struct {
	path string
	data []byte
}

type plan struct {
	options options
	files   []renderedFile
}

type renderBuffer struct{ bytes.Buffer }

func (buffer *renderBuffer) Write(value []byte) (int, error) {
	if len(value) > maxRenderedBytes-buffer.Len() {
		return 0, fail(ErrRender, "render_limit")
	}
	return buffer.Buffer.Write(value)
}

func prepare(ctx context.Context, value options) (plan, error) {
	if err := validate(value); err != nil {
		return plan{}, err
	}
	return render(ctx, value, templates)
}

func render(ctx context.Context, value options, sources fs.FS) (plan, error) {
	result := plan{options: value}
	paths := []struct{ source, target string }{
		{"go.mod.tmpl", "go.mod"}, {"README.md.tmpl", "README.md"},
		{".gitignore.tmpl", ".gitignore"},
		{"cmd/PROJECT_NAME/main.go.tmpl", "cmd/" + value.Name + "/main.go"},
		{"cmd/PROJECT_NAME/main_test.go.tmpl", "cmd/" + value.Name + "/main_test.go"},
		{"config/README.md.tmpl", "config/README.md"},
	}
	total := 0
	seen := make(map[string]bool, len(paths))
	for _, file := range paths {
		if err := cancellation(ctx); err != nil {
			return plan{}, err
		}
		if !validRelative(file.target) || seen[file.target] {
			return plan{}, fail(ErrRender, "manifest")
		}
		seen[file.target] = true
		raw, err := fs.ReadFile(sources, "templates/"+file.source)
		if err != nil {
			return plan{}, fail(ErrRender, "template", err)
		}
		if len(raw) > maxRenderedBytes {
			return plan{}, fail(ErrRender, "template_limit")
		}
		parsed, err := template.New(file.source).Option("missingkey=error").Parse(string(raw))
		if err != nil {
			return plan{}, fail(ErrRender, "template", err)
		}
		buffer := new(renderBuffer)
		dataModel := struct {
			options
			GoVersion, FrameworkModule string
		}{value, goVersion, frameworkModule}
		if err := parsed.Execute(buffer, dataModel); err != nil {
			return plan{}, fail(ErrRender, "render", err)
		}
		data := bytes.Clone(buffer.Bytes())
		if strings.HasSuffix(file.target, ".go") {
			data, err = format.Source(data)
			if err != nil {
				return plan{}, fail(ErrRender, "format", err)
			}
		}
		if file.target == "go.mod" {
			module, err := modfile.Parse("go.mod", data, nil)
			if err != nil {
				return plan{}, fail(ErrRender, "module", err)
			}
			if module.Module == nil || module.Module.Mod.Path != value.Module ||
				module.Go == nil || module.Go.Version != goVersion ||
				len(module.Require) != 1 || module.Require[0].Mod.Path != frameworkModule ||
				module.Require[0].Mod.Version != frameworkVersion ||
				module.Require[0].Indirect || module.Toolchain != nil ||
				len(module.Replace) != 0 || len(module.Exclude) != 0 ||
				len(module.Retract) != 0 || len(module.Tool) != 0 ||
				len(module.Ignore) != 0 || len(module.Godebug) != 0 {
				return plan{}, fail(ErrRender, "module")
			}
		}
		if len(data) > maxRenderedBytes-total {
			return plan{}, fail(ErrRender, "render_limit")
		}
		total += len(data)
		result.files = append(result.files, renderedFile{path: file.target, data: data})
	}
	maximum := newReport(value)
	maximum.Directories = []string{".", "cmd", "cmd/" + value.Name, "config"}
	for _, file := range result.files {
		maximum.Files = append(maximum.Files, fileReport{Path: file.path, BytesWritten: int64(len(file.data)), Complete: true})
	}
	maximum.State, maximum.FailedPath = "not_started", "cmd/"+value.Name+"/main_test.go"
	raw, err := json.Marshal(maximum)
	if err != nil || len(raw) > maxReportBytes {
		return plan{}, fail(ErrRender, "report_limit", err)
	}
	if err := cancellation(ctx); err != nil {
		return plan{}, err
	}
	return result, nil
}
