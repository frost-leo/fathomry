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
	_ "embed"

	"golang.org/x/mod/modfile"
)

//go:embed templates/go.mod.tmpl
var moduleTemplate []byte

//go:embed templates/main.go.tmpl
var mainTemplate []byte

//go:embed templates/README.md
var readmeTemplate []byte

//go:embed templates/gitignore.tmpl
var ignoreTemplate []byte

func render(modulePath, replacement string) ([]outputFile, error) {
	metadata, err := modfile.Parse("go.mod", moduleTemplate, nil)
	if err != nil {
		return nil, err
	}
	if err := metadata.AddModuleStmt(modulePath); err != nil {
		return nil, err
	}
	if err := metadata.AddGoStmt(supportedGo); err != nil {
		return nil, err
	}
	if err := metadata.AddRequire(frameworkModule, "v0.0.0"); err != nil {
		return nil, err
	}
	if err := metadata.AddReplace(frameworkModule, "", replacement, ""); err != nil {
		return nil, err
	}
	moduleData, err := metadata.Format()
	if err != nil {
		return nil, err
	}
	if _, err := modfile.Parse("go.mod", moduleData, nil); err != nil {
		return nil, err
	}
	return []outputFile{
		{"go.mod", moduleData},
		{"main.go", mainTemplate},
		{"README.md", readmeTemplate},
		{".gitignore", ignoreTemplate},
	}, nil
}
