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

package viper

import (
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/framework/configuration"
)

// File selects one original document. Name is a public non-secret label; Path is
// a private relative slash path without dot/parent components or backslashes.
// Encoding defaults to yaml (which also accepts JSON syntax); json selects the
// stricter native JSON acquisition parser. Layer must be Base, Environment or Local.
// Optional permits only absence, never invalid or inaccessible content.
type File struct {
	private
	Name     string
	Path     string
	Layer    configuration.Layer
	Encoding string
	Optional bool
}

// Options declares a finite local acquisition. Root is a literal absolute path,
// and must already be filepath.Clean; it is not a containment guarantee or
// environment expansion. SchemaVersion is the declared
// project-data schema format (default 1), independent of Viper/adapter versions.
// Up to three files are allowed, with unique public names and layers. Empty Files
// deliberately selects defaults/environment-only preparation. No I/O runs in New.
type Options struct {
	private
	Root          string
	SchemaVersion uint32
	Files         []File
}

// New validates the complete declaration before any file is opened. It copies
// strings and slice storage. Mutating Options later cannot reconfigure Provider.
func New(options Options) (*Provider, error) {
	if !filepath.IsAbs(options.Root) || filepath.Clean(options.Root) != options.Root || len(options.Root) > 4096 ||
		!utf8.ValidString(options.Root) || strings.ContainsRune(options.Root, 0) ||
		len(options.Files) > configuration.MaxSources {
		return nil, problem(configuration.InvalidInput, "", nil)
	}
	names := make(map[string]bool)
	layers := make(map[configuration.Layer]bool)
	files := slices.Clone(options.Files)
	for index, file := range files {
		if !label(file.Name) || names[file.Name] || layers[file.Layer] ||
			file.Layer < configuration.Base || file.Layer > configuration.Local ||
			file.Path == "." || !fs.ValidPath(file.Path) || !filepath.IsLocal(filepath.FromSlash(file.Path)) || len(file.Path) > 4096 ||
			!utf8.ValidString(file.Path) || strings.ContainsAny(file.Path, "\x00\\") ||
			file.Encoding != "" && file.Encoding != "yaml" && file.Encoding != "json" {
			return nil, problem(configuration.InvalidInput, "", nil)
		}
		if len(filepath.Join(options.Root, filepath.FromSlash(file.Path))) > 4096 {
			return nil, problem(configuration.InvalidInput, "", nil)
		}
		names[file.Name], layers[file.Layer] = true, true
		file.Name, file.Path = strings.Clone(file.Name), strings.Clone(file.Path)
		file.Encoding = strings.Clone(file.Encoding)
		if file.Encoding == "" {
			file.Encoding = "yaml"
		}
		files[index] = file
	}
	slices.SortFunc(files, func(left, right File) int { return int(left.Layer) - int(right.Layer) })
	format := options.SchemaVersion
	if format == 0 {
		format = 1
	}
	return &Provider{root: strings.Clone(filepath.Clean(options.Root)), format: format, files: files}, nil
}

func label(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '.' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}
