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
	"archive/zip"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"testing"

	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
	"golang.org/x/mod/sumdb/dirhash"
	modzip "golang.org/x/mod/zip"
)

type moduleSource struct{ name, path string }

func (file moduleSource) Path() string                 { return file.name }
func (file moduleSource) Lstat() (os.FileInfo, error)  { return os.Lstat(file.path) }
func (file moduleSource) Open() (io.ReadCloser, error) { return os.Open(file.path) }

func TestDependency(t *testing.T) {
	t.Run("development_replacements_remain_inside_selected_checkout", func(t *testing.T) {
		result, err := developmentDependency(repository(t))
		if err != nil {
			t.Fatal(err)
		}
		if result.version != "v0.0.0" || result.goVersion != minimumGo || result.source != filepath.ToSlash(repository(t)) || len(result.replacements) != len(sdkPins()) {
			t.Fatal("incorrect development dependency")
		}
		for _, entry := range result.replacements {
			if entry.new.Version != "" || !strings.HasPrefix(entry.new.Path, result.source+"/third_party/") {
				t.Fatal("replacement escaped selected checkout")
			}
		}
	})
	t.Run("versions_are_exact_not_latest_or_dirty", func(t *testing.T) {
		for _, version := range []string{"latest", "main", "(devel)", "v1.0.0+dirty", "v1.0.0+custom", ""} {
			if exactModuleVersion(frameworkModule, version) {
				t.Fatal("non-exact version admitted")
			}
		}
		for _, version := range []string{"v0.1.0", "v0.0.0-20260930074612-e7c164f1d613"} {
			if !exactModuleVersion(frameworkModule, version) {
				t.Fatal("exact version refused")
			}
		}
		if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version == "(devel)" {
			if _, err := selectDependency(""); err == nil {
				t.Fatal("development build invented a release")
			}
		}
	})
	t.Run("module_file_admission", func(t *testing.T) {
		directory := t.TempDir()
		root, err := os.OpenRoot(directory)
		if err != nil {
			t.Fatal(err)
		}
		defer root.Close()
		for _, fixture := range []struct {
			name, content string
			valid         bool
		}{
			{"go.mod", "module example.org/valid\ngo 1.27.0\n", true}, {"space file", "module example.org/valid\n", true},
			{"malformed", "not a module", false}, {"oversized", strings.Repeat(" ", (1<<20)+1), false},
		} {
			if err := os.WriteFile(filepath.Join(directory, fixture.name), []byte(fixture.content), 0600); err != nil {
				t.Fatal(err)
			}
			mod, err := readModule(root, fixture.name)
			if (err == nil) != fixture.valid || err == nil && mod.Module == nil {
				t.Fatal("wrong module admission", fixture.name)
			}
		}
		if _, err := readModule(root, "."); err == nil {
			t.Fatal("directory admitted")
		}
		if _, err := readModule(root, "missing"); err == nil {
			t.Fatal("missing module admitted")
		}
	})
}

func TestSDKReplacementPolicy(t *testing.T) {
	root := repository(t)
	raw, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	moduleFile, err := modfile.Parse("go.mod", raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	pins := sdkPins()
	if len(pins) != len(moduleFile.Replace) {
		t.Fatal("SDK replacement policy is incomplete")
	}
	for _, pin := range pins {
		t.Run(pin.directory, func(t *testing.T) {
			found := false
			for _, entry := range moduleFile.Replace {
				if entry.Old.Path == pin.original && filepath.ToSlash(filepath.Clean(entry.New.Path)) == pin.directory {
					found = true
				}
			}
			if !found {
				t.Fatal("pin no longer matches maintained source")
			}
			source := filepath.Join(root, filepath.FromSlash(pin.directory))
			checked, err := modzip.CheckDir(source)
			if err != nil || checked.Err() != nil {
				t.Fatal("invalid module source", err)
			}
			files := make([]modzip.File, 0, len(checked.Valid)+1)
			hasLicense := false
			for _, path := range checked.Valid {
				relative, err := filepath.Rel(source, path)
				if err != nil {
					t.Fatal(err)
				}
				name := filepath.ToSlash(relative)
				if name == "LICENSE" {
					hasLicense = true
				}
				files = append(files, moduleSource{name, path})
			}
			if !hasLicense {
				files = append(files, moduleSource{"LICENSE", filepath.Join(root, "LICENSE")})
			}
			slices.SortFunc(files, func(left, right modzip.File) int { return strings.Compare(left.Path(), right.Path()) })
			var buffer bytes.Buffer
			if err := modzip.Create(&buffer, module.Version{Path: frameworkModule + "/" + pin.directory, Version: pin.version}, files); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "module.zip")
			if err := os.WriteFile(path, buffer.Bytes(), 0600); err != nil {
				t.Fatal(err)
			}
			sum, err := dirhash.HashZip(path, dirhash.Hash1)
			if err != nil || sum != pin.sum {
				t.Fatalf("maintained SDK differs from pinned source: %s / %s: %v", sum, pin.sum, err)
			}
			reader, err := zip.NewReader(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
			if err != nil || len(reader.File) == 0 {
				t.Fatal("empty source qualification")
			}
		})
	}
}
