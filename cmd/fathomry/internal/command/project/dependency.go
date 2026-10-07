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
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"go/version"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"

	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
)

type dependency struct {
	goVersion    string
	version      string
	source       string
	replacements []replacement
}
type replacement struct{ old, new module.Version }
type modulePin struct{ original, directory, version, sum string }

func sdkPins() []modulePin {
	return []modulePin{
		{"github.com/bogdanfinn/tls-client", "third_party/tls-client", "v0.0.0-20261007150852-1c04f9c3b198", "h1:g/j5H3E2ktrVj4TT7sZ2ANJfH24HocDIYgpxQqn77e8="},
		{"github.com/enetx/http2", "third_party/surf-http2", "v0.0.0-20260930074612-e7c164f1d613", "h1:fFGtOTpPXhfyiZeBadmxGLHZVdxGPmPLsgQFTbzdfp8="},
		{"github.com/enetx/http3", "third_party/surf-http3", "v0.0.0-20260930074612-e7c164f1d613", "h1:aAdnugFgXbWxuWRQRcdV1gKuSoZP4fEzs0M7WP/cTMI="},
		{"github.com/enetx/surf", "third_party/surf", "v0.0.0-20260930074612-e7c164f1d613", "h1:OIyhEtx4Z26/K7iLsB5WGxYTvOTNsSvYdw000hC0F0M="},
		{"github.com/nukilabs/qpack", "third_party/nuki-qpack", "v0.0.0-20260930074612-e7c164f1d613", "h1:70IZ753KVivUDel6GRCQpYkQz5ZIbPC1zLGbPYSFPAk="},
		{"github.com/nukilabs/quic-go", "third_party/nuki-quic-go", "v0.0.0-20260930074612-e7c164f1d613", "h1:hvTCpen/HHo8v+xfCqDCs5hz12l1WwsY88G8DnWJdRE="},
		{"github.com/nukilabs/socks", "third_party/nuki-socks", "v0.0.0-20260930074612-e7c164f1d613", "h1:gCQEnfYNVwLit12biJMWMT9eBNrxRGCgGMdVrXULp+4="},
		{"github.com/nukilabs/tlsclient", "third_party/nuki", "v0.0.0-20260930074612-e7c164f1d613", "h1:jBuc2XNyjG6nUiCmTOswBxusE8mXDedn9ydkrGOndh8="},
		{"github.com/sardanioss/httpcloak", "third_party/httpcloak", "v0.0.0-20260930074612-e7c164f1d613", "h1:hLY4LNZ10ImRPqmyxdXbEjnd2GQXi1dNBqsJpcM6D6c="},
		{"github.com/sardanioss/net", "third_party/httpcloak-net", "v0.0.0-20260930074612-e7c164f1d613", "h1:s6yIw3Kwg5po+YuXevV/eQbOmrjj5HXwstF+eqfGf6o="},
		{"github.com/sardanioss/quic-go", "third_party/httpcloak-quic-go", "v0.0.0-20260930074612-e7c164f1d613", "h1:cZrWOXVpeXdj3zWVyySZFSfocnA9oAQfHchW24smvsU="},
		{"github.com/sardanioss/udpbara", "third_party/udpbara", "v0.0.0-20260930074612-e7c164f1d613", "h1:4kf1j4tSYRiQmv1CWCUY/kM6KzUnhn6DwdVevIcHV20="},
		{"go.temporal.io/sdk", "third_party/temporal-sdk", "v0.0.0-20260930074612-e7c164f1d613", "h1:MdY61SwGWMujwmuVY0TYZMl69nTl8kdFQh+fFDWeTdI="},
	}
}

func selectDependency(source string) (dependency, error) {
	if source != "" {
		return developmentDependency(source)
	}
	info, ok := debug.ReadBuildInfo()
	if !ok || info.Main.Path != frameworkModule || info.Main.Replace != nil || !exactModuleVersion(frameworkModule, info.Main.Version) {
		return dependency{}, fail(ErrDependency)
	}
	result := dependency{goVersion: minimumGo, version: info.Main.Version}
	for _, pin := range sdkPins() {
		result.replacements = append(result.replacements, replacement{module.Version{Path: pin.original}, module.Version{Path: frameworkModule + "/" + pin.directory, Version: pin.version}})
	}
	return result, nil
}

func exactModuleVersion(path, value string) bool {
	return len(value) <= 256 && value != "" && module.CanonicalVersion(value) == value && module.Check(path, value) == nil
}

func developmentDependency(path string) (result dependency, err error) {
	path, err = filepath.Abs(path)
	if err != nil {
		return result, fail(ErrDependency, err)
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return result, fail(ErrDependency, err)
	}
	defer func() {
		if problem := root.Close(); problem != nil {
			err = fail(ErrDependency, err, problem)
		}
	}()
	parsed, err := readModule(root, "go.mod")
	if err != nil {
		return result, err
	}
	if parsed.Module == nil || parsed.Module.Mod.Path != frameworkModule || parsed.Go == nil || version.Compare("go"+parsed.Go.Version, "go"+minimumGo) < 0 || len(parsed.Exclude) != 0 || len(parsed.Replace) > 64 {
		return result, fail(ErrDependency)
	}
	if err := checkConfigurationAPI(root); err != nil {
		return result, err
	}
	result = dependency{source: filepath.ToSlash(path), version: "v0.0.0", goVersion: parsed.Go.Version}
	seen := map[module.Version]bool{}
	for _, entry := range parsed.Replace {
		if module.CheckPath(entry.Old.Path) != nil || entry.Old.Version != "" && !exactModuleVersion(entry.Old.Path, entry.Old.Version) || seen[entry.Old] {
			return dependency{}, fail(ErrDependency)
		}
		seen[entry.Old] = true
		next := replacement{entry.Old, entry.New}
		if entry.New.Version == "" {
			local := filepath.Clean(entry.New.Path)
			if !filepath.IsLocal(local) {
				return dependency{}, fail(ErrDependency)
			}
			declaration, err := readModule(root, filepath.Join(local, "go.mod"))
			if err != nil {
				return dependency{}, err
			}
			if declaration.Module == nil || declaration.Module.Mod.Path != entry.Old.Path {
				return dependency{}, fail(ErrDependency)
			}
			next.new.Path = filepath.ToSlash(filepath.Join(path, local))
		} else if !exactModuleVersion(entry.New.Path, entry.New.Version) {
			return dependency{}, fail(ErrDependency)
		}
		result.replacements = append(result.replacements, next)
	}
	slices.SortFunc(result.replacements, func(left, right replacement) int {
		if order := strings.Compare(left.old.Path, right.old.Path); order != 0 {
			return order
		}
		return strings.Compare(left.old.Version, right.old.Version)
	})
	return result, nil
}

func readSourceFile(root *os.Root, path string) (raw []byte, err error) {
	before, err := root.Stat(path)
	if err != nil {
		return nil, fail(ErrDependency, err)
	}
	if !before.Mode().IsRegular() || before.Size() > 1<<20 {
		return nil, fail(ErrDependency)
	}
	file, err := root.OpenFile(path, sourceReadFlags(), 0)
	if err != nil {
		return nil, fail(ErrDependency, err)
	}
	defer func() {
		if problem := file.Close(); problem != nil {
			raw = nil
			err = fail(ErrDependency, err, problem)
		}
	}()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || opened.Size() > 1<<20 || !os.SameFile(before, opened) {
		return nil, fail(ErrDependency, err)
	}
	raw, err = io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil || len(raw) > 1<<20 {
		return nil, fail(ErrDependency, err)
	}
	return raw, nil
}
func readModule(root *os.Root, path string) (*modfile.File, error) {
	raw, err := readSourceFile(root, path)
	if err != nil {
		return nil, err
	}
	value, err := modfile.Parse(path, raw, nil)
	if err != nil {
		return nil, fail(ErrDependency, err)
	}
	return value, nil
}

// Source admission checks exported declarations, not implementation filenames.
// It is not a claim that an arbitrary checkout has passed compatibility tests.
func checkConfigurationAPI(root *os.Root) error {
	directory, err := root.OpenFile("framework/configuration/v1", sourceReadFlags(), 0)
	if err != nil {
		return fail(ErrDependency, err)
	}
	info, err := directory.Stat()
	if err != nil || !info.IsDir() {
		return fail(ErrDependency, err, directory.Close())
	}
	entries, err := directory.ReadDir(65)
	closeErr := directory.Close()
	if err != nil && !errors.Is(err, io.EOF) || closeErr != nil || len(entries) > 64 {
		return fail(ErrDependency, err, closeErr)
	}
	functions := map[string]bool{}
	fields := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		raw, err := readSourceFile(root, filepath.Join("framework/configuration/v1", entry.Name()))
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), entry.Name(), raw, parser.SkipObjectResolution)
		if err != nil {
			return fail(ErrDependency, err)
		}
		for _, declaration := range file.Decls {
			if function, ok := declaration.(*ast.FuncDecl); ok && (function.Name.Name == "Load" || function.Name.Name == "Watch" || function.Name.Name == "ReadInputs" || function.Name.Name == "PrepareNacos") && function.Recv == nil {
				functions[function.Name.Name] = true
			}
			if block, ok := declaration.(*ast.GenDecl); ok && block.Tok == token.TYPE {
				for _, spec := range block.Specs {
					if value, ok := spec.(*ast.TypeSpec); ok && (value.Name.Name == "Declaration" || value.Name.Name == "Dependencies") {
						if definition, ok := value.Type.(*ast.StructType); ok {
							for _, field := range definition.Fields.List {
								for _, name := range field.Names {
									fields[value.Name.Name+"."+name.Name] = true
								}
							}
						}
					}
				}
			}
		}
	}
	if !functions["Load"] || !functions["Watch"] || !functions["ReadInputs"] || !functions["PrepareNacos"] || !fields["Declaration.Variables"] || !fields["Dependencies.Provider"] || fields["Declaration.Layers"] || fields["Dependencies.Source"] || fields["Dependencies.Runtime"] || fields["Declaration.Environment"] {
		return fail(ErrDependency)
	}
	return nil
}
