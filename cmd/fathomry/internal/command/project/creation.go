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
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/failure"
	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
	"golang.org/x/mod/semver"
)

const FrameworkModule = "github.com/frost-leo/fathomry"

// Public identities withhold native paths and parser diagnostics. Cancellation
// preserves the caller's intentional cause. Incomplete means a created directory
// remains caller-owned; it does not imply rollback or a usable project.
const (
	InvalidInput  failure.Code = "fathomry.project.invalid_input"
	Exists        failure.Code = "fathomry.project.exists"
	Unavailable   failure.Code = "fathomry.project.unavailable"
	InvalidBundle failure.Code = "fathomry.project.invalid_bundle"
	Incomplete    failure.Code = "fathomry.project.incomplete"
	Cancelled     failure.Code = "fathomry.project.cancelled"
)

// Options are borrowed during Create. Directory is an absolute clean new target,
// not a parent. Name is a portable lowercase project/binary name (1-64 bytes).
// Module is an explicit independent module path. FrameworkVersion is an exact
// canonical v0/v1 module version, including a valid pre-release/pseudo-version.
// Configuration selects local (default) or remote; Provider selects viper or
// nacos respectively. EnvironmentSources overrides declared environments with
// name=mode/provider entries. DefaultLocale selects en or zh-Hans resources.
// Bundle optionally selects an absolute clean local bundle directory; no network,
// installed-module-cache discovery, shell command or Git operation is performed.
type Options struct {
	private
	Directory          string
	Name               string
	Module             string
	FrameworkVersion   string
	Configuration      string
	Provider           string
	EnvironmentSources []string
	DefaultLocale      string
	Bundle             string
}

// Result describes filesystem effects, independently of the returned error.
// Directory deliberately exposes the caller-selected path. Created transfers
// cleanup responsibility to the caller, even on error. FilesWritten counts
// successfully closed files. Complete confirms generation, not build/readiness.
type Result struct {
	private
	Directory    string
	Created      bool
	Complete     bool
	FilesWritten int
}

// Create validates all declarations and bundle content before creating the target.
// It refuses any existing destination, including an empty directory or symlink.
// Partial writes remain visible and owned; it never recursively deletes a target.
// Source bundle files must be stable regular files during this call. Contexts
// cannot interrupt arbitrary blocked filesystem operations or prove crash durability.
func Create(ctx context.Context, options Options) (Result, error) {
	if ctx == nil {
		return Result{}, problem(InvalidInput)
	}
	if err := contextError(ctx); err != nil {
		return Result{}, err
	}
	if _, err := selectSources(options); err != nil {
		return Result{}, err
	}
	if options.DefaultLocale == "" {
		options.DefaultLocale = "en"
	}
	if !absolute(options.Directory) || !name(options.Name) || len(options.Module) > 256 || module.CheckPath(options.Module) != nil ||
		options.Module == FrameworkModule || strings.HasPrefix(options.Module, FrameworkModule+"/") ||
		!frameworkVersion(options.FrameworkVersion) || options.DefaultLocale != "en" && options.DefaultLocale != "zh-Hans" ||
		options.Bundle != "" && !absolute(options.Bundle) {
		return Result{}, problem(InvalidInput)
	}
	files, err := renderProject(options)
	if err != nil {
		return Result{}, problem(Unavailable)
	}
	parent, err := os.OpenRoot(filepath.Dir(options.Directory))
	if err != nil {
		return Result{}, problem(Unavailable)
	}
	defer parent.Close()
	parentInfo, err := parent.Stat(".")
	if err != nil {
		return Result{}, problem(Unavailable)
	}
	var bundle *snapshot
	if options.Bundle != "" {
		bundle, err = openSnapshot(ctx, options.Bundle, options.FrameworkVersion, parentInfo)
		if err != nil {
			return Result{}, err
		}
		defer bundle.root.Close()
		for _, entry := range bundle.manifest.Modules {
			if moduleOverlap(entry.Path, options.Module) {
				return Result{}, problem(InvalidBundle)
			}
		}
	}
	moduleFile := new(modfile.File)
	if moduleFile.AddModuleStmt(options.Module) != nil || moduleFile.AddGoStmt("1.27.0") != nil ||
		moduleFile.AddRequire(FrameworkModule, options.FrameworkVersion) != nil {
		return Result{}, problem(InvalidInput)
	}
	if bundle != nil {
		for _, entry := range bundle.manifest.Modules {
			if moduleFile.AddRequire(entry.Path, entry.Version) != nil ||
				moduleFile.AddReplace(entry.Path, "", "./third_party/fathomry/"+entry.Directory, "") != nil {
				return Result{}, problem(InvalidBundle)
			}
		}
		files["third_party/fathomry/manifest.json"] = bundle.raw
	}
	moduleFile.SortBlocks()
	encoded, err := moduleFile.Format()
	if err != nil {
		return Result{}, problem(InvalidInput)
	}
	files["go.mod"] = append([]byte(moduleNotice), encoded...)
	if err := contextError(ctx); err != nil {
		return Result{}, err
	}
	target := filepath.Base(options.Directory)
	if target == "." || target == string(filepath.Separator) {
		return Result{}, problem(InvalidInput)
	}
	if err := parent.Mkdir(target, 0755); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return Result{}, problem(Exists)
		}
		return Result{}, problem(Unavailable)
	}
	result := Result{Directory: options.Directory, Created: true}
	fail := func(err error) (Result, error) { return result, failure.New(Incomplete, err) }
	destination, err := parent.OpenRoot(target)
	if err != nil {
		return fail(problem(Unavailable))
	}
	defer destination.Close()
	for _, path := range sortedKeys(files) {
		if err := writeFile(ctx, destination, path, files[path]); err != nil {
			return fail(err)
		}
		result.FilesWritten++
	}
	if bundle != nil {
		for _, entry := range bundle.manifest.Modules {
			for _, path := range sortedKeys(entry.Files) {
				data, err := readSnapshotFile(ctx, bundle.root, entry.Directory+"/"+path, entry.Files[path])
				if err != nil {
					return fail(err)
				}
				if err := writeFile(ctx, destination, "third_party/fathomry/"+entry.Directory+"/"+path, data); err != nil {
					return fail(err)
				}
				result.FilesWritten++
			}
		}
	}
	if err := contextError(ctx); err != nil {
		return fail(err)
	}
	if bundle != nil {
		if err := verifyDependencies(ctx, options.Directory, options.FrameworkVersion, bundle.digest); err != nil {
			return fail(err)
		}
	}
	result.Complete = true
	return result, nil
}

func writeFile(ctx context.Context, root *os.Root, path string, data []byte) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := root.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return problem(Unavailable)
	}
	file, err := root.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return problem(Unavailable)
	}
	count, writeErr := file.Write(data)
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil || count != len(data) {
		return problem(Unavailable)
	}
	return nil
}

func absolute(path string) bool {
	return len(path) <= 4096 && utf8.ValidString(path) && !strings.ContainsRune(path, 0) &&
		filepath.IsAbs(path) && filepath.Clean(path) == path
}

func name(value string) bool {
	if len(value) == 0 || len(value) > 64 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '-' || char == '_') {
			return false
		}
	}
	return module.CheckFilePath(value) == nil
}

func frameworkVersion(value string) bool {
	return len(value) <= 128 && semver.IsValid(value) && module.CanonicalVersion(value) == value &&
		(semver.Major(value) == "v0" || semver.Major(value) == "v1") && !strings.Contains(value, "+")
}

func moduleOverlap(left, right string) bool {
	return left == right || strings.HasPrefix(left, right+"/") || strings.HasPrefix(right, left+"/")
}

func contextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return failure.New(Cancelled, errors.Join(err, context.Cause(ctx)))
	}
	return nil
}
func problem(code failure.Code) error { return failure.New(code, nil) }

type private struct{}

func (private) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, "project[restricted]") }
func (private) LogValue() slog.Value           { return slog.StringValue("project[restricted]") }
func (private) MarshalJSON() ([]byte, error)   { return nil, problem(InvalidInput) }
func (*private) UnmarshalJSON([]byte) error    { return problem(InvalidInput) }
