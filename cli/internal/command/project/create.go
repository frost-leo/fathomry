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
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/mod/modfile"
	"golang.org/x/mod/module"
)

const (
	frameworkModule = "github.com/frost-leo/fathomry"
	supportedGo     = "1.27.0"
	maxModuleBytes  = 1 << 20
)

var (
	errArguments = errors.New("project: directory, module and source are required")
	errIdentity  = errors.New("project: conflicting project module identity")
	errSource    = errors.New("project: unsupported source module profile")
	errOverlap   = errors.New("project: destination is inside the framework source")
)

type request struct {
	directory string
	module    string
	source    string
}

func (input request) validate() error {
	if input.directory == "" || input.source == "" || input.module == "" ||
		strings.ContainsRune(input.directory, 0) || strings.ContainsRune(input.source, 0) {
		return errArguments
	}
	for _, path := range []string{input.directory, input.source} {
		if !filepath.IsAbs(path) && (filepath.VolumeName(path) != "" || os.IsPathSeparator(path[0])) {
			return errArguments
		}
	}
	if err := module.CheckPath(input.module); err != nil {
		return err
	}
	if input.module == frameworkModule || input.module == frameworkModule+"/cli" {
		return errIdentity
	}
	return nil
}

type effect uint8

const (
	untouched effect = iota
	partial
	complete
)

type outputFile struct {
	name string
	data []byte
}

type prepared struct {
	directory string
	files     []outputFile
}

func create(ctx context.Context, input request) (effect, error) {
	output, err := prepare(ctx, input)
	if err != nil {
		return untouched, errors.Join(err, canceled(ctx))
	}
	return output.write(ctx, exclusiveFile)
}

func prepare(ctx context.Context, input request) (prepared, error) {
	if err := canceled(ctx); err != nil {
		return prepared{}, err
	}
	if err := input.validate(); err != nil {
		return prepared{}, err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return prepared{}, err
	}
	cwd, err = filepath.EvalSymlinks(cwd)
	if err != nil {
		return prepared{}, err
	}
	resolve := func(path string) string {
		if filepath.IsAbs(path) {
			return path
		}
		// Join would erase symlink/.. before filesystem traversal.
		return cwd + string(filepath.Separator) + path
	}
	source := resolve(input.source)
	realSource, err := filepath.EvalSymlinks(source)
	if err != nil {
		return prepared{}, err
	}
	destination := resolve(input.directory)
	if _, err := os.Lstat(destination); err == nil {
		return prepared{}, &os.PathError{Op: "create", Path: destination, Err: os.ErrExist}
	} else if !errors.Is(err, os.ErrNotExist) {
		return prepared{}, err
	}
	for len(destination) > 0 && os.IsPathSeparator(destination[len(destination)-1]) {
		destination = destination[:len(destination)-1]
	}
	parent, name := filepath.Split(destination)
	parent, err = filepath.EvalSymlinks(parent)
	if err != nil {
		return prepared{}, err
	}
	info, err := os.Stat(parent)
	if err != nil {
		return prepared{}, err
	}
	if !info.IsDir() {
		return prepared{}, errors.New("project: parent is not a directory")
	}
	destination = filepath.Join(parent, name)
	within, err := filepath.Rel(realSource, destination)
	if err != nil {
		return prepared{}, err
	}
	if within == "." || within != ".." && !strings.HasPrefix(within, ".."+string(filepath.Separator)) {
		return prepared{}, errOverlap
	}
	if err := inspectSource(ctx, realSource); err != nil {
		return prepared{}, err
	}
	// Go cleans replacement paths; retain an alias only if cleaning selects the same source.
	sourceBinding := filepath.Clean(source)
	if resolved, err := filepath.EvalSymlinks(sourceBinding); err != nil || resolved != realSource {
		sourceBinding = realSource
	}
	replacement, err := filepath.Rel(destination, sourceBinding)
	if err != nil {
		return prepared{}, err
	}
	replacement = filepath.ToSlash(replacement)
	if !strings.HasPrefix(replacement, "./") && !strings.HasPrefix(replacement, "../") {
		replacement = "./" + replacement
	}
	files, err := render(input.module, replacement)
	if err != nil {
		return prepared{}, err
	}
	if err := canceled(ctx); err != nil {
		return prepared{}, err
	}
	return prepared{destination, files}, nil
}

func inspectSource(ctx context.Context, source string) error {
	path := filepath.Join(source, "go.mod")
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > maxModuleBytes {
		return errSource
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maxModuleBytes+1))
	if err := errors.Join(readErr, file.Close(), canceled(ctx)); err != nil {
		return err
	}
	if len(data) > maxModuleBytes {
		return errSource
	}
	metadata, err := modfile.Parse("go.mod", data, nil)
	if err != nil {
		return errors.Join(errSource, err)
	}
	if metadata.Module == nil || metadata.Module.Mod.Path != frameworkModule ||
		metadata.Go == nil || metadata.Go.Version != supportedGo ||
		metadata.Toolchain != nil && metadata.Toolchain.Name != "default" && metadata.Toolchain.Name != "go"+supportedGo {
		return errSource
	}
	info, err = os.Stat(filepath.Join(source, "cli"))
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errSource
	}
	return canceled(ctx)
}

func exclusiveFile(path string) (io.WriteCloser, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
}

func (output prepared) write(ctx context.Context, openFile func(string) (io.WriteCloser, error)) (effect, error) {
	if err := canceled(ctx); err != nil {
		return untouched, err
	}
	if err := os.Mkdir(output.directory, 0o755); err != nil {
		return untouched, errors.Join(err, canceled(ctx))
	}
	for _, file := range output.files {
		if err := canceled(ctx); err != nil {
			return partial, err
		}
		writer, err := openFile(filepath.Join(output.directory, file.name))
		if err != nil {
			return partial, errors.Join(err, canceled(ctx))
		}
		if err := canceled(ctx); err != nil {
			return partial, errors.Join(err, writer.Close())
		}
		count, writeErr := writer.Write(file.data)
		if count != len(file.data) {
			writeErr = errors.Join(writeErr, io.ErrShortWrite)
		}
		if err := errors.Join(writeErr, writer.Close()); err != nil {
			return partial, errors.Join(err, canceled(ctx))
		}
	}
	return complete, canceled(ctx)
}

func canceled(ctx context.Context) error {
	if ctx.Err() == nil {
		return nil
	}
	return errors.Join(ctx.Err(), context.Cause(ctx))
}
