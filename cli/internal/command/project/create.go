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

const (
	errArguments = ErrArguments
	errIdentity  = ErrIdentity
	errSource    = ErrSource
	errOverlap   = ErrOverlap
)

type request struct {
	directory string
	module    string
	source    string
}

func (input request) validate() error {
	if input.directory == "" || input.source == "" || input.module == "" ||
		strings.ContainsRune(input.directory, 0) || strings.ContainsRune(input.source, 0) {
		return failed(ErrArguments)
	}
	for _, path := range []string{input.directory, input.source} {
		if !filepath.IsAbs(path) && (filepath.VolumeName(path) != "" || os.IsPathSeparator(path[0])) {
			return failed(ErrArguments)
		}
	}
	if err := module.CheckPath(input.module); err != nil {
		return failed(ErrArguments, err)
	}
	if input.module == frameworkModule || input.module == frameworkModule+"/cli" {
		return failed(ErrIdentity)
	}
	return nil
}

// effect is this invocation's creation-stage observation, not a recovery policy.
// Untouched does not mean the destination is absent. Partial means complete
// write/close success was not established, even if all bytes happen to exist.
// Complete reports all planned writes/closes succeeded, not crash durability.
type effect uint8

const (
	unknown effect = iota
	untouched
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

type creationResult struct {
	observation effect
	err         error
}

func create(ctx context.Context, input request) creationResult {
	output, err := prepare(ctx, input)
	if err != nil {
		return creationResult{observation: untouched, err: combine(err, canceled(ctx))}
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
		return prepared{}, failed(ErrPreparation, err)
	}
	cwd, err = filepath.EvalSymlinks(cwd)
	if err != nil {
		return prepared{}, failed(ErrPreparation, err)
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
		return prepared{}, failed(ErrSource, err)
	}
	destination := resolve(input.directory)
	if _, err := os.Lstat(destination); err == nil {
		return prepared{}, failed(ErrExists, &os.PathError{Op: "create", Path: destination, Err: os.ErrExist})
	} else if !errors.Is(err, os.ErrNotExist) {
		return prepared{}, failed(ErrDestination, err)
	}
	for len(destination) > 0 && os.IsPathSeparator(destination[len(destination)-1]) {
		destination = destination[:len(destination)-1]
	}
	parent, name := filepath.Split(destination)
	parent, err = filepath.EvalSymlinks(parent)
	if err != nil {
		return prepared{}, failed(ErrDestination, err)
	}
	info, err := os.Stat(parent)
	if err != nil {
		return prepared{}, failed(ErrDestination, err)
	}
	if !info.IsDir() {
		return prepared{}, failed(ErrDestination)
	}
	destination = filepath.Join(parent, name)
	within, err := filepath.Rel(realSource, destination)
	if err != nil {
		return prepared{}, failed(ErrDestination, err)
	}
	if within == "." || within != ".." && !strings.HasPrefix(within, ".."+string(filepath.Separator)) {
		return prepared{}, failed(ErrOverlap)
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
		return prepared{}, failed(ErrPreparation, err)
	}
	replacement = filepath.ToSlash(replacement)
	if !strings.HasPrefix(replacement, "./") && !strings.HasPrefix(replacement, "../") {
		replacement = "./" + replacement
	}
	files, err := render(input.module, replacement)
	if err != nil {
		return prepared{}, failed(ErrPreparation, err)
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
		return failed(ErrSource, err)
	}
	if !info.Mode().IsRegular() || info.Size() > maxModuleBytes {
		return failed(ErrSource)
	}
	file, err := os.Open(path)
	if err != nil {
		return failed(ErrSource, err)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maxModuleBytes+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return failed(ErrSource, combine(readErr, closeErr, canceled(ctx)))
	}
	if err := canceled(ctx); err != nil {
		return err
	}
	if len(data) > maxModuleBytes {
		return failed(ErrSource)
	}
	metadata, err := modfile.Parse("go.mod", data, nil)
	if err != nil {
		return failed(ErrSource, err)
	}
	if metadata.Module == nil || metadata.Module.Mod.Path != frameworkModule ||
		metadata.Go == nil || metadata.Go.Version != supportedGo ||
		metadata.Toolchain != nil && metadata.Toolchain.Name != "default" && metadata.Toolchain.Name != "go"+supportedGo {
		return failed(ErrSource)
	}
	info, err = os.Stat(filepath.Join(source, "cli"))
	if err != nil {
		return failed(ErrSource, err)
	}
	if !info.IsDir() {
		return failed(ErrSource)
	}
	return canceled(ctx)
}

func exclusiveFile(path string) (io.WriteCloser, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
}

func (output prepared) write(ctx context.Context, openFile func(string) (io.WriteCloser, error)) creationResult {
	if err := canceled(ctx); err != nil {
		return creationResult{observation: untouched, err: err}
	}
	if err := os.Mkdir(output.directory, 0o755); err != nil {
		condition := ErrDestination
		if errors.Is(err, os.ErrExist) {
			condition = ErrExists
		}
		return creationResult{observation: untouched, err: failed(condition, combine(err, canceled(ctx)))}
	}
	for _, file := range output.files {
		if err := canceled(ctx); err != nil {
			return creationResult{observation: partial, err: err}
		}
		writer, err := openFile(filepath.Join(output.directory, file.name))
		if err != nil {
			return creationResult{observation: partial, err: failed(ErrCreation, combine(err, canceled(ctx)))}
		}
		if err := canceled(ctx); err != nil {
			if closeErr := writer.Close(); closeErr != nil {
				return creationResult{observation: partial, err: combine(err, failed(ErrCreation, closeErr))}
			}
			return creationResult{observation: partial, err: err}
		}
		count, writeErr := writer.Write(file.data)
		if count != len(file.data) {
			writeErr = combine(writeErr, io.ErrShortWrite)
		}
		if err := combine(writeErr, writer.Close()); err != nil {
			return creationResult{observation: partial, err: failed(ErrCreation, combine(err, canceled(ctx)))}
		}
	}
	return creationResult{observation: complete, err: canceled(ctx)}
}

func canceled(ctx context.Context) error {
	if ctx.Err() == nil {
		return nil
	}
	return errors.Join(ctx.Err(), context.Cause(ctx))
}
