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
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
)

type fileReport struct {
	Path         string `json:"path"`
	BytesWritten int64  `json:"bytes_written"`
	Complete     bool   `json:"complete"`
}

type report struct {
	Name             string       `json:"name"`
	ModulePath       string       `json:"module_path"`
	FrameworkVersion string       `json:"framework_version"`
	Target           string       `json:"target"`
	Directories      []string     `json:"directories"`
	Files            []fileReport `json:"files"`
	State            string       `json:"state"`
	FailedPath       string       `json:"failed_path,omitempty"`
}

func newReport(value options) report {
	return report{Name: value.Name, ModulePath: value.Module, FrameworkVersion: value.FrameworkVersion,
		Target: value.Name, Directories: []string{}, Files: []fileReport{}, State: "not_started"}
}

func (result *report) finish() {
	if len(result.Directories) == 0 {
		return
	}
	result.State = "partial"
	if len(result.Files) != len(templateFiles) || len(result.Directories) != len(projectDirectories)+1 {
		return
	}
	for _, file := range result.Files {
		if !file.Complete {
			return
		}
	}
	result.State = "complete"
}

func cancellation(ctx context.Context) error {
	if ctx == nil {
		return fail(ErrInput, "context")
	}
	if ctx.Err() != nil {
		return fail(ErrCanceled, "create", ctx.Err(), context.Cause(ctx))
	}
	return nil
}

func create(ctx context.Context, selected plan) (result report, err error) {
	result = newReport(selected.options)
	if err := cancellation(ctx); err != nil {
		return result, err
	}
	parent, err := os.OpenRoot(".")
	if err != nil {
		return result, fail(ErrCreate, "open_parent", err)
	}
	defer func() {
		if cause := parent.Close(); cause != nil {
			err = fail(ErrCreate, "close_parent", err, cause)
		}
		if cause := cancellation(ctx); cause != nil {
			if err == nil {
				err = cause
			} else {
				err = fail(ErrCreate, "create", err, cause)
			}
		}
	}()
	return createInParent(ctx, parent, selected, openFile)
}

type fileOpener func(*os.Root, string) (io.WriteCloser, error)

func openFile(root *os.Root, path string) (io.WriteCloser, error) {
	mode := fs.FileMode(0644)
	if path == ".env" {
		mode = 0600
	}
	return root.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
}

// The caller retains parent. The selected plan is already validated and rendered.
func createInParent(ctx context.Context, parent *os.Root, selected plan, open fileOpener) (result report, err error) {
	result = newReport(selected.options)
	defer result.finish()
	if err := cancellation(ctx); err != nil {
		return result, err
	}
	if err := parent.Mkdir(selected.options.Name, 0755); err != nil {
		result.FailedPath = "."
		if errors.Is(err, fs.ErrExist) {
			return result, fail(ErrExists, "claim", err)
		}
		return result, fail(ErrCreate, "claim", err)
	}
	result.Directories = append(result.Directories, ".")
	root, err := parent.OpenRoot(selected.options.Name)
	if err != nil {
		result.FailedPath = "."
		return result, fail(ErrCreate, "open_target", err)
	}
	defer func() {
		if cause := root.Close(); cause != nil {
			if result.FailedPath == "" {
				result.FailedPath = "."
			}
			err = fail(ErrCreate, "close_target", err, cause)
		}
	}()
	for _, directory := range projectDirectories {
		if err := cancellation(ctx); err != nil {
			return result, err
		}
		if err := root.Mkdir(directory, 0755); err != nil {
			result.FailedPath = directory
			return result, fail(ErrCreate, "mkdir", err)
		}
		result.Directories = append(result.Directories, directory)
	}
	for _, file := range selected.files {
		if err := cancellation(ctx); err != nil {
			return result, err
		}
		written, created, cause := writeFile(ctx, root, file, open)
		if created {
			result.Files = append(result.Files, written)
		}
		if cause != nil {
			result.FailedPath = file.path
			return result, fail(ErrCreate, "write", cause)
		}
	}
	return result, cancellation(ctx)
}

func writeFile(ctx context.Context, root *os.Root, file renderedFile, open fileOpener) (result fileReport, created bool, err error) {
	result.Path = file.path
	if err := cancellation(ctx); err != nil {
		return result, false, err
	}
	output, err := open(root, file.path)
	if err != nil {
		return result, false, err
	}
	created = true
	if err = cancellation(ctx); err == nil {
		var count int
		count, err = output.Write(file.data)
		if count < 0 || count > len(file.data) {
			err = errors.Join(err, io.ErrShortWrite)
		} else {
			result.BytesWritten = int64(count)
			if count != len(file.data) {
				err = errors.Join(err, io.ErrShortWrite)
			}
		}
	}
	closeError := output.Close()
	result.Complete = err == nil && closeError == nil && result.BytesWritten == int64(len(file.data))
	return result, created, errors.Join(err, closeError, cancellation(ctx))
}
