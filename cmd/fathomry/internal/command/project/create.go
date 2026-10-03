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
	"slices"
	"strings"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
)

func create(ctx context.Context, tree plan, write func(*os.Root, projectFile) error) (err error) {
	if ctx.Err() != nil {
		return command.Fail(command.ErrCanceled, ctx.Err(), context.Cause(ctx))
	}
	parent, err := os.OpenRoot(filepath.Dir(tree.destination))
	if err != nil {
		return fail(ErrDestination, err)
	}
	created := false
	defer func() {
		if closeErr := parent.Close(); closeErr != nil {
			code := ErrDestination
			if created {
				code = ErrPartial
			}
			err = fail(code, err, closeErr)
		}
	}()
	leaf := filepath.Base(tree.destination)
	if err := parent.Mkdir(leaf, 0755); err != nil {
		return fail(ErrDestination, err)
	}
	created = true
	before, err := parent.Lstat(leaf)
	if err != nil || !before.IsDir() {
		return fail(ErrPartial, err)
	}
	root, err := parent.OpenRoot(leaf)
	if err != nil {
		return fail(ErrPartial, err)
	}
	defer func() {
		if closeErr := root.Close(); closeErr != nil {
			err = fail(ErrPartial, err, closeErr)
		}
	}()
	opened, err := root.Stat(".")
	if err != nil || !os.SameFile(before, opened) {
		return fail(ErrPartial, err)
	}
	folders := map[string]bool{}
	for _, file := range tree.files {
		for folder := filepath.Dir(file.name); folder != "."; folder = filepath.Dir(folder) {
			folders[folder] = true
		}
	}
	ordered := make([]string, 0, len(folders))
	for folder := range folders {
		ordered = append(ordered, folder)
	}
	slices.SortFunc(ordered, func(left, right string) int {
		if depth := strings.Count(left, string(filepath.Separator)) - strings.Count(right, string(filepath.Separator)); depth != 0 {
			return depth
		}
		return strings.Compare(left, right)
	})
	for _, folder := range ordered {
		if ctx.Err() != nil {
			return fail(ErrPartial, ctx.Err(), context.Cause(ctx))
		}
		if err := root.Mkdir(folder, 0755); err != nil {
			return fail(ErrPartial, err)
		}
	}
	for _, file := range tree.files {
		if ctx.Err() != nil {
			return fail(ErrPartial, ctx.Err(), context.Cause(ctx))
		}
		if err := write(root, file); err != nil {
			return fail(ErrPartial, err)
		}
	}
	if ctx.Err() != nil {
		return fail(ErrPartial, ctx.Err(), context.Cause(ctx))
	}
	return nil
}
func writeProjectFile(root *os.Root, value projectFile) error {
	file, err := root.OpenFile(filepath.FromSlash(value.name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	count, err := file.Write(value.content)
	if err == nil && count != len(value.content) {
		err = io.ErrShortWrite
	}
	return errors.Join(err, file.Close())
}
