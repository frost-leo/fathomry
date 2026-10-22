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

package vet

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
)

type inputFile interface {
	io.Reader
	Stat() (os.FileInfo, error)
	Close() error
}

func openInput(path string) (inputFile, error) { return os.OpenFile(path, inputReadFlags(), 0) }

func readInput(ctx context.Context, path string, maximum int) ([]byte, error) {
	return readFile(ctx, path, maximum, openInput)
}

func readFile(ctx context.Context, path string, maximum int, open func(string) (inputFile, error)) (raw []byte, err error) {
	if ctx.Err() != nil {
		return nil, command.Fail(command.ErrCanceled, ctx.Err(), context.Cause(ctx))
	}
	before, err := os.Stat(path)
	if err != nil {
		return nil, command.Fail(command.ErrExecution, err)
	}
	if !before.Mode().IsRegular() {
		return nil, command.Fail(command.ErrUsage)
	}
	if maximum < 0 || before.Size() > int64(maximum) {
		return nil, command.Fail(command.ErrLimit)
	}
	file, err := open(path)
	if err != nil {
		return nil, command.Fail(command.ErrExecution, err)
	}
	defer func() {
		if problem := file.Close(); problem != nil {
			raw = nil
			err = command.Fail(command.ErrCleanup, errors.Join(err, problem))
		}
	}()
	opened, err := file.Stat()
	if err != nil {
		return nil, command.Fail(command.ErrExecution, err)
	}
	if !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		return nil, command.Fail(command.ErrExecution)
	}
	if opened.Size() < 0 || opened.Size() > int64(maximum) {
		return nil, command.Fail(command.ErrLimit)
	}
	raw, err = io.ReadAll(io.LimitReader(contextInput{ctx: ctx, reader: file}, int64(maximum)+1))
	if ctx.Err() != nil {
		return nil, command.Fail(command.ErrCanceled, ctx.Err(), context.Cause(ctx), err)
	}
	if err != nil {
		return nil, command.Fail(command.ErrExecution, err)
	}
	if len(raw) > maximum {
		return nil, command.Fail(command.ErrLimit)
	}
	return raw, nil
}

type contextInput struct {
	ctx    context.Context
	reader io.Reader
}

func (input contextInput) Read(data []byte) (int, error) {
	if err := input.ctx.Err(); err != nil {
		return 0, err
	}
	return input.reader.Read(data)
}
