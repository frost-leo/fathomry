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

package build

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

const maxExecutableBytes = 1 << 30

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader contextReader) Read(data []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(data)
}

func executableHash(ctx context.Context, path string) (string, error) {
	file, err := openExecutable(path)
	if err != nil {
		return "", err
	}
	digest, err := copyExecutable(ctx, io.Discard, file)
	return digest, errors.Join(err, file.Close())
}

func openExecutable(path string) (*os.File, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() {
		return nil, fail(ErrInspection)
	}
	file, err := os.OpenFile(path, executableReadFlags(), 0)
	if err != nil {
		return nil, err
	}
	after, err := file.Stat()
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) {
		return nil, fail(ErrInspection, err, file.Close())
	}
	return file, nil
}
func copyExecutable(ctx context.Context, destination io.Writer, file *os.File) (string, error) {
	info, err := file.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 || info.Size() == 0 || info.Size() > maxExecutableBytes {
		return "", fail(ErrLimit)
	}
	hasher := sha256.New()
	count, err := io.Copy(io.MultiWriter(destination, hasher), contextReader{ctx, io.LimitReader(file, maxExecutableBytes+1)})
	if err != nil {
		return "", err
	}
	if count != info.Size() || count > maxExecutableBytes {
		return "", fail(ErrLimit)
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func publish(ctx context.Context, source, destination, expected string) (published bool, err error) {
	if err := canceled(ctx); err != nil {
		return false, err
	}
	if source == destination {
		return false, fail(ErrOptions)
	}
	parent, name := filepath.Dir(destination), filepath.Base(destination)
	if err := os.MkdirAll(parent, 0755); err != nil {
		return false, fail(ErrPublish, err)
	}
	root, err := os.OpenRoot(parent)
	if err != nil {
		return false, fail(ErrPublish, err)
	}
	defer func() {
		if cleanup := root.Close(); cleanup != nil {
			err = fail(ErrCleanup, err, cleanup)
		}
	}()
	checkDestination := func() error {
		info, err := root.Lstat(name)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fail(ErrOptions)
		}
		sourceInfo, err := os.Stat(source)
		if err != nil {
			return err
		}
		if os.SameFile(sourceInfo, info) {
			return fail(ErrOptions)
		}
		return nil
	}
	if err := checkDestination(); err != nil {
		return false, fail(ErrPublish, err)
	}
	staged := ".fathomry-" + rand.Text()
	output, err := root.OpenFile(staged, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return false, fail(ErrPublish, err)
	}
	defer func() {
		if cleanup := root.Remove(staged); cleanup != nil && !errors.Is(cleanup, fs.ErrNotExist) {
			err = fail(ErrCleanup, err, cleanup)
		}
	}()
	input, err := openExecutable(source)
	if err != nil {
		return false, fail(ErrPublish, err, output.Close())
	}
	info, err := input.Stat()
	if err != nil {
		return false, fail(ErrPublish, err, input.Close(), output.Close())
	}
	actual, copied := copyExecutable(ctx, output, input)
	copied = errors.Join(copied, input.Close())
	if copied == nil && actual != expected {
		copied = fail(ErrInspection)
	}
	if copied == nil {
		copied = output.Chmod(info.Mode().Perm())
	}
	if copied == nil {
		copied = output.Sync()
	}
	copied = errors.Join(copied, output.Close())
	if copied != nil {
		return false, phaseError(ctx, ErrPublish, copied)
	}
	if err := canceled(ctx); err != nil {
		return false, err
	}
	if err := checkDestination(); err != nil {
		return false, fail(ErrPublish, err)
	}
	if err := root.Rename(staged, name); err != nil {
		return false, fail(ErrPublish, err)
	}
	return true, nil
}
