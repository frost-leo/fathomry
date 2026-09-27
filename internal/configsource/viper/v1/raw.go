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

package viper

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"unicode/utf8"
)

// RawFile reuses the owned bounded file path without requiring native parsing.
// A nil result with missing=true is positive OS absence; empty present bytes are
// successful raw content. Close/read errors never become successful absence.
func RawFile(ctx context.Context, path string, limit int) (raw []byte, missing bool, err error) {
	if ctx == nil || !validPath(path) || !utf8.ValidString(path) || strings.ContainsRune(path, 0) || limit < 0 || limit > MaxDocumentBytes {
		return nil, false, fail(ErrInput, "raw-file")
	}
	if ctx.Err() != nil {
		return nil, false, fail(ErrRead, "raw-file", ctx.Err(), context.Cause(ctx))
	}
	raw, err = readFile(ctx, path, limit)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) && !errors.Is(err, ErrClose) {
			return nil, true, nil
		}
		return nil, false, err
	}
	if !utf8.Valid(raw) {
		return nil, false, fail(ErrDecode, "raw-utf8")
	}
	return raw, false, nil
}
