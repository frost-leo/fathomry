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

package chromedp

import (
	"context"
	"strings"

	sdk "github.com/chromedp/chromedp"
)

// SetUploadFiles delegates native selector/readiness and file-input behavior.
// Paths belong to the browser host. Nil is invalid; an explicit empty slice keeps
// native behavior and is not a portable clear operation. Use Clear to clear the
// input. Parameters are borrowed and immutable until execution returns.
func SetUploadFiles[S TextSelector, P ~string](selector S, files []P, options ...QueryOption) Action[sdk.Void] {
	options = cloneQueryOptions(options)
	return Action[sdk.Void]{bytes: int64(len(selector)), local: func(ctx context.Context, session *Session) (sdk.Void, error) {
		work, values, err := session.queryWork(ctx, options)
		if err != nil {
			return sdk.Void{}, err
		}
		copied, err := uploadPaths(files, int64(len(selector)), session.owner.settings.MaxCommandBytes)
		if err != nil {
			return sdk.Void{}, err
		}
		return sdk.Run(work, sdk.SetUploadFiles(selector, copied, values...))
	}}
}

func uploadPaths[P ~string](files []P, size, limit int64) ([]string, error) {
	if files == nil {
		return nil, failure(ErrInput, "upload-paths")
	}
	if int64(len(files)) > limit/2 {
		return nil, failure(ErrLimit, "upload-paths")
	}
	for _, file := range files {
		if file == "" || strings.ContainsRune(string(file), '\x00') {
			return nil, failure(ErrInput, "upload-path")
		}
		if int64(len(file)) > limit-size {
			return nil, failure(ErrLimit, "upload-path-bytes")
		}
		size += int64(len(file))
	}
	copied := make([]string, len(files))
	for index, file := range files {
		copied[index] = string(file)
	}
	return copied, nil
}
