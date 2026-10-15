/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package lark

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"strings"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// AssetInput uses exactly one of Bytes or Reader. Reader is borrowed until
// actual return, is never closed, and must cooperate with caller cancellation.
// ImageType is message/avatar. Duration preserves omitted versus explicit zero.
type AssetInput struct {
	private
	Name, FileType, ImageType string
	Duration                  *int
	Bytes                     []byte
	Reader                    io.Reader
}

// UploadAsset acquires a bounded input under the operation reservation before
// native multipart encoding. Source/evidence stay retained through a late Reader.
func (client *Client) UploadAsset(ctx context.Context, id fault.Correlation, image bool, input AssetInput, options RequestOptions) (*invocation.Receipt[Result], error) {
	if err := client.ready("upload", false); err != nil {
		return nil, err
	}
	if input.Name == "" || len(input.Name) > 255 || filepath.Base(input.Name) != input.Name || strings.ContainsAny(input.Name, "\\\r\n\x00") ||
		(input.Reader == nil) == (input.Bytes == nil) {
		return nil, failure(ErrInput, "asset")
	}
	if input.Duration != nil && (*input.Duration < 0 || *input.Duration > 2147483647) {
		return nil, failure(ErrInput, "duration")
	}
	operation, path, key := "upload-file", "/open-apis/im/v1/files", "file_key"
	if image {
		if input.ImageType != "message" && input.ImageType != "avatar" || input.FileType != "" || input.Duration != nil {
			return nil, failure(ErrInput, "image")
		}
		operation, path, key = "upload-image", "/open-apis/im/v1/images", "image_key"
	} else {
		switch input.FileType {
		case "opus", "mp4", "pdf", "doc", "xls", "ppt", "stream":
		default:
			return nil, failure(ErrInput, "file-type")
		}
		if input.ImageType != "" {
			return nil, failure(ErrInput, "file")
		}
	}
	options, err := freezeRequestOptions(options)
	if err != nil {
		return nil, err
	}
	limit := client.owner.settings.MaxAssetBytes
	if image {
		limit = min(limit, 10<<20)
	}
	return client.execute(ctx, id, requestSpec{operation: operation, path: path, method: "POST", ackKey: key, requestID: options.RequestID, headers: options.Headers,
		readInput: func(work context.Context) (*Upload, error) {
			read := func() (*Upload, error) {
				reader := input.Reader
				if reader == nil {
					if len(input.Bytes) > limit {
						return nil, failure(ErrLimit, "asset")
					}
					reader = bytes.NewReader(input.Bytes)
				}
				data, readErr := io.ReadAll(io.LimitReader(assetReader{ctx: work, reader: reader}, int64(limit)+1))
				if readErr != nil {
					return nil, failure(ErrInput, "asset-read", readErr)
				}
				if work.Err() != nil {
					return nil, failure(ErrCall, "asset-read", work.Err(), context.Cause(work))
				}
				if len(data) == 0 || len(data) > limit {
					return nil, failure(ErrLimit, "asset")
				}
				return &Upload{Name: input.Name, Type: input.FileType, ImageType: input.ImageType, DurationValue: input.Duration, Data: data}, nil
			}
			if input.Reader != nil {
				return callBorrowed(work, ErrInput, "asset-reader-exit", read)
			}
			return read()
		}})
}

type assetReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader assetReader) Read(data []byte) (int, error) {
	if reader.ctx.Err() != nil {
		return 0, reader.ctx.Err()
	}
	return reader.reader.Read(data)
}
