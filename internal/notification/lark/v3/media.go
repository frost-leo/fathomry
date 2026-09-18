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

package lark

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

// Upload borrows Data for the synchronous method only; do not mutate it until
// return. No readers, paths or goroutines escape. File Type is opus, mp4, pdf,
// doc, xls, ppt or stream; Duration is milliseconds for audio/video, zero omitted.
// Image upload always uses image_type=message, not avatar administration.
type Upload struct {
	private
	Name, Type string
	Duration   int
	Data       []byte
}

func (client *Client) upload(ctx context.Context, id fault.Correlation, value Upload, image bool) (*invocation.Receipt[Result], error) {
	if err := client.ready("upload", false); err != nil {
		return nil, err
	}
	if len(value.Data) == 0 || len(value.Data) > client.owner.settings.MaxAssetBytes {
		return nil, failure(ErrLimit, "asset")
	}
	if value.Name == "" || len(value.Name) > 255 || filepath.Base(value.Name) != value.Name || strings.ContainsAny(value.Name, "\\\r\n\x00") || value.Duration < 0 || value.Duration > 2147483647 {
		return nil, failure(ErrInput, "asset")
	}
	operation, path, key := "upload-file", "/open-apis/im/v1/files", "file_key"
	if image {
		if len(value.Data) > 10<<20 {
			return nil, failure(ErrLimit, "image")
		}
		if value.Type != "" || value.Duration != 0 {
			return nil, failure(ErrInput, "image")
		}
		operation, path, key = "upload-image", "/open-apis/im/v1/images", "image_key"
	} else {
		switch value.Type {
		case "opus", "mp4", "pdf", "doc", "xls", "ppt", "stream":
		default:
			return nil, failure(ErrUnsupported, "file-type")
		}
	}
	return client.execute(ctx, id, requestSpec{operation: operation, method: "POST", path: path, ackKey: key, bodyBytes: len(value.Data), upload: &value})
}

// UploadImage uploads a chart/image asset under this app's ownership. Sending is
// a separate call: an upload can remain accepted after later delivery fails.
func (client *Client) UploadImage(ctx context.Context, id fault.Correlation, name string, data []byte) (*invocation.Receipt[Result], error) {
	return client.upload(ctx, id, Upload{Name: name, Data: data}, true)
}

// UploadFile uploads an explicitly typed attachment or audio/video resource.
func (client *Client) UploadFile(ctx context.Context, id fault.Correlation, value Upload) (*invocation.Receipt[Result], error) {
	return client.upload(ctx, id, value, false)
}

// DownloadImage retrieves an image uploaded by the same application. Message
// attachments received from others use DownloadResource with the message ID.
func (client *Client) DownloadImage(ctx context.Context, id fault.Correlation, key string) (*invocation.Receipt[Result], error) {
	if !identifier(key) {
		return nil, failure(ErrInput, "image-key")
	}
	return client.execute(ctx, id, requestSpec{operation: "download-image", method: "GET", path: "/open-apis/im/v1/images/" + key, target: key, download: true})
}

// DownloadFile retrieves a same-application file with a bounded in-memory result.
func (client *Client) DownloadFile(ctx context.Context, id fault.Correlation, key string) (*invocation.Receipt[Result], error) {
	if !identifier(key) {
		return nil, failure(ErrInput, "file-key")
	}
	return client.execute(ctx, id, requestSpec{operation: "download-file", method: "GET", path: "/open-apis/im/v1/files/" + key, target: key, download: true})
}

// DownloadResource reads an image/file belonging to an accessible message. It
// cannot confer access to an expired or different-application resource.
func (client *Client) DownloadResource(ctx context.Context, id fault.Correlation, messageID, key, kind string) (*invocation.Receipt[Result], error) {
	path, err := messagePath(messageID)
	if err != nil {
		return nil, err
	}
	if !identifier(key) || kind != "image" && kind != "file" {
		return nil, failure(ErrInput, "resource")
	}
	return client.execute(ctx, id, requestSpec{operation: "download-resource", method: "GET", path: path + "/resources/" + key,
		query: larkcore.QueryParams{"type": {kind}}, target: messageID, download: true})
}
