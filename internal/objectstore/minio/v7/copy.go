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

package minio

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/url"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/minio/minio-go/v7"
)

// CopyRequest copies one complete source in this same bucket/prefix.
// Destination conditions are explicitly rejected: v7.3.0's selected CopyObject
// path cannot carry them. Metadata/tags follow native COPY semantics.
// No ComposeObject, multipart copy, ranges or cross-bucket authority is exposed.
type CopyRequest struct {
	private
	Source               Address
	Key                  string
	MatchETag            string
	IfAbsent             bool
	DestinationMatchETag string
}

func (response *controlResponse) copyFailure() error {
	if response.statusCode != http.StatusOK {
		return nil
	}
	if response.readErr != io.EOF {
		return failure(ErrProtocol, "copy-response", response.readErr)
	}
	var root struct{ XMLName xml.Name }
	decoder := xml.NewDecoder(bytes.NewReader(response.body.Bytes()))
	if err := decoder.Decode(&root); err != nil {
		return failure(ErrProtocol, "copy-response", err)
	}
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return failure(ErrProtocol, "copy-response", err)
		}
		switch token := token.(type) {
		case xml.CharData:
			if len(bytes.TrimSpace(token)) != 0 {
				return failure(ErrProtocol, "copy-response")
			}
		case xml.Comment, xml.ProcInst:
		default:
			return failure(ErrProtocol, "copy-response")
		}
	}
	switch root.XMLName.Local {
	case "CopyObjectResult":
		return nil
	case "Error":
		nativeErr := native.ErrorResponse{StatusCode: response.statusCode, Server: response.server}
		if err := xml.Unmarshal(response.body.Bytes(), &nativeErr); err != nil {
			return failure(ErrProtocol, "copy-response", err)
		}
		if nativeErr.Code == "" {
			return failure(ErrProtocol, "copy-response", nativeErr)
		}
		return nativeErr
	default:
		return failure(ErrProtocol, "copy-response")
	}
}

func (client *Client) Copy(ctx context.Context, id fault.Correlation, request CopyRequest) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx); err != nil {
		return nil, err
	}
	if err := client.address(request.Source, false); err != nil {
		return nil, err
	}
	if err := client.address(Address{Key: request.Key}, true); err != nil {
		return nil, err
	}
	if request.IfAbsent || request.DestinationMatchETag != "" {
		return nil, failure(ErrUnsupported, "copy-condition")
	}
	if !validETag(request.MatchETag) {
		return nil, failure(ErrInput, "copy-condition")
	}
	return client.start(ctx, id, "copy", false, func(work context.Context, state *exchange, data *resultData) (error, error) {
		opts := native.StatObjectOptions{VersionID: request.Source.VersionID}
		if request.MatchETag != "" {
			_ = opts.SetMatchETag(request.MatchETag)
		}
		info, err := client.owner.native.StatObject(work, client.owner.settings.Bucket, request.Source.Key, opts)
		if err != nil {
			return nativeFailure(ErrRead, "copy-source", work, err), nil
		}
		if info.Size < 0 || info.Size > client.owner.settings.MaxTransferBytes || info.Size > 5<<30 {
			return failure(ErrLimit, "copy-size"), nil
		}
		if info.ETag == "" || !validETag(info.ETag) || request.Source.VersionID != "" && info.VersionID != request.Source.VersionID {
			return failure(ErrProtocol, "copy-source"), nil
		}
		if err := work.Err(); err != nil {
			return nativeFailure(ErrWrite, "copy", work, err), nil
		}
		data.transfer.Effect = Unknown
		// v7.3.0 decodes HTTP 200 Error bodies as an empty successful copy DTO.
		// Observe only this call's already-bounded response without changing status.
		capture := &controlResponseCapture{}
		copied, err := client.owner.native.Client.CopyObject(context.WithValue(work, controlResponseKey{}, capture), native.CopyDestOptions{Bucket: client.owner.settings.Bucket, Object: request.Key},
			native.CopySrcOptions{Bucket: client.owner.settings.Bucket, Object: request.Source.Key, VersionID: url.QueryEscape(request.Source.VersionID), MatchETag: "\"" + info.ETag + "\""})
		if err == nil && len(capture.pages) != 1 {
			err = ErrProtocol
		}
		for _, response := range capture.pages {
			err = errors.Join(err, response.copyFailure())
		}
		if err == nil && (copied.ETag == "" || !validETag(copied.ETag)) {
			err = ErrProtocol
		}
		if err != nil {
			return nativeFailure(ErrWrite, "copy", work, err), nil
		}
		copied.Size = info.Size
		data.object = uploadInfo(copied)
		data.objectPresent = true
		data.transfer = Transfer{Effect: Acknowledged, Bytes: info.Size, Complete: true}
		data.complete = true
		return nil, nil
	})
}
