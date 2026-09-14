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
	"strings"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/minio/minio-go/v7"
)

// ListRequest selects a bounded recursive listing. Prefix must be within the
// source's prefix. Versions requires the source Versions grant. StartAfter is a
// lexical key boundary, not a stable snapshot or a version continuation token.
type ListRequest struct {
	private
	Prefix     string
	StartAfter string
	Versions   bool
}

func (client *Client) prefix(prefix string) error {
	if !validPath(prefix, true) {
		return failure(ErrInput, "prefix")
	}
	if !strings.HasPrefix(prefix, client.owner.settings.Prefix) {
		return failure(ErrAuthority, "prefix")
	}
	return nil
}

func (response *controlResponse) listFailure(expectedRoot string) error {
	if response.statusCode != http.StatusOK {
		return nil
	}
	if response.readErr != io.EOF {
		return failure(ErrProtocol, "list-response", response.readErr)
	}
	decoder := xml.NewDecoder(bytes.NewReader(response.body.Bytes()))
	var root struct {
		XMLName     xml.Name
		IsTruncated []string
	}
	if err := decoder.Decode(&root); err != nil {
		return failure(ErrProtocol, "list-response", err)
	}
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return failure(ErrProtocol, "list-response", err)
		}
		switch value := token.(type) {
		case xml.Comment:
		case xml.CharData:
			if strings.TrimSpace(string(value)) != "" {
				return failure(ErrProtocol, "list-response")
			}
		default:
			return failure(ErrProtocol, "list-response")
		}
	}
	if root.XMLName.Local == "Error" {
		responseError := native.ErrorResponse{StatusCode: response.statusCode, Server: response.server}
		if err := xml.Unmarshal(response.body.Bytes(), &responseError); err != nil {
			return failure(ErrProtocol, "list-response", err)
		}
		return failure(ErrProtocol, "list-response", responseError)
	}
	if root.XMLName.Local != expectedRoot {
		return failure(ErrProtocol, "list-response")
	}
	if len(root.IsTruncated) != 1 {
		return failure(ErrProtocol, "list-terminal")
	}
	switch strings.TrimSpace(root.IsTruncated[0]) {
	case "true", "false", "1", "0":
	default:
		return failure(ErrProtocol, "list-terminal")
	}
	return nil
}

func listResponsesFailure(capture *controlResponseCapture, after int, expectedRoot string) error {
	var failures []error
	for _, response := range capture.pages[after:] {
		if err := response.listFailure(expectedRoot); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// List consumes the native synchronous iterator, never a channel producer.
// Count, aggregate response bytes, HTTP exchanges and deadline are independent
// limits. A truncated integration result retains partial objects with ErrLimit.
func (client *Client) List(ctx context.Context, id fault.Correlation, request ListRequest) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx); err != nil {
		return nil, err
	}
	if err := client.prefix(request.Prefix); err != nil {
		return nil, err
	}
	if request.StartAfter != "" {
		if err := client.address(Address{Key: request.StartAfter}, false); err != nil {
			return nil, err
		}
	}
	if request.Versions && !client.owner.settings.Versions {
		return nil, failure(ErrUnsupported, "versions")
	}
	return client.start(ctx, id, "list", false, func(work context.Context, state *exchange, data *resultData) (error, error) {
		owner := false
		opts := native.ListObjectsOptions{Prefix: request.Prefix, StartAfter: request.StartAfter, Recursive: true,
			WithVersions: request.Versions, MaxKeys: client.owner.settings.MaxEntries, FetchOwner: &owner}
		capture := &controlResponseCapture{}
		root := "ListBucketResult"
		if request.Versions {
			root = "ListVersionsResult"
		}
		checked := 0
		for info := range client.owner.native.ListObjectsIter(context.WithValue(work, controlResponseKey{}, capture), client.owner.settings.Bucket, opts) {
			responseError := listResponsesFailure(capture, checked, root)
			checked = len(capture.pages)
			if info.Err != nil || responseError != nil {
				return nativeFailure(ErrList, "iterate", work, errors.Join(info.Err, responseError)), nil
			}
			if len(data.objects) >= client.owner.settings.MaxEntries {
				return failure(ErrLimit, "entries"), nil
			}
			if !validPath(info.Key, false) || !strings.HasPrefix(info.Key, request.Prefix) || info.Key <= request.StartAfter || info.Size < 0 ||
				!validText(info.VersionID, 1024, !request.Versions) {
				return failure(ErrProtocol, "list-key"), nil
			}
			data.objects = append(data.objects, objectInfo(info))
		}
		if err := listResponsesFailure(capture, checked, root); err != nil {
			return nativeFailure(ErrList, "iterate", work, err), nil
		}
		if err := work.Err(); err != nil {
			return nativeFailure(ErrList, "iterate", work, err), nil
		}
		data.complete = true
		return nil, nil
	})
}

// UploadQuery is one native multipart-inspection page; markers are opaque data.
type UploadQuery struct {
	private
	Prefix         string
	KeyMarker      string
	UploadIDMarker string
}

// ListUploads inspects one bounded page. Truncated URL-encoded responses with
// an upload marker retain validated entries but return ErrUnsupported: v7.3.0
// changes that opaque marker during decoding. No guessed continuation escapes.
// Native decoding failures retain their cause but omit partially decoded entries.
func (client *Client) ListUploads(ctx context.Context, id fault.Correlation, query UploadQuery) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx); err != nil {
		return nil, err
	}
	if err := client.prefix(query.Prefix); err != nil {
		return nil, err
	}
	if !validPath(query.KeyMarker, true) || !validText(query.UploadIDMarker, 1024, true) || query.UploadIDMarker != "" && query.KeyMarker == "" ||
		query.KeyMarker != "" && !strings.HasPrefix(query.KeyMarker, query.Prefix) {
		return nil, failure(ErrInput, "upload-query")
	}
	return client.start(ctx, id, "list-uploads", false, func(work context.Context, state *exchange, data *resultData) (error, error) {
		capture := &controlResponseCapture{}
		page, err := client.owner.native.ListMultipartUploads(context.WithValue(work, controlResponseKey{}, capture), client.owner.settings.Bucket, query.Prefix, query.KeyMarker, query.UploadIDMarker, "", client.owner.settings.MaxEntries)
		responseError := listResponsesFailure(capture, 0, "ListMultipartUploadsResult")
		if responseError != nil {
			return nativeFailure(ErrList, "uploads", work, errors.Join(err, responseError)), nil
		}
		if err != nil {
			if page.IsTruncated && page.EncodingType == "url" {
				return nativeFailure(ErrUnsupported, "upload-cursor-encoding", work, err), nil
			}
			return nativeFailure(ErrList, "uploads", work, err), nil
		}
		if page.Bucket != client.owner.settings.Bucket {
			return failure(ErrProtocol, "upload-bucket"), nil
		}
		if len(page.CommonPrefixes) > 0 {
			return failure(ErrProtocol, "upload-prefixes"), nil
		}
		for _, upload := range page.Uploads {
			if len(data.uploads) >= client.owner.settings.MaxEntries {
				return failure(ErrLimit, "uploads"), nil
			}
			if !validPath(upload.Key, false) || !strings.HasPrefix(upload.Key, query.Prefix) || !validText(upload.UploadID, 1024, false) {
				return failure(ErrProtocol, "upload-entry"), nil
			}
			data.uploads = append(data.uploads, Upload{Key: upload.Key, ID: upload.UploadID, Initiated: upload.Initiated})
		}
		if !page.IsTruncated {
			data.complete = true
			return nil, nil
		}
		if page.EncodingType == "url" && page.NextUploadIDMarker != "" {
			// The SDK URL-decodes this opaque field even though S3 only encodes
			// key fields. The original marker cannot be recovered without guessing.
			return failure(ErrUnsupported, "upload-cursor-encoding"), nil
		}
		if !validPath(page.NextKeyMarker, true) || page.NextKeyMarker != "" && !strings.HasPrefix(page.NextKeyMarker, query.Prefix) || !validText(page.NextUploadIDMarker, 1024, true) ||
			page.IsTruncated && (page.NextKeyMarker == "" || page.NextKeyMarker == query.KeyMarker && page.NextUploadIDMarker == query.UploadIDMarker) {
			return failure(ErrProtocol, "upload-cursor"), nil
		}
		data.nextKey = page.NextKeyMarker
		data.nextUpload = page.NextUploadIDMarker
		data.complete = !page.IsTruncated
		return nil, nil
	})
}
func (client *Client) validUpload(upload Upload, write bool) error {
	if err := client.address(Address{Key: upload.Key}, write); err != nil {
		return err
	}
	if !validText(upload.ID, 1024, false) {
		return failure(ErrInput, "upload-id")
	}
	return nil
}

// ListParts returns one bounded native page, with the next marker kept explicit.
func (client *Client) ListParts(ctx context.Context, id fault.Correlation, upload Upload, after int) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx); err != nil {
		return nil, err
	}
	if err := client.validUpload(upload, false); err != nil {
		return nil, err
	}
	if after < 0 || after > 10000 {
		return nil, failure(ErrInput, "part-marker")
	}
	return client.start(ctx, id, "list-parts", false, func(work context.Context, state *exchange, data *resultData) (error, error) {
		capture := &controlResponseCapture{}
		page, err := client.owner.native.ListObjectParts(context.WithValue(work, controlResponseKey{}, capture), client.owner.settings.Bucket, upload.Key, upload.ID, after, client.owner.settings.MaxEntries)
		err = errors.Join(err, listResponsesFailure(capture, 0, "ListPartsResult"))
		if err != nil {
			return nativeFailure(ErrList, "parts", work, err), nil
		}
		if page.Bucket != client.owner.settings.Bucket || page.Key != upload.Key || page.UploadID != upload.ID {
			return failure(ErrProtocol, "part-upload"), nil
		}
		previous := after
		for _, part := range page.ObjectParts {
			if len(data.parts) >= client.owner.settings.MaxEntries {
				return failure(ErrLimit, "parts"), nil
			}
			if part.PartNumber <= previous || part.PartNumber > 10000 || part.Size < 0 {
				return failure(ErrProtocol, "part-order"), nil
			}
			previous = part.PartNumber
			data.parts = append(data.parts, part)
		}
		if page.IsTruncated && (len(data.parts) == 0 || page.NextPartNumberMarker != previous) {
			return failure(ErrProtocol, "part-cursor"), nil
		}
		if page.IsTruncated {
			data.nextPart = page.NextPartNumberMarker
		}
		data.complete = !page.IsTruncated
		return nil, nil
	})
}

// Abort performs one explicitly requested abort. Composition must establish
// ownership/authority for this exact upload ID; listing does not establish it.
func (client *Client) Abort(ctx context.Context, id fault.Correlation, upload Upload) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx); err != nil {
		return nil, err
	}
	if err := client.validUpload(upload, true); err != nil {
		return nil, err
	}
	return client.start(ctx, id, "abort", false, func(work context.Context, state *exchange, data *resultData) (error, error) {
		data.transfer = Transfer{UploadID: upload.ID, Effect: Unknown, AbortAttempted: true}
		err := client.owner.native.AbortMultipartUpload(work, client.owner.settings.Bucket, upload.Key, upload.ID)
		if err == nil {
			data.transfer.Effect = Acknowledged
			data.transfer.AbortAcknowledged = true
			data.complete = true
		}
		return nativeFailure(ErrCleanup, "abort", work, err), nil
	})
}
