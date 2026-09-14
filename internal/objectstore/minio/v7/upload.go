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
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"io"
	"maps"
	"net/http"
	"strings"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/encrypt"
)

// WriteRequest always specifies Size: -1 means unknown, zero means empty.
// IfAbsent and MatchETag are mutually exclusive final-object conditions.
// Metadata keys are lowercase ASCII tokens WITHOUT an x-amz-meta- prefix;
// the integration adds that prefix and cannot inject reserved native headers.
// Encryption is empty (server defaults) or "SSE-S3" (HTTPS only); no downgrade.
type WriteRequest struct {
	private
	Key         string
	Size        int64
	IfAbsent    bool
	MatchETag   string
	ContentType string
	Metadata    map[string]string
	Encryption  string
}

func validMetadata(metadata map[string]string) bool {
	if len(metadata) > 32 {
		return false
	}
	size := 0
	for key, value := range metadata {
		if len(key) < 1 || len(key) > 128 || strings.HasPrefix(key, "x-amz-") || strings.HasPrefix(key, "x-minio-") {
			return false
		}
		for _, char := range key {
			if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '-' || char == '_') {
				return false
			}
		}
		if !validText(value, 2048, true) {
			return false
		}
		size += len(key) + len(value)
	}
	return size <= 8192
}
func (client *Client) writeOptions(request WriteRequest) (native.PutObjectOptions, error) {
	if err := client.address(Address{Key: request.Key}, true); err != nil {
		return native.PutObjectOptions{}, err
	}
	if request.Size < -1 || request.Size > client.owner.settings.MaxTransferBytes ||
		!validETag(request.MatchETag) || request.IfAbsent && request.MatchETag != "" ||
		!validText(request.ContentType, 256, true) || !validMetadata(request.Metadata) {
		return native.PutObjectOptions{}, failure(ErrInput, "write")
	}
	if request.Encryption != "" && request.Encryption != "SSE-S3" || request.Encryption != "" && client.owner.settings.Plaintext {
		return native.PutObjectOptions{}, failure(ErrUnsupported, "encryption")
	}
	opts := native.PutObjectOptions{ContentType: request.ContentType, DisableContentSha256: true, UserMetadata: make(map[string]string, len(request.Metadata))}
	for key, value := range request.Metadata {
		opts.UserMetadata["x-amz-meta-"+key] = value
	}
	if request.IfAbsent {
		opts.SetMatchETagExcept("*")
	}
	if request.MatchETag != "" {
		opts.SetMatchETag(request.MatchETag)
	}
	if request.Encryption == "SSE-S3" {
		opts.ServerSideEncryption = encrypt.NewSSE()
	}
	return opts, nil
}

// Put borrows input until receipt release. It never closes the reader. Small
// input is checked before a single PUT; multipart buffers one serial part.
// Exact-size validation consumes at most one extra byte to reject trailing data.
// A separately caller-owned cleanup context authorizes one abort after a known
// upload ID fails. Cancellation/abort never converts unknown effects to absence.
func (client *Client) Put(ctx, cleanupCtx context.Context, id fault.Correlation, request WriteRequest, input io.Reader) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx); err != nil {
		return nil, err
	}
	if cleanupCtx == nil || input == nil {
		return nil, failure(ErrInput, "put")
	}
	opts, err := client.writeOptions(request)
	if err != nil {
		return nil, err
	}
	return client.start(ctx, id, "put", false, func(work context.Context, state *exchange, data *resultData) (error, error) {
		primary := client.upload(work, state, request, opts, input, data)
		var cleanup error
		if primary != nil && data.transfer.UploadID != "" {
			clean, cancel, err := (invocation.Budget{Limit: client.owner.settings.CleanupTimeout}).Context(cleanupCtx, invocation.Cleanup)
			if err == nil {
				data.transfer.AbortAttempted = true
				cleanupWork := context.WithValue(controlledContext(clean, state), cleanupKey{}, true)
				err = client.owner.native.AbortMultipartUpload(cleanupWork, client.owner.settings.Bucket, request.Key, data.transfer.UploadID)
				cancel()
				data.transfer.AbortAcknowledged = err == nil
			}
			cleanup = nativeFailure(ErrCleanup, "abort", cleanupCtx, err)
		}
		return nativeFailure(ErrWrite, "put", work, primary), cleanup
	})
}
func readInput(ctx context.Context, input io.Reader, buffer []byte) (int, error) {
	total, empty := 0, 0
	for total < len(buffer) {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		count, err := input.Read(buffer[total:])
		if count < 0 || count > len(buffer)-total {
			return total, ErrInput
		}
		total += count
		if err != nil {
			return total, err
		}
		if count == 0 {
			empty++
			if empty >= 100 {
				return total, io.ErrNoProgress
			}
		} else {
			empty = 0
		}
	}
	return total, nil
}
func hashes(buffer []byte) (string, string) {
	md5sum := md5.Sum(buffer)
	sha := sha256.Sum256(buffer)
	return base64.StdEncoding.EncodeToString(md5sum[:]), hex.EncodeToString(sha[:])
}

func (response *controlResponse) allocationIdentity(bucket, key, uploadID string) (bool, error) {
	if response.statusCode != http.StatusOK {
		return false, nil
	}
	var allocation struct {
		XMLName xml.Name `xml:"InitiateMultipartUploadResult"`
		Bucket  []string
		Key     []string
		ID      []string `xml:"UploadId"`
	}
	decoder := xml.NewDecoder(bytes.NewReader(response.body.Bytes()))
	decodeErr := decoder.Decode(&allocation)
	if allocation.XMLName.Local != "InitiateMultipartUploadResult" || len(allocation.Bucket) != 1 || allocation.Bucket[0] != bucket ||
		len(allocation.Key) != 1 || allocation.Key[0] != key || len(allocation.ID) != 1 || allocation.ID[0] != uploadID || !validText(uploadID, 1024, false) {
		return false, failure(ErrProtocol, "upload-allocation", decodeErr)
	}
	if decodeErr != nil {
		return true, failure(ErrProtocol, "upload-allocation", decodeErr)
	}
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return false, failure(ErrProtocol, "upload-allocation", err)
		}
		switch token := token.(type) {
		case xml.CharData:
			if len(bytes.TrimSpace(token)) != 0 {
				return false, failure(ErrProtocol, "upload-allocation")
			}
		case xml.Comment, xml.ProcInst:
		default:
			return false, failure(ErrProtocol, "upload-allocation")
		}
	}
	if response.readErr != io.EOF {
		return true, failure(ErrProtocol, "upload-allocation", response.readErr)
	}
	return true, nil
}

func (client *Client) upload(ctx context.Context, state *exchange, request WriteRequest, opts native.PutObjectOptions, input io.Reader, data *resultData) error {
	value := client.owner.settings
	buffer := make([]byte, value.PartBytes+1)
	hash := sha256.New()
	var completed []native.CompletePart
	eof := false
	for {
		capacity := value.PartBytes
		remaining := value.MaxTransferBytes - data.transfer.Bytes
		if request.Size >= 0 && request.Size-data.transfer.Bytes < remaining {
			remaining = request.Size - data.transfer.Bytes
		}
		if int64(capacity) > remaining {
			capacity = int(remaining) + 1
		}
		count, readErr := readInput(ctx, input, buffer[:capacity])
		data.transfer.Bytes += int64(count)
		_, _ = hash.Write(buffer[:count])
		data.transfer.SHA256 = hex.EncodeToString(hash.Sum(nil))
		if data.transfer.Bytes > value.MaxTransferBytes {
			return ErrLimit
		}
		if request.Size >= 0 && data.transfer.Bytes > request.Size {
			return ErrInput
		}
		if readErr != nil && readErr != io.EOF {
			return readErr
		}
		eof = readErr == io.EOF
		if eof && request.Size >= 0 && data.transfer.Bytes != request.Size {
			return io.ErrUnexpectedEOF
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if data.transfer.UploadID == "" && eof {
			md5sum, sha := hashes(buffer[:count])
			data.transfer.Effect = Unknown
			info, err := client.owner.native.PutObject(ctx, value.Bucket, request.Key, bytes.NewReader(buffer[:count]), int64(count), md5sum, sha, opts)
			if err != nil {
				return err
			}
			if info.ETag == "" || !validETag(info.ETag) {
				return ErrProtocol
			}
			info.Size = int64(count)
			data.object = uploadInfo(info)
			data.objectPresent = true
			data.transfer.Effect = Acknowledged
			data.transfer.Complete = true
			data.complete = true
			return nil
		}
		if data.transfer.UploadID == "" {
			// Conditions apply at publication, not upload-ID allocation.
			initial := native.PutObjectOptions{ContentType: opts.ContentType, UserMetadata: maps.Clone(opts.UserMetadata), ServerSideEncryption: opts.ServerSideEncryption}
			data.transfer.Effect = Unknown
			capture := &controlResponseCapture{}
			uploadID, err := client.owner.native.NewMultipartUpload(context.WithValue(ctx, controlResponseKey{}, capture), value.Bucket, request.Key, initial)
			if len(capture.pages) != 1 {
				return errors.Join(err, failure(ErrProtocol, "upload-allocation"))
			}
			// Core exposes only the ID. Confirm its allocation identity before
			// adopting even a partial-response ID for caller-authorized cleanup.
			owned, allocationErr := capture.pages[0].allocationIdentity(value.Bucket, request.Key, uploadID)
			if owned {
				data.transfer.UploadID = uploadID
			}
			err = errors.Join(err, allocationErr)
			if err != nil {
				return err
			}
			if data.transfer.UploadID == "" {
				return ErrProtocol
			}
		}
		if count > 0 {
			if len(completed) >= value.MaxParts {
				return ErrLimit
			}
			md5sum, sha := hashes(buffer[:count])
			part, err := client.owner.native.PutObjectPart(ctx, value.Bucket, request.Key, data.transfer.UploadID, len(completed)+1, bytes.NewReader(buffer[:count]), int64(count),
				native.PutObjectPartOptions{Md5Base64: md5sum, Sha256Hex: sha, DisableContentSha256: true, SSE: opts.ServerSideEncryption})
			if err != nil {
				return err
			}
			if !validETag(part.ETag) || part.ETag == "" {
				return ErrProtocol
			}
			completed = append(completed, native.CompletePart{PartNumber: len(completed) + 1, ETag: part.ETag})
			data.transfer.PartsAcknowledged++
		}
		if eof {
			break
		}
	}
	data.transfer.CompletionAttempted = true
	info, err := client.owner.native.CompleteMultipartUpload(ctx, value.Bucket, request.Key, data.transfer.UploadID, completed, opts)
	if err != nil {
		return err
	}
	if info.ETag == "" || !validETag(info.ETag) || info.Bucket != value.Bucket || info.Key != request.Key {
		return ErrProtocol
	}
	info.Size = data.transfer.Bytes
	data.object = uploadInfo(info)
	data.objectPresent = true
	data.transfer.Effect = Acknowledged
	data.transfer.Complete = true
	data.complete = true
	return nil
}
