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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/minio/minio-go/v7"
)

// ReadRequest uses an exact inclusive-start, byte-count range. Length zero
// requests the full object and requires Offset zero. Suffix/open-ended ranges
// and Seek/ReadAt handles are not exposed. ExpectedSHA256, when supplied, is
// lowercase hex over the requested bytes (the range, not the entire object).
type ReadRequest struct {
	private
	Address        Address
	Offset         int64
	Length         int64
	MatchETag      string
	ExpectedSHA256 string
}

func validETag(value string) bool {
	return validText(value, 256, true) && !strings.ContainsAny(value, "\"\\")
}
func validDigest(value string) bool {
	if value == "" {
		return true
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && strings.ToLower(value) == value
}
func (client *Client) readOptions(request ReadRequest, maximum int64) (native.GetObjectOptions, error) {
	if err := client.address(request.Address, false); err != nil {
		return native.GetObjectOptions{}, err
	}
	if request.Offset < 0 || request.Length < 0 || request.Length > maximum ||
		request.Length == 0 && request.Offset != 0 || request.Offset > 1<<62 ||
		!validETag(request.MatchETag) || !validDigest(request.ExpectedSHA256) {
		return native.GetObjectOptions{}, failure(ErrInput, "read")
	}
	opts := native.GetObjectOptions{VersionID: request.Address.VersionID, Checksum: true}
	if request.MatchETag != "" {
		_ = opts.SetMatchETag(request.MatchETag)
	}
	if request.Length > 0 {
		// SetRange(start, 0) means open-ended for positive start; an exact one-byte
		// range starting at zero remains correctly encoded by the native method.
		if err := opts.SetRange(request.Offset, request.Offset+request.Length-1); err != nil {
			return opts, err
		}
	}
	return opts, nil
}

// Stat preserves version/delete-marker metadata returned alongside native errors.
func (client *Client) Stat(ctx context.Context, id fault.Correlation, address Address) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx); err != nil {
		return nil, err
	}
	if err := client.address(address, false); err != nil {
		return nil, err
	}
	return client.start(ctx, id, "stat", false, func(work context.Context, state *exchange, data *resultData) (error, error) {
		info, err := client.owner.native.StatObject(work, client.owner.settings.Bucket, address.Key, native.StatObjectOptions{VersionID: address.VersionID, Checksum: true})
		data.object = objectInfo(info)
		data.objectPresent = err == nil || info.VersionID != "" || info.IsDeleteMarker
		if err == nil && address.VersionID != "" && info.VersionID != address.VersionID {
			err = ErrProtocol
		}
		data.complete = err == nil
		return nativeFailure(ErrRead, "stat", work, err), nil
	})
}

// Read retains at most MaxReadBytes in immutable independent evidence. A partial
// body and an error can coexist; successful empty data is a non-nil empty slice.
func (client *Client) Read(ctx context.Context, id fault.Correlation, request ReadRequest) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx); err != nil {
		return nil, err
	}
	opts, err := client.readOptions(request, int64(client.owner.settings.MaxReadBytes))
	if err != nil {
		return nil, err
	}
	return client.start(ctx, id, "read", true, func(work context.Context, state *exchange, data *resultData) (error, error) {
		var buffer bytes.Buffer
		err := client.download(work, state, request, opts, int64(client.owner.settings.MaxReadBytes), &buffer, data)
		data.content = buffer.Bytes()
		if data.content == nil && data.transfer.Complete {
			data.content = []byte{}
		}
		return err, nil
	})
}

// Download borrows destination until receipt release and never closes it.
// Writer failures preserve accepted byte count and partial external sink effects.
// Cancellation cannot interrupt a non-cooperating Writer; source use remains held.
func (client *Client) Download(ctx context.Context, id fault.Correlation, request ReadRequest, destination io.Writer) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx); err != nil {
		return nil, err
	}
	if destination == nil {
		return nil, failure(ErrInput, "destination")
	}
	opts, err := client.readOptions(request, client.owner.settings.MaxTransferBytes)
	if err != nil {
		return nil, err
	}
	return client.start(ctx, id, "download", true, func(work context.Context, state *exchange, data *resultData) (error, error) {
		return client.download(work, state, request, opts, client.owner.settings.MaxTransferBytes, destination, data), nil
	})
}
func (client *Client) download(ctx context.Context, state *exchange, request ReadRequest, opts native.GetObjectOptions, maximum int64, destination io.Writer, data *resultData) error {
	state.payloadLimit = maximum
	body, info, headers, err := client.owner.native.GetObject(ctx, client.owner.settings.Bucket, request.Address.Key, opts)
	data.object = objectInfo(info)
	data.objectPresent = err == nil
	if err != nil {
		return nativeFailure(ErrRead, "get", ctx, err)
	}
	defer body.Close()
	if request.Address.VersionID != "" && info.VersionID != request.Address.VersionID {
		return failure(ErrProtocol, "version")
	}
	if request.Length > 0 {
		var start, end, total int64
		if count, scanErr := fmt.Sscanf(headers.Get("Content-Range"), "bytes %d-%d/%d", &start, &end, &total); scanErr != nil || count != 3 ||
			start != request.Offset || end != request.Offset+request.Length-1 || total <= end || info.Size != request.Length {
			return failure(ErrProtocol, "range")
		}
	} else if headers.Get("Content-Range") != "" {
		return failure(ErrProtocol, "unexpected-range")
	}
	if info.Size < 0 || info.Size > maximum {
		return failure(ErrLimit, "object-size")
	}
	hash := sha256.New()
	buffer := make([]byte, 32<<10)
	var primary error
	emptyReads := 0
	for {
		if err := ctx.Err(); err != nil {
			primary = err
			break
		}
		count, readErr := body.Read(buffer)
		if readErr != nil && readErr != io.EOF {
			primary = readErr
		}
		if count > 0 {
			emptyReads = 0
			if data.transfer.Bytes+int64(count) > maximum {
				primary = errors.Join(primary, ErrLimit)
				break
			}
			data.transfer.Effect = Unknown
			written, writeErr := destination.Write(buffer[:count])
			if written < 0 || written > count {
				primary = errors.Join(primary, ErrInput)
				break
			}
			_, _ = hash.Write(buffer[:written])
			data.transfer.Bytes += int64(written)
			if writeErr != nil {
				primary = errors.Join(primary, writeErr)
				break
			}
			if written != count {
				primary = errors.Join(primary, io.ErrShortWrite)
				break
			}
		} else {
			emptyReads++
			if emptyReads >= 100 && readErr == nil {
				readErr = io.ErrNoProgress
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				data.transfer.Complete = true
			} else {
				primary = readErr
			}
			break
		}
	}
	data.transfer.SHA256 = hex.EncodeToString(hash.Sum(nil))
	if primary == nil && data.transfer.Bytes != info.Size {
		primary = io.ErrUnexpectedEOF
		data.transfer.Complete = false
	}
	if primary == nil && request.ExpectedSHA256 != "" {
		if data.transfer.SHA256 != request.ExpectedSHA256 {
			primary = ErrIntegrity
		} else {
			data.transfer.Verified = true
		}
	}
	data.complete = primary == nil && data.transfer.Complete
	// A successful download acknowledges consumption only, not persistence of an
	// arbitrary destination Writer. Its partial effects remain unknown on error.
	if data.complete {
		data.transfer.Effect = Acknowledged
	}
	return nativeFailure(ErrRead, "consume", ctx, primary)
}
