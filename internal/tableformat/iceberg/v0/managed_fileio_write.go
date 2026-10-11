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

package iceberg

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"sync"

	nativeio "github.com/apache/iceberg-go/io"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

func (files *managedIO) Create(uri string) (nativeio.FileWriter, error) {
	if !files.config.AllowWrite {
		return nil, failure(ErrAuthority, "file-write")
	}
	key, err := files.key(uri)
	if err != nil {
		return nil, err
	}
	done, err := files.start()
	if err != nil {
		return nil, err
	}
	defer done()
	if err = files.reserveHandle(uri); err != nil {
		return nil, err
	}
	if err = files.reserve("create", uri, 0, false); err != nil {
		files.releaseHandle(nil, uri)
		return nil, err
	}
	writer := &managedWriteFile{files: files, uri: uri, key: key}
	if err = files.addHandle(writer); err != nil {
		files.releaseHandle(nil, uri)
		return nil, err
	}
	return writer, nil
}

func (files *managedIO) WriteFile(uri string, data []byte) error {
	done, err := files.start()
	if err != nil {
		return err
	}
	defer done()
	writer, err := files.Create(uri)
	if err != nil {
		return err
	}
	if _, err = writer.Write(data); err != nil {
		return errors.Join(err, writer.(*managedWriteFile).shutdown())
	}
	return writer.Close()
}

// Each writer holds at most min(PartSize, MaxObjectBytes) bytes and uploads one
// part at a time. The admitted envelope includes MaxOpenFiles such buffers.
// Close commits; family shutdown never commits a writer the caller left open.
type managedWriteFile struct {
	files              *managedIO
	uri, key, uploadID string
	mu                 sync.Mutex
	closed             bool
	err                error
	buffer             []byte
	total              int64
	parts              []types.CompletedPart
	completeSubmitted  bool
	abortReserved      bool
}

type managedIOCleanupKey struct{}

func (writer *managedWriteFile) releaseBuffer() {
	capacity := cap(writer.buffer)
	writer.buffer = nil
	writer.parts = nil
	if capacity != 0 && writer.files.config.Check != nil {
		writer.files.note(writer.files.config.Check("buffer-release", writer.uri, int64(capacity)))
	}
}

func (writer *managedWriteFile) Write(data []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.closed {
		return 0, fs.ErrClosed
	}
	if writer.err != nil {
		return 0, writer.err
	}
	done, err := writer.files.start()
	if err != nil {
		writer.err = err
		return 0, err
	}
	defer done()
	if int64(len(data)) > writer.files.config.MaxObjectBytes-writer.total {
		writer.err = failure(ErrLimit, "object-bytes")
		return 0, writer.err
	}
	if err = writer.files.reserve("write", writer.uri, int64(len(data)), false); err != nil {
		writer.err = err
		return 0, err
	}
	if len(data) == 0 {
		return 0, nil
	}
	if writer.buffer == nil {
		capacity := int(min(int64(writer.files.config.PartSize), writer.files.config.MaxObjectBytes))
		if check := writer.files.config.Check; check != nil {
			if err = check("buffer", writer.uri, int64(capacity)); err != nil {
				writer.err = err
				return 0, err
			}
		}
		writer.buffer = make([]byte, 0, capacity)
	}
	written := 0
	for len(data) != 0 {
		if len(writer.buffer) == cap(writer.buffer) {
			if err = writer.flushPart(); err != nil {
				writer.err = err
				return written, err
			}
		}
		count := min(cap(writer.buffer)-len(writer.buffer), len(data))
		writer.buffer = append(writer.buffer, data[:count]...)
		data = data[count:]
		written += count
		writer.total += int64(count)
	}
	return written, nil
}

func (writer *managedWriteFile) ReadFrom(reader io.Reader) (int64, error) {
	done, err := writer.files.start()
	if err != nil {
		return 0, err
	}
	defer done()
	return io.Copy(writerOnly{writer}, reader)
}

func (writer *managedWriteFile) event(operation string, effect Effect, count int64, err error) {
	writer.files.observe(managedIOEvent{Operation: operation, URI: writer.uri, UploadID: writer.uploadID, Bytes: count, Effect: effect, Err: err})
}

func (writer *managedWriteFile) flushPart() error {
	if len(writer.parts) >= 10000 {
		return failure(ErrLimit, "multipart-parts")
	}
	files := writer.files
	if writer.uploadID == "" {
		// Keep one request and its admission for abort before starting an
		// upload. Cleanup must not need new capacity from a saturated family.
		if err := files.reserve("abort-reservation", writer.uri, 0, true); err != nil {
			return err
		}
		writer.abortReserved = true
		if err := files.reserve("multipart-create", writer.uri, 0, true); err != nil {
			return err
		}
		writer.event("multipart-create", Unknown, 0, nil)
		output, err := files.config.Client.CreateMultipartUpload(files.ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(files.bucket), Key: aws.String(writer.key)}, managedNoRetry)
		if err != nil {
			writer.event("multipart-create", managedMutationEffect(err), 0, err)
			return err
		}
		writer.uploadID = aws.ToString(output.UploadId)
		if writer.uploadID == "" {
			return failure(ErrProtocol, "multipart-id")
		}
		writer.event("multipart-create", Acknowledged, 0, nil)
	}
	if err := files.reserve("upload-part", writer.uri, 0, true); err != nil {
		return err
	}
	part := int32(len(writer.parts) + 1)
	writer.event("upload-part", Unknown, int64(len(writer.buffer)), nil)
	output, err := files.config.Client.UploadPart(files.ctx, &s3.UploadPartInput{Bucket: aws.String(files.bucket), Key: aws.String(writer.key),
		UploadId: aws.String(writer.uploadID), PartNumber: aws.Int32(part), ContentLength: aws.Int64(int64(len(writer.buffer))), Body: bytes.NewReader(writer.buffer)}, managedNoRetry)
	if err != nil {
		writer.event("upload-part", managedMutationEffect(err), int64(len(writer.buffer)), err)
		return err
	}
	if aws.ToString(output.ETag) == "" {
		return failure(ErrProtocol, "multipart-part-etag")
	}
	writer.parts = append(writer.parts, types.CompletedPart{ETag: output.ETag, PartNumber: aws.Int32(part)})
	writer.event("upload-part", Acknowledged, int64(len(writer.buffer)), nil)
	writer.buffer = writer.buffer[:0]
	return nil
}

func managedMutationEffect(err error) Effect {
	if err == nil {
		return Acknowledged
	}
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "PreconditionFailed", "AccessDenied", "InvalidRequest", "NoSuchBucket", "NoSuchUpload":
			return Rejected
		}
	}
	return Unknown
}

func (writer *managedWriteFile) Close() error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.closed {
		return writer.err
	}
	writer.closed = true
	defer writer.files.releaseHandle(writer, writer.uri)
	defer writer.releaseBuffer()
	done, err := writer.files.start()
	if err == nil {
		defer done()
	} else {
		writer.err = errors.Join(writer.err, err)
	}
	if writer.err == nil {
		writer.err = writer.commit()
	}
	if writer.err != nil {
		writer.err = errors.Join(writer.err, writer.abort())
	}
	return writer.err
}

func (writer *managedWriteFile) commit() error {
	files := writer.files
	if writer.uploadID == "" {
		if err := files.reserve("put", writer.uri, 0, true); err != nil {
			return err
		}
		writer.event("put", Unknown, writer.total, nil)
		output, err := files.config.Client.PutObject(files.ctx, &s3.PutObjectInput{Bucket: aws.String(files.bucket), Key: aws.String(writer.key),
			Body: bytes.NewReader(writer.buffer), ContentLength: aws.Int64(writer.total), IfNoneMatch: aws.String("*")}, managedNoRetry)
		if err == nil && aws.ToString(output.ETag) == "" {
			err = failure(ErrProtocol, "put-etag")
		}
		writer.event("put", managedMutationEffect(err), writer.total, err)
		return err
	}
	if len(writer.buffer) != 0 {
		if err := writer.flushPart(); err != nil {
			return err
		}
	}
	if err := files.reserve("multipart-complete", writer.uri, 0, true); err != nil {
		return err
	}
	writer.completeSubmitted = true
	writer.event("multipart-complete", Unknown, writer.total, nil)
	output, err := files.config.Client.CompleteMultipartUpload(files.ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String(files.bucket), Key: aws.String(writer.key),
		UploadId: aws.String(writer.uploadID), IfNoneMatch: aws.String("*"), MultipartUpload: &types.CompletedMultipartUpload{Parts: writer.parts}}, managedNoRetry)
	if err == nil && aws.ToString(output.ETag) == "" {
		err = failure(ErrProtocol, "multipart-complete-etag")
	}
	writer.event("multipart-complete", managedMutationEffect(err), writer.total, err)
	return err
}

func (writer *managedWriteFile) abort() error {
	if writer.uploadID == "" || writer.completeSubmitted || !writer.abortReserved {
		return nil
	}
	writer.abortReserved = false
	ctx, cancel := context.WithTimeout(context.WithoutCancel(writer.files.ctx), writer.files.config.CleanupTimeout)
	defer cancel()
	ctx = context.WithValue(ctx, managedIOCleanupKey{}, writer.files)
	writer.event("multipart-abort", Unknown, 0, nil)
	_, err := writer.files.config.Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(writer.files.bucket),
		Key: aws.String(writer.key), UploadId: aws.String(writer.uploadID)}, managedNoRetry)
	writer.event("multipart-abort", managedMutationEffect(err), 0, err)
	writer.files.note(err)
	return err
}

func (writer *managedWriteFile) shutdown() error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.closed {
		return nil
	}
	writer.closed = true
	defer writer.files.releaseHandle(writer, writer.uri)
	defer writer.releaseBuffer()
	return writer.abort()
}

var _ nativeio.WriteFileIO = (*managedIO)(nil)
