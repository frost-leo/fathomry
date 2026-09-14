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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	nativeio "github.com/apache/iceberg-go/io"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/frost-leo/fathomry/internal/fault"
)

// fileIO deliberately implements no walking, bulk deletion, or raw SDK escape.
// This provider owns its S3 client, transport, buffered handles and byte accounting.
type fileIO struct {
	ctx   context.Context
	state *exchange
}

func (files *fileIO) key(uri string) (string, error) {
	if len(uri) > 2048 {
		return "", failure(ErrLimit, "file-uri")
	}
	parsed, err := url.Parse(uri)
	root, _ := url.Parse(files.state.owner.settings.Location)
	if err != nil || parsed.Scheme != "s3" || parsed.Host != root.Host || parsed.User != nil || parsed.RawQuery != "" ||
		parsed.Fragment != "" || parsed.RawPath != "" || parsed.Opaque != "" || parsed.ForceQuery || parsed.String() != uri || !strings.HasPrefix(uri, files.state.owner.settings.Location) ||
		!fs.ValidPath(strings.TrimPrefix(parsed.Path, "/")) {
		return "", failure(ErrAuthority, "file")
	}
	return strings.TrimPrefix(parsed.Path, "/"), nil
}
func (files *fileIO) begin() (fault.Correlation, error) {
	state := files.state
	state.mu.Lock()
	defer state.mu.Unlock()
	state.fileOps++
	if state.fileOps > state.owner.settings.MaxFileOps {
		return fault.Correlation{}, failure(ErrLimit, "file-operations")
	}
	return fault.Correlation{Call: fmt.Sprintf("file-%d", state.fileOps)}, files.ctx.Err()
}

func (files *fileIO) enter() (func(), error) {
	select {
	case files.state.fileGate <- struct{}{}:
		return func() { <-files.state.fileGate }, nil
	case <-files.ctx.Done():
		return nil, errors.Join(files.ctx.Err(), context.Cause(files.ctx))
	}
}
func (files *fileIO) charge(count int64) error {
	state := files.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if count < 0 || count > state.owner.settings.MaxIOBytes-state.ioBytes {
		return failure(ErrLimit, "file-bytes")
	}
	state.ioBytes += count
	return nil
}
func (files *fileIO) Open(uri string) (nativeio.File, error) {
	key, err := files.key(uri)
	if err != nil {
		return nil, err
	}
	if _, err = files.begin(); err != nil {
		return nil, err
	}
	leave, err := files.enter()
	if err != nil {
		return nil, err
	}
	defer leave()
	owner := files.state.owner
	bucket, _ := owner.settings.storageAddress()
	ctx := storageContext(files.ctx, files.state)
	object, err := owner.storage.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, storageFailure(err)
	}
	if object.ContentLength == nil || *object.ContentLength <= 0 || object.ETag == nil || *object.ETag == "" {
		return nil, failure(ErrProtocol, "file-stat")
	}
	size := *object.ContentLength
	if size > int64(owner.settings.MaxObjectBytes) {
		return nil, failure(ErrLimit, "object-bytes")
	}
	if err = files.charge(size); err != nil {
		return nil, err
	}
	output, err := owner.storage.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key),
		Range: aws.String(fmt.Sprintf("bytes=0-%d", size-1)), IfMatch: object.ETag})
	if err != nil {
		return nil, storageFailure(err)
	}
	data, err := io.ReadAll(output.Body)
	files.state.note(output.Body.Close(), true)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != size || output.ETag == nil || *output.ETag != *object.ETag {
		return nil, failure(ErrProtocol, "file-size-or-identity")
	}
	return &memoryFile{Reader: bytes.NewReader(data), name: path.Base(key), size: size}, nil
}
func (files *fileIO) Create(uri string) (nativeio.FileWriter, error) {
	if _, err := files.key(uri); err != nil {
		return nil, err
	}
	if !files.state.owner.settings.Writes {
		return nil, failure(ErrAuthority, "file-write")
	}
	if err := files.ctx.Err(); err != nil {
		return nil, err
	}
	return &fileWriter{files: files, uri: uri}, nil
}
func (files *fileIO) WriteFile(uri string, data []byte) error {
	writer, err := files.Create(uri)
	if err != nil {
		return err
	}
	if _, err = writer.Write(data); err != nil {
		return err
	}
	return writer.Close()
}
func (*fileIO) Remove(string) error { return failure(ErrUnsupported, "automatic-file-deletion") }

type fileWriter struct {
	files  *fileIO
	uri    string
	data   bytes.Buffer
	once   sync.Once
	closed bool
	err    error
}

func (writer *fileWriter) Write(data []byte) (int, error) {
	if writer.closed {
		return 0, fs.ErrClosed
	}
	if writer.err != nil {
		return 0, writer.err
	}
	if err := writer.files.ctx.Err(); err != nil {
		writer.err = err
		return 0, err
	}
	if len(data) > writer.files.state.owner.settings.MaxObjectBytes-writer.data.Len() {
		writer.err = failure(ErrLimit, "object-bytes")
		return 0, writer.err
	}
	if err := writer.files.charge(int64(len(data))); err != nil {
		writer.err = err
		return 0, err
	}
	return writer.data.Write(data)
}

type writerOnly struct{ io.Writer }

func (writer *fileWriter) ReadFrom(reader io.Reader) (int64, error) {
	return io.Copy(writerOnly{writer}, reader)
}
func (writer *fileWriter) Close() error {
	writer.once.Do(func() {
		writer.closed = true
		defer writer.data.Reset()
		defer func() { writer.files.state.note(writer.err, true) }()
		if writer.err != nil {
			return
		}
		if strings.HasSuffix(writer.uri, ".parquet") {
			body := writer.data.Bytes()
			if len(body) < 12 || string(body[:4]) != "PAR1" || string(body[len(body)-4:]) != "PAR1" ||
				uint64(binary.LittleEndian.Uint32(body[len(body)-8:])) > uint64(len(body)-12) {
				writer.err = failure(ErrProtocol, "unfinished-parquet")
				return
			}
		}
		key, err := writer.files.key(writer.uri)
		if err != nil {
			writer.err = err
			return
		}
		if _, err := writer.files.begin(); err != nil {
			writer.err = err
			return
		}
		leave, err := writer.files.enter()
		if err != nil {
			writer.err = err
			return
		}
		defer leave()
		state := writer.files.state
		state.mu.Lock()
		index := len(state.files)
		state.files = append(state.files, FileEffect{URI: writer.uri})
		state.mu.Unlock()
		ctx := context.WithValue(storageContext(writer.files.ctx, state), storageWriteKey{}, index)
		bucket, _ := state.owner.settings.storageAddress()
		output, err := state.owner.storage.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key),
			Body: bytes.NewReader(writer.data.Bytes()), ContentLength: aws.Int64(int64(writer.data.Len())), IfNoneMatch: aws.String("*")})
		writer.err = err
		if err == nil {
			if output.ETag == nil || *output.ETag == "" {
				writer.err = failure(ErrProtocol, "upload-reply")
				return
			}
			state.mu.Lock()
			state.files[index].Effect = Acknowledged
			state.mu.Unlock()
		}
	})
	return writer.err
}

type memoryFile struct {
	*bytes.Reader
	name string
	size int64
}

func (file *memoryFile) Close() error { file.Reader.Reset(nil); return nil }
func (file *memoryFile) Stat() (fs.FileInfo, error) {
	return fileInfo{name: file.name, size: file.size}, nil
}

type fileInfo struct {
	name string
	size int64
}

func (info fileInfo) Name() string  { return info.name }
func (info fileInfo) Size() int64   { return info.size }
func (fileInfo) Mode() fs.FileMode  { return 0o600 }
func (fileInfo) ModTime() time.Time { return time.Time{} }
func (fileInfo) IsDir() bool        { return false }
func (fileInfo) Sys() any           { return nil }
