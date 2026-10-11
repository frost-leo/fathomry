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
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	nativeio "github.com/apache/iceberg-go/io"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type managedIOConfig struct {
	Context                                context.Context
	Client                                 *s3.Client
	Root                                   string
	MaxObjectBytes, MaxIOBytes, MaxFileOps int64
	MaxOpenFiles, MaxListEntries, PartSize int
	CleanupTimeout                         time.Duration
	AllowWrite, AllowDelete                bool
	Check                                  func(operation, uri string, bytes int64) error
	Observe                                func(managedIOEvent)
	Cleanup                                func(error)
}

type managedIOEvent struct {
	Operation, URI, UploadID string
	Bytes                    int64
	Effect                   Effect
	Err                      error
}

type managedHandle interface{ shutdown() error }

// managedIO belongs to one admitted family. Callbacks are synchronous and must
// not reenter its handles. The configured client and its transport are borrowed;
// Close revokes and joins only this family's work, never the source's client.
type managedIO struct {
	config            managedIOConfig
	ctx               context.Context
	cancel            context.CancelFunc
	bucket, prefix    string
	mu                sync.Mutex
	closed            bool
	ops, bytes        int64
	openFiles, listed int
	handles           map[managedHandle]struct{}
	active            sync.WaitGroup
	closeOnce         sync.Once
	cleanupErr        error
}

func newManagedIO(config managedIOConfig) (*managedIO, error) {
	if config.Context == nil || config.Client == nil || config.MaxObjectBytes <= 0 || config.MaxIOBytes <= 0 || config.MaxFileOps <= 0 ||
		config.MaxOpenFiles <= 0 || config.MaxListEntries <= 0 || config.PartSize < 5<<20 || int64(config.PartSize) > 5<<30 || config.CleanupTimeout <= 0 {
		return nil, failure(ErrInput, "file-io-options")
	}
	root, err := canonicalS3URI(config.Root)
	if err != nil || !strings.HasSuffix(config.Root, "/") {
		return nil, failure(ErrInput, "file-io-root", err)
	}
	ctx, cancel := context.WithCancel(config.Context)
	return &managedIO{config: config, ctx: ctx, cancel: cancel, bucket: root.Host,
		prefix: strings.TrimPrefix(root.Path, "/"), handles: make(map[managedHandle]struct{})}, nil
}

func canonicalS3URI(uri string) (*url.URL, error) {
	parsed, err := url.Parse(uri)
	if err != nil || len(uri) > 8192 || parsed.Scheme != "s3" || parsed.Host == "" || parsed.User != nil || parsed.Opaque != "" ||
		parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || strings.ContainsAny(parsed.Host, ":@%") || !strings.HasPrefix(parsed.Path, "/") {
		return nil, failure(ErrAuthority, "file-uri")
	}
	canonical := &url.URL{Scheme: "s3", Host: parsed.Host, Path: parsed.Path}
	key := strings.TrimSuffix(strings.TrimPrefix(parsed.Path, "/"), "/")
	if canonical.String() != uri || key != "" && !fs.ValidPath(key) {
		return nil, failure(ErrAuthority, "file-uri")
	}
	return parsed, nil
}

func (files *managedIO) key(uri string) (string, error) {
	parsed, err := canonicalS3URI(uri)
	if err != nil || parsed.Host != files.bucket {
		return "", failure(ErrAuthority, "file-root", err)
	}
	key := strings.TrimPrefix(parsed.Path, "/")
	if !strings.HasPrefix(key, files.prefix) {
		return "", failure(ErrAuthority, "file-root")
	}
	return key, nil
}

func (files *managedIO) start() (func(), error) {
	files.mu.Lock()
	defer files.mu.Unlock()
	if files.closed {
		return nil, fs.ErrClosed
	}
	if err := files.ctx.Err(); err != nil {
		return nil, errors.Join(err, context.Cause(files.ctx))
	}
	files.active.Add(1)
	return files.active.Done, nil
}

func (files *managedIO) reserve(operation, uri string, bytes int64, request bool) error {
	files.mu.Lock()
	if files.closed {
		files.mu.Unlock()
		return fs.ErrClosed
	}
	if bytes < 0 || bytes > files.config.MaxIOBytes-files.bytes || request && files.ops >= files.config.MaxFileOps {
		files.mu.Unlock()
		return failure(ErrLimit, "file-io-budget")
	}
	files.bytes += bytes
	if request {
		files.ops++
	}
	files.mu.Unlock()
	if err := files.ctx.Err(); err != nil {
		return errors.Join(err, context.Cause(files.ctx))
	}
	if files.config.Check != nil {
		return files.config.Check(operation, uri, bytes)
	}
	return nil
}

func (files *managedIO) reserveHandle(uri string) error {
	files.mu.Lock()
	if files.closed {
		files.mu.Unlock()
		return fs.ErrClosed
	}
	if files.openFiles >= files.config.MaxOpenFiles {
		files.mu.Unlock()
		return failure(ErrLimit, "open-files")
	}
	files.openFiles++
	files.mu.Unlock()
	if files.config.Check != nil {
		if err := files.config.Check("open-handle", uri, 0); err != nil {
			files.mu.Lock()
			files.openFiles--
			files.mu.Unlock()
			return err
		}
	}
	return nil
}

func (files *managedIO) addHandle(handle managedHandle) error {
	files.mu.Lock()
	defer files.mu.Unlock()
	if files.closed {
		return fs.ErrClosed
	}
	files.handles[handle] = struct{}{}
	return nil
}

func (files *managedIO) releaseHandle(handle managedHandle, uri string) {
	files.mu.Lock()
	if handle != nil {
		if _, exists := files.handles[handle]; !exists {
			files.mu.Unlock()
			return
		}
		delete(files.handles, handle)
	}
	files.openFiles--
	files.mu.Unlock()
	if files.config.Check != nil {
		files.note(files.config.Check("close-handle", uri, 0))
	}
}

func (files *managedIO) note(err error) {
	if err == nil {
		return
	}
	files.mu.Lock()
	files.cleanupErr = errors.Join(files.cleanupErr, err)
	files.mu.Unlock()
	if files.config.Cleanup != nil {
		files.config.Cleanup(err)
	}
}

func (files *managedIO) observe(event managedIOEvent) {
	if files.config.Observe != nil {
		files.config.Observe(event)
	}
}

func (files *managedIO) Close() error {
	files.closeOnce.Do(func() {
		files.mu.Lock()
		files.closed = true
		handles := make([]managedHandle, 0, len(files.handles))
		for handle := range files.handles {
			handles = append(handles, handle)
		}
		files.mu.Unlock()
		files.cancel()
		for _, handle := range handles {
			files.note(handle.shutdown())
		}
		files.active.Wait()
	})
	files.mu.Lock()
	defer files.mu.Unlock()
	return files.cleanupErr
}

func managedNoRetry(options *s3.Options) { options.Retryer = aws.NopRetryer{} }

func (files *managedIO) Open(uri string) (nativeio.File, error) {
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
	retained := false
	defer func() {
		if !retained {
			files.releaseHandle(nil, uri)
		}
	}()
	if err = files.reserve("head", uri, 0, true); err != nil {
		return nil, err
	}
	output, err := files.config.Client.HeadObject(files.ctx, &s3.HeadObjectInput{Bucket: aws.String(files.bucket), Key: aws.String(key)}, managedNoRetry)
	if err != nil {
		return nil, storageFailure(err)
	}
	if output.ContentLength == nil || *output.ContentLength < 0 || output.ETag == nil || *output.ETag == "" {
		return nil, failure(ErrProtocol, "file-stat")
	}
	if *output.ContentLength > files.config.MaxObjectBytes {
		return nil, failure(ErrLimit, "object-bytes")
	}
	ctx, cancel := context.WithCancel(files.ctx)
	file := &managedReadFile{files: files, ctx: ctx, cancel: cancel, uri: uri, key: key, size: *output.ContentLength, etag: *output.ETag,
		version: aws.ToString(output.VersionId), modified: aws.ToTime(output.LastModified)}
	if err = files.addHandle(file); err != nil {
		cancel()
		return nil, err
	}
	retained = true
	return file, nil
}

func (files *managedIO) ReadFile(uri string) ([]byte, error) {
	done, err := files.start()
	if err != nil {
		return nil, err
	}
	defer done()
	file, err := files.Open(uri)
	if err != nil {
		return nil, err
	}
	reader := file.(*managedReadFile)
	defer reader.Close()
	if reader.size > int64(math.MaxInt) {
		return nil, failure(ErrLimit, "read-allocation")
	}
	// Reserve the complete allocation before allocating; readAt then uses the
	// reservation rather than charging the same bytes for a second time.
	if err = files.reserve("read-file", uri, reader.size, false); err != nil {
		return nil, err
	}
	data := make([]byte, int(reader.size))
	_, err = reader.readAt(data, 0, true)
	if err != nil {
		return nil, err
	}
	return data, nil
}

type managedReadFile struct {
	files                   *managedIO
	ctx                     context.Context
	cancel                  context.CancelFunc
	uri, key, etag, version string
	size                    int64
	modified                time.Time
	mu                      sync.Mutex
	closed                  bool
	active                  sync.WaitGroup
	positionMu              sync.Mutex
	position                int64
}

func (file *managedReadFile) Stat() (fs.FileInfo, error) {
	file.mu.Lock()
	defer file.mu.Unlock()
	if file.closed {
		return nil, fs.ErrClosed
	}
	return managedFileInfo{name: path.Base(file.key), size: file.size, modified: file.modified}, nil
}

func (file *managedReadFile) Read(data []byte) (int, error) {
	file.positionMu.Lock()
	defer file.positionMu.Unlock()
	count, err := file.ReadAt(data, file.position)
	file.position += int64(count)
	return count, err
}

func (file *managedReadFile) Seek(offset int64, whence int) (int64, error) {
	file.positionMu.Lock()
	defer file.positionMu.Unlock()
	file.mu.Lock()
	defer file.mu.Unlock()
	if file.closed {
		return 0, fs.ErrClosed
	}
	var base int64
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		base = file.position
	case io.SeekEnd:
		base = file.size
	default:
		return 0, fs.ErrInvalid
	}
	if offset < -base || offset > math.MaxInt64-base {
		return 0, fs.ErrInvalid
	}
	file.position = base + offset
	return file.position, nil
}

func (file *managedReadFile) ReadAt(data []byte, offset int64) (int, error) {
	return file.readAt(data, offset, false)
}

func (file *managedReadFile) readAt(data []byte, offset int64, prepaid bool) (int, error) {
	file.mu.Lock()
	if file.closed {
		file.mu.Unlock()
		return 0, fs.ErrClosed
	}
	file.active.Add(1)
	file.mu.Unlock()
	defer file.active.Done()
	done, err := file.files.start()
	if err != nil {
		return 0, err
	}
	defer done()
	if offset < 0 {
		return 0, fs.ErrInvalid
	}
	if len(data) == 0 {
		return 0, nil
	}
	if offset >= file.size {
		return 0, io.EOF
	}
	wanted := min(int64(len(data)), file.size-offset)
	charge := wanted
	if prepaid {
		charge = 0
	}
	if err = file.files.reserve("get", file.uri, charge, true); err != nil {
		return 0, err
	}
	input := &s3.GetObjectInput{Bucket: aws.String(file.files.bucket), Key: aws.String(file.key), IfMatch: aws.String(file.etag),
		Range: aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+wanted-1))}
	if file.version != "" {
		input.VersionId = aws.String(file.version)
	}
	output, err := file.files.config.Client.GetObject(file.ctx, input, managedNoRetry)
	if err != nil {
		return 0, storageFailure(err)
	}
	defer func() { file.files.note(output.Body.Close()) }()
	if aws.ToString(output.ETag) != file.etag || aws.ToInt64(output.ContentLength) != wanted ||
		aws.ToString(output.ContentRange) != fmt.Sprintf("bytes %d-%d/%d", offset, offset+wanted-1, file.size) ||
		file.version != "" && aws.ToString(output.VersionId) != file.version {
		return 0, failure(ErrProtocol, "file-range-identity")
	}
	count, err := io.ReadFull(output.Body, data[:int(wanted)])
	if err == nil {
		var extra [1]byte
		if n, terminal := output.Body.Read(extra[:]); n != 0 || terminal != io.EOF {
			err = failure(ErrProtocol, "file-range-length", terminal)
		}
	}
	if err == nil && count < len(data) {
		err = io.EOF
	}
	return count, err
}

func (file *managedReadFile) Close() error {
	file.mu.Lock()
	if !file.closed {
		file.closed = true
		file.cancel()
	}
	file.mu.Unlock()
	file.active.Wait()
	file.files.releaseHandle(file, file.uri)
	return nil
}

func (file *managedReadFile) shutdown() error { return file.Close() }

type managedFileInfo struct {
	name      string
	size      int64
	modified  time.Time
	directory bool
}

func (info managedFileInfo) Name() string { return info.name }
func (info managedFileInfo) Size() int64  { return info.size }
func (info managedFileInfo) Mode() fs.FileMode {
	if info.directory {
		return fs.ModeDir | 0o700
	}
	return 0o600
}
func (info managedFileInfo) ModTime() time.Time         { return info.modified }
func (info managedFileInfo) IsDir() bool                { return info.directory }
func (info managedFileInfo) Sys() any                   { return nil }
func (info managedFileInfo) Type() fs.FileMode          { return info.Mode().Type() }
func (info managedFileInfo) Info() (fs.FileInfo, error) { return info, nil }
