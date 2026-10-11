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
	"io/fs"
	"net/url"
	"path"
	"slices"
	"strings"

	nativeio "github.com/apache/iceberg-go/io"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (files *managedIO) Remove(uri string) error { return files.remove(files.ctx, uri) }

func (files *managedIO) remove(ctx context.Context, uri string) error {
	if !files.config.AllowDelete {
		return failure(ErrAuthority, "file-delete")
	}
	key, err := files.key(uri)
	if err != nil {
		return err
	}
	done, err := files.start()
	if err != nil {
		return err
	}
	defer done()
	work, cancel := context.WithCancel(files.ctx)
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	defer cancel()
	if err = ctx.Err(); err != nil {
		return errors.Join(err, context.Cause(ctx))
	}
	if err = files.reserve("delete", uri, 0, true); err != nil {
		return err
	}
	files.observe(managedIOEvent{Operation: "delete", URI: uri, Effect: Unknown})
	_, err = files.config.Client.DeleteObject(work, &s3.DeleteObjectInput{Bucket: aws.String(files.bucket), Key: aws.String(key)}, managedNoRetry)
	if errors.Is(storageFailure(err), fs.ErrNotExist) {
		err = nil
	}
	files.observe(managedIOEvent{Operation: "delete", URI: uri, Effect: managedMutationEffect(err), Err: err})
	files.note(err)
	return err
}

// DeleteFiles records one exact acknowledgement per object. It does not delete
// versions, infer an orphan, retry an unknown response, or compensate a commit.
func (files *managedIO) DeleteFiles(ctx context.Context, uris []string) ([]string, error) {
	if ctx == nil {
		return nil, failure(ErrInput, "delete-context")
	}
	if len(uris) == 0 {
		return nil, nil
	}
	if len(uris) > files.config.MaxListEntries {
		return nil, failure(ErrLimit, "delete-entries")
	}
	done, err := files.start()
	if err != nil {
		return nil, err
	}
	defer done()
	var deleted []string
	var combined error
	for _, uri := range uris {
		if err := ctx.Err(); err != nil {
			return deleted, errors.Join(combined, err, context.Cause(ctx))
		}
		if err := files.remove(ctx, uri); err != nil {
			combined = errors.Join(combined, err)
			if errors.Is(err, ErrLimit) || errors.Is(err, fs.ErrClosed) || files.ctx.Err() != nil {
				break
			}
		} else {
			deleted = append(deleted, uri)
		}
	}
	return deleted, combined
}

func (files *managedIO) uri(key string) string {
	return (&url.URL{Scheme: "s3", Host: files.bucket, Path: "/" + key}).String()
}

func (files *managedIO) visit(uri string, info fs.DirEntry, visit fs.WalkDirFunc) error {
	files.mu.Lock()
	if files.listed >= files.config.MaxListEntries {
		files.mu.Unlock()
		return failure(ErrLimit, "list-entries")
	}
	files.listed++
	files.mu.Unlock()
	if err := files.ctx.Err(); err != nil {
		return errors.Join(err, context.Cause(files.ctx))
	}
	if files.config.Check != nil {
		if err := files.config.Check("list-entry", uri, 0); err != nil {
			return err
		}
	}
	return visit(uri, info, nil)
}

func (files *managedIO) WalkDir(root string, visit fs.WalkDirFunc) error {
	if visit == nil {
		return failure(ErrInput, "walk-callback")
	}
	done, err := files.start()
	if err != nil {
		return err
	}
	defer done()
	uri := root
	if !strings.HasSuffix(uri, "/") {
		uri += "/"
	}
	prefix, err := files.key(uri)
	if err != nil {
		return err
	}
	if !strings.HasSuffix(root, "/") && prefix != files.prefix {
		key := strings.TrimSuffix(prefix, "/")
		if err = files.reserve("head", root, 0, true); err != nil {
			return err
		}
		output, headErr := files.config.Client.HeadObject(files.ctx, &s3.HeadObjectInput{Bucket: aws.String(files.bucket), Key: aws.String(key)}, managedNoRetry)
		if headErr == nil {
			if output.ContentLength == nil || *output.ContentLength < 0 {
				return failure(ErrProtocol, "walk-file-stat")
			}
			err = files.visit(root, managedFileInfo{name: path.Base(key), size: *output.ContentLength, modified: aws.ToTime(output.LastModified)}, visit)
			if err == fs.SkipAll || err == fs.SkipDir {
				return nil
			}
			return err
		}
		if !errors.Is(storageFailure(headErr), fs.ErrNotExist) {
			return headErr
		}
	}
	err = files.visit(strings.TrimSuffix(root, "/"), managedFileInfo{name: path.Base(strings.TrimSuffix(prefix, "/")), directory: true}, visit)
	if err == fs.SkipDir || err == fs.SkipAll {
		return nil
	}
	if err != nil {
		return err
	}
	err = files.walkPrefix(prefix, visit)
	if err == fs.SkipAll {
		return nil
	}
	return err
}

func (files *managedIO) walkPrefix(prefix string, visit fs.WalkDirFunc) error {
	var token *string
	last := ""
	for {
		files.mu.Lock()
		remaining := files.config.MaxListEntries - files.listed
		files.mu.Unlock()
		if remaining <= 0 {
			return failure(ErrLimit, "list-entries")
		}
		uri := files.uri(prefix)
		if err := files.reserve("list", uri, 0, true); err != nil {
			return err
		}
		page, err := files.config.Client.ListObjectsV2(files.ctx, &s3.ListObjectsV2Input{Bucket: aws.String(files.bucket), Prefix: aws.String(prefix),
			Delimiter: aws.String("/"), MaxKeys: aws.Int32(int32(min(remaining, 1000))), ContinuationToken: token}, managedNoRetry)
		if err != nil {
			return err
		}
		type entry struct {
			key  string
			info managedFileInfo
		}
		if len(page.Contents)+len(page.CommonPrefixes) > min(remaining, 1000) {
			return failure(ErrProtocol, "list-page-size")
		}
		entries := make([]entry, 0, len(page.Contents)+len(page.CommonPrefixes))
		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			if key == prefix {
				continue
			}
			if object.Size == nil || *object.Size < 0 || !strings.HasPrefix(key, prefix) || strings.Contains(strings.TrimPrefix(key, prefix), "/") {
				return failure(ErrProtocol, "list-object")
			}
			if _, err := files.key(files.uri(key)); err != nil {
				return err
			}
			entries = append(entries, entry{key: key, info: managedFileInfo{name: path.Base(key), size: *object.Size, modified: aws.ToTime(object.LastModified)}})
		}
		for _, common := range page.CommonPrefixes {
			key := aws.ToString(common.Prefix)
			if key == prefix || !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, "/") || strings.Contains(strings.TrimSuffix(strings.TrimPrefix(key, prefix), "/"), "/") {
				return failure(ErrProtocol, "list-prefix")
			}
			if _, err := files.key(files.uri(key)); err != nil {
				return err
			}
			entries = append(entries, entry{key: key, info: managedFileInfo{name: path.Base(strings.TrimSuffix(key, "/")), directory: true}})
		}
		slices.SortFunc(entries, func(left, right entry) int { return strings.Compare(left.key, right.key) })
		for _, item := range entries {
			if item.key <= last {
				return failure(ErrProtocol, "list-order")
			}
			last = item.key
			err := files.visit(strings.TrimSuffix(files.uri(item.key), "/"), item.info, visit)
			if err == fs.SkipDir {
				if item.info.directory {
					continue
				}
				return nil
			}
			if err != nil {
				return err
			}
			if item.info.directory {
				if err = files.walkPrefix(item.key, visit); err != nil {
					return err
				}
			}
		}
		if !aws.ToBool(page.IsTruncated) {
			return nil
		}
		next := aws.ToString(page.NextContinuationToken)
		if next == "" || next == aws.ToString(token) {
			return failure(ErrProtocol, "list-continuation")
		}
		token = aws.String(next)
	}
}

var (
	_ nativeio.ReadFileIO      = (*managedIO)(nil)
	_ nativeio.ListableIO      = (*managedIO)(nil)
	_ nativeio.BulkRemovableIO = (*managedIO)(nil)
)
