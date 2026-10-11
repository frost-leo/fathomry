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
	"github.com/apache/iceberg-go/encryption"
	nativeio "github.com/apache/iceberg-go/io"
	"io/fs"
	"slices"
)

func (f *ManagedSession) ReadFile(uri string, keyMetadata encryption.EncryptionKeyMetadata, visit func(encryption.EncryptedInputFile) error) error {
	return managedDo(f, "read-file", grantRead, func(ctx context.Context) error {
		if visit == nil {
			return failure(ErrInput, "file-visitor")
		}
		if len(keyMetadata) > int(f.source.prepared.settings.MaxMetadataBytes) {
			return failure(ErrLimit, "key-metadata")
		}
		io, err := f.fileIO(ctx, uri, nil)
		if err != nil {
			return err
		}
		file, err := io.Open(uri)
		if err != nil {
			return err
		}
		defer func() { f.noteCleanup(file.Close()) }()
		manager := f.source.prepared.native.Encryption
		if manager == nil {
			manager = encryption.PlaintextEncryptionManager{}
		}
		wrapped, err := manager.NewDecryptedInputFile(ctx, file, slices.Clone(keyMetadata))
		if err != nil {
			if wrapped != nil && !typedNil(wrapped) {
				f.noteCleanup(wrapped.Close())
			}
			return err
		}
		if wrapped == nil || typedNil(wrapped) {
			return failure(ErrProtocol, "encryption-input")
		}
		defer func() { f.noteCleanup(wrapped.Close()) }()
		return visit(wrapped)
	})
}
func (f *ManagedSession) CreateFile(uri, keyID string, visit func(encryption.EncryptedOutputFile) error) (encryption.EncryptionKeyMetadata, error) {
	return managedCall(f, "create-file", grantWrite, func(ctx context.Context) (encryption.EncryptionKeyMetadata, error) {
		if visit == nil || len(keyID) > 1024 {
			return nil, failure(ErrInput, "file-visitor")
		}
		io, err := f.fileIO(ctx, uri, nil)
		if err != nil {
			return nil, err
		}
		file, err := io.(nativeio.WriteFileIO).Create(uri)
		if err != nil {
			return nil, err
		}
		owned := file.(*managedWriteFile)
		manager := f.source.prepared.native.Encryption
		if manager == nil {
			manager = encryption.PlaintextEncryptionManager{}
		}
		wrapped, err := manager.NewEncryptedOutputFile(ctx, file, keyID)
		if err != nil || wrapped == nil || typedNil(wrapped) {
			cleanup := owned.shutdown()
			if wrapped != nil && !typedNil(wrapped) {
				cleanup = errors.Join(cleanup, wrapped.Close())
			}
			f.noteCleanup(cleanup)
			if err == nil {
				err = failure(ErrProtocol, "encryption-output")
			}
			return nil, errors.Join(err, cleanup)
		}
		if err = visit(wrapped); err != nil {
			stopped := owned.shutdown()
			closed := wrapped.Close()
			f.noteCleanup(errors.Join(stopped, closed))
			return nil, errors.Join(err, stopped, closed)
		}
		if err = wrapped.Close(); err != nil {
			cleanup := owned.shutdown()
			f.noteCleanup(cleanup)
			return nil, errors.Join(err, cleanup)
		}
		if err = file.Close(); err != nil {
			return nil, err
		}
		metadata := wrapped.KeyMetadata()
		if int64(len(metadata)) > f.source.prepared.settings.MaxMetadataBytes {
			return nil, failure(ErrLimit, "key-metadata")
		}
		return slices.Clone(metadata), nil
	})
}
func (f *ManagedSession) WalkFiles(root string, visit fs.WalkDirFunc) error {
	return managedDo(f, "walk-files", grantRead, func(ctx context.Context) error {
		if visit == nil {
			return failure(ErrInput, "file-visitor")
		}
		io, err := f.fileIO(ctx, root, nil)
		if err != nil {
			return err
		}
		return io.(nativeio.ListableIO).WalkDir(root, visit)
	})
}
func (f *ManagedSession) DeleteFiles(paths []string) ([]string, error) {
	return managedCall(f, "delete-files", grantDeleteFiles, func(ctx context.Context) ([]string, error) {
		if len(paths) == 0 {
			return nil, nil
		}
		if int64(len(paths)) > f.source.prepared.settings.MaxFileOps {
			return nil, failure(ErrLimit, "delete-files")
		}
		for _, path := range paths {
			if err := f.location(path); err != nil {
				return nil, err
			}
		}
		io, err := f.fileIO(ctx, paths[0], nil)
		if err != nil {
			return nil, err
		}
		return io.(nativeio.BulkRemovableIO).DeleteFiles(ctx, slices.Clone(paths))
	})
}
