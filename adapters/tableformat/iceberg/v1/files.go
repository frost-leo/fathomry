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
	"github.com/apache/iceberg-go/encryption"
	"io/fs"
)

func (s *Session) ReadFile(uri string, keyMetadata encryption.EncryptionKeyMetadata, visit func(encryption.EncryptedInputFile) error) error {
	if err := s.valid(); err != nil {
		return err
	}
	return translate(s.native.ReadFile(uri, keyMetadata, visit), "read-file")
}
func (s *Session) CreateFile(uri, keyID string, visit func(encryption.EncryptedOutputFile) error) (encryption.EncryptionKeyMetadata, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	v, err := s.native.CreateFile(uri, keyID, visit)
	return v, translate(err, "create-file")
}
func (s *Session) WalkFiles(root string, visit fs.WalkDirFunc) error {
	if err := s.valid(); err != nil {
		return err
	}
	return translate(s.native.WalkFiles(root, visit), "walk-files")
}
func (s *Session) DeleteFiles(paths []string) ([]string, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	v, err := s.native.DeleteFiles(paths)
	return v, translate(err, "delete-files")
}
