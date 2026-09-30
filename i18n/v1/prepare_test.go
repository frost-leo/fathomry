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

package i18n

import (
	"errors"
	"io"
	"io/fs"
	"testing"
)

type observedFS struct {
	base                                            fs.FS
	opened, closed                                  int
	paged, stalled, bothOnOpen                      bool
	readErr, dirCloseErr, fileCloseErr, errorOnOpen error
}

func (filesystem *observedFS) Open(name string) (fs.File, error) {
	file, err := filesystem.base.Open(name)
	if err != nil {
		return nil, err
	}
	filesystem.opened++
	wrapped := &observedFile{File: file, filesystem: filesystem, directory: name == "."}
	if filesystem.errorOnOpen != nil {
		if filesystem.bothOnOpen {
			return wrapped, filesystem.errorOnOpen
		}
		_ = wrapped.Close()
		return nil, filesystem.errorOnOpen
	}
	return wrapped, nil
}

type observedFile struct {
	fs.File
	filesystem *observedFS
	directory  bool
}

func (file *observedFile) Read(data []byte) (int, error) {
	if !file.directory && file.filesystem.readErr != nil {
		return 0, file.filesystem.readErr
	}
	return file.File.Read(data)
}
func (file *observedFile) ReadDir(count int) ([]fs.DirEntry, error) {
	if file.filesystem.stalled {
		return nil, nil
	}
	if file.filesystem.paged {
		count = 1
	}
	return file.File.(fs.ReadDirFile).ReadDir(count)
}
func (file *observedFile) Close() error {
	file.filesystem.closed++
	original := file.File.Close()
	if original != nil {
		return original
	}
	if file.directory {
		return file.filesystem.dirCloseErr
	}
	return file.filesystem.fileCloseErr
}
func TestResourceLifetime(t *testing.T) {
	t.Run("paged_directory_is_fully_consumed", func(t *testing.T) {
		observed := &observedFS{base: copyFiles(t, "en", "zh-CN", "fr"), paged: true}
		component := fixtureComponent()
		component.Resources = observed
		component.Directory = "."
		catalog := mustCatalog(t, component)
		owners, _ := catalog.Components()
		if len(owners[0].Locales) != 3 || observed.opened != 4 || observed.closed != 4 {
			t.Fatal("short directory batch or close accounting lost")
		}
	})
	t.Run("native_errors_and_cleanup", func(t *testing.T) {
		readFailure := errors.New("private-read-canary")
		closeFailure := errors.New("private-close-canary")
		openFailure := errors.New("private-open-canary")
		for _, configure := range []func(*observedFS){
			func(value *observedFS) { value.readErr = readFailure; value.fileCloseErr = closeFailure },
			func(value *observedFS) { value.dirCloseErr = closeFailure },
			func(value *observedFS) { value.bothOnOpen = true; value.errorOnOpen = openFailure },
			func(value *observedFS) { value.stalled = true },
		} {
			observed := &observedFS{base: copyFiles(t, "en", "zh-CN")}
			configure(observed)
			component := fixtureComponent()
			component.Resources = observed
			component.Directory = "."
			catalog, err := Prepare(component)
			if catalog != nil || !errors.Is(err, ErrResource) || observed.opened != observed.closed {
				t.Fatal("failed preparation leaked a file or published state")
			}
			for _, cause := range []error{observed.readErr, observed.fileCloseErr, observed.dirCloseErr, observed.errorOnOpen} {
				if cause != nil && !errors.Is(err, cause) {
					t.Fatal("native I/O cause lost")
				}
			}
			if observed.stalled && !errors.Is(err, io.ErrNoProgress) {
				t.Fatal("stalled iterator was not bounded")
			}
		}
	})
	t.Run("typed_nil_filesystem", func(t *testing.T) {
		var missing *observedFS
		component := fixtureComponent()
		component.Resources = missing
		if _, err := Prepare(component); !errors.Is(err, ErrResource) {
			t.Fatal("typed nil filesystem invoked")
		}
	})
}
