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

package create

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func parentRoot(t testing.TB) (*os.Root, string) {
	t.Helper()
	path := t.TempDir()
	root, err := os.OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})
	return root, path
}

func TestCreateCompleteAndExistingTargets(t *testing.T) {
	for _, kind := range []string{"absent", "directory", "file", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			parent, path := parentRoot(t)
			existing := []byte("existing-owner")
			switch kind {
			case "directory":
				if err := parent.Mkdir("demo", 0755); err != nil {
					t.Fatal(err)
				}
				if err := parent.WriteFile("demo/sentinel", existing, 0600); err != nil {
					t.Fatal(err)
				}
			case "file":
				if err := parent.WriteFile("demo", existing, 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				outside := t.TempDir()
				if err := os.WriteFile(filepath.Join(outside, "sentinel"), existing, 0600); err != nil {
					t.Fatal(err)
				}
				if err := parent.Symlink(outside, "demo"); err != nil {
					t.Fatal(err)
				}
			}
			value := prepared(t)
			result, err := createInParent(context.Background(), parent, value, openFile)
			if kind != "absent" {
				if !errors.Is(err, ErrExists) || len(result.Directories) != 0 || len(result.Files) != 0 || result.State != "not_started" {
					t.Fatal("existing target admitted", result, err)
				}
				preserved := filepath.Join(path, "demo")
				if kind != "file" {
					preserved = filepath.Join(preserved, "sentinel")
				}
				data, err := os.ReadFile(preserved)
				if err != nil || !bytes.Equal(data, existing) {
					t.Fatal("existing content changed", err)
				}
				return
			}
			if err != nil || result.State != "complete" || len(result.Directories) != 4 || len(result.Files) != 6 {
				t.Fatal(result, err)
			}
			count := 0
			if err := filepath.WalkDir(filepath.Join(path, "demo"), func(_ string, entry fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !entry.IsDir() {
					count++
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if count != 6 {
				t.Fatal("unexpected output files", count)
			}
			for index, file := range value.files {
				data, err := os.ReadFile(filepath.Join(path, "demo", filepath.FromSlash(file.path)))
				observed := result.Files[index]
				if err != nil || !bytes.Equal(data, file.data) || !observed.Complete || observed.BytesWritten != int64(len(data)) {
					t.Fatal("write acknowledgement differs", file.path, err)
				}
			}
			repeated, err := createInParent(context.Background(), parent, value, openFile)
			if !errors.Is(err, ErrExists) || repeated.State != "not_started" {
				t.Fatal("retry adopted output", repeated, err)
			}
		})
	}
}

func TestConcurrentExclusiveClaim(t *testing.T) {
	parent, _ := parentRoot(t)
	value := prepared(t)
	var winners atomic.Int32
	var wait sync.WaitGroup
	for range 32 {
		wait.Go(func() {
			result, err := createInParent(context.Background(), parent, value, openFile)
			if err == nil {
				if result.State != "complete" {
					t.Error("winner incomplete")
				}
				winners.Add(1)
			} else if !errors.Is(err, ErrExists) || result.State != "not_started" || len(result.Files) != 0 {
				t.Error("loser changed target", result, err)
			}
		})
	}
	wait.Wait()
	if winners.Load() != 1 {
		t.Fatal("wrong winner count", winners.Load())
	}
}

type faultyFile struct {
	io.WriteCloser
	short      bool
	writeError error
	closeError error
	afterWrite func()
	afterClose func()
	closed     bool
}

func (file *faultyFile) Write(value []byte) (int, error) {
	if file.short {
		value = value[:min(7, len(value))]
	}
	count, err := file.WriteCloser.Write(value)
	if file.afterWrite != nil {
		file.afterWrite()
	}
	return count, errors.Join(err, file.writeError)
}
func (file *faultyFile) Close() error {
	file.closed = true
	err := file.WriteCloser.Close()
	if file.afterClose != nil {
		file.afterClose()
	}
	return errors.Join(err, file.closeError)
}

func TestPartialWriteCloseAndCancellation(t *testing.T) {
	for _, kind := range []string{"write", "short", "close", "before_write", "after_write", "after_last_close"} {
		t.Run(kind, func(t *testing.T) {
			parent, path := parentRoot(t)
			value := prepared(t)
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			cause := errors.New("private-create-canary")
			count := 0
			var wrapped []*faultyFile
			opener := func(root *os.Root, name string) (io.WriteCloser, error) {
				original, err := openFile(root, name)
				if err != nil {
					return nil, err
				}
				count++
				wrapper := &faultyFile{WriteCloser: original}
				if count == 1 {
					switch kind {
					case "write":
						wrapper.short = true
						wrapper.writeError = cause
					case "short":
						wrapper.short = true
					case "close":
						wrapper.closeError = cause
					case "before_write":
						cancel(cause)
					case "after_write":
						wrapper.afterWrite = func() { cancel(cause) }
					}
				}
				if kind == "after_last_close" && count == 6 {
					wrapper.afterClose = func() { cancel(cause) }
				}
				wrapped = append(wrapped, wrapper)
				return wrapper, nil
			}
			result, err := createInParent(ctx, parent, value, opener)
			if err == nil {
				t.Fatal("fault was hidden")
			}
			if len(result.Directories) != 4 {
				t.Fatal("directory facts lost", result)
			}
			if kind == "short" {
				if !errors.Is(err, io.ErrShortWrite) {
					t.Fatal("short write lost", err)
				}
			} else if !errors.Is(err, cause) {
				t.Fatal("original cause lost", err)
			}
			if kind == "after_last_close" {
				if result.State != "complete" || len(result.Files) != 6 {
					t.Fatal("complete effects erased by late cancel", result)
				}
			} else if result.State != "partial" || len(result.Files) != 1 {
				t.Fatal("unexpected partial effects", result)
			}
			first := result.Files[0]
			if kind == "after_write" || kind == "after_last_close" {
				if !first.Complete {
					t.Fatal("completed file acknowledgement erased")
				}
			} else if first.Complete {
				t.Fatal("incomplete file certified")
			}
			if kind == "before_write" && first.BytesWritten != 0 {
				t.Fatal("canceled write was dispatched")
			}
			if (kind == "write" || kind == "short") && first.BytesWritten != 7 {
				t.Fatal("partial byte count lost")
			}
			for _, file := range wrapped {
				if !file.closed {
					t.Fatal("file owner abandoned")
				}
			}
			entries, readErr := os.ReadDir(filepath.Join(path, "demo"))
			if readErr != nil || len(entries) == 0 {
				t.Fatal("partial target deleted", readErr)
			}
		})
	}
}

func TestCanceledBeforeClaimAndLeafConflict(t *testing.T) {
	parent, path := parentRoot(t)
	value := prepared(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := createInParent(ctx, parent, value, openFile)
	if !errors.Is(err, context.Canceled) || result.State != "not_started" {
		t.Fatal(result, err)
	}
	if _, err := os.Stat(filepath.Join(path, "demo")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("canceled claim created target", err)
	}
	outside := filepath.Join(t.TempDir(), "original")
	if err := os.WriteFile(outside, []byte("sentinel"), 0600); err != nil {
		t.Fatal(err)
	}
	opener := func(root *os.Root, name string) (io.WriteCloser, error) {
		if err := root.Symlink(outside, name); err != nil {
			return nil, err
		}
		return openFile(root, name)
	}
	result, err = createInParent(context.Background(), parent, value, opener)
	if !errors.Is(err, fs.ErrExist) || result.State != "partial" || len(result.Files) != 0 {
		t.Fatal("leaf conflict admitted", result, err)
	}
	data, err := os.ReadFile(outside)
	if err != nil || string(data) != "sentinel" {
		t.Fatal("outside leaf changed", err)
	}
}

func TestRepeatedRootCloseRetainsCompleteEffects(t *testing.T) {
	parent, _ := parentRoot(t)
	value := prepared(t)
	count := 0
	opener := func(root *os.Root, name string) (io.WriteCloser, error) {
		file, err := openFile(root, name)
		if err != nil {
			return nil, err
		}
		count++
		if count == 6 {
			return &faultyFile{WriteCloser: file, afterClose: func() { _ = root.Close() }}, nil
		}
		return file, nil
	}
	result, err := createInParent(context.Background(), parent, value, opener)
	if err != nil || result.State != "complete" {
		t.Fatal("idempotent root close lost effect facts", result, err)
	}
}
