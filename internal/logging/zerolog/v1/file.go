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

package zerolog

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
)

const activeName = "current.jsonl"
const lockName = ".fathomry.lock"

type archive struct {
	name     string
	sequence uint64
}
type fileSink struct {
	settings fileSettings
	root     *os.Root
	file     *os.File
	lock     *os.File
	locked   bool
	size     int64
	sequence uint64
	archives []archive
	failed   error
	closed   bool
}

// The directory is exclusively managed, including across cooperating processes.
// A failed writer leaves the lock NAME as recovery evidence after closing all
// handles. Only an operator, after proving no live owner, may reconcile/remove it.
func openFile(settings fileSettings) (*fileSink, error) {
	sink := &fileSink{settings: settings}
	root, err := os.OpenRoot(settings.Directory)
	if err != nil {
		return sink, failure(ErrMaintenance, "open-directory", err)
	}
	sink.root = root
	info, err := root.Stat(".")
	if err != nil {
		return sink, failure(ErrMaintenance, "stat-directory", err)
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return sink, failure(ErrInput, "private-directory")
	}
	sink.lock, err = root.OpenFile(lockName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return sink, failure(ErrState, "directory-owned", err)
	}
	sink.locked = true
	if err := sink.inventory(); err != nil {
		return sink, err
	}
	sink.file, err = root.OpenFile(activeName, os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0600)
	if errors.Is(err, os.ErrExist) {
		original, statErr := root.Lstat(activeName)
		if statErr != nil {
			return sink, failure(ErrMaintenance, "stat-active", statErr)
		}
		if !privateRegular(original) || original.Size() > settings.MaxBytes {
			return sink, failure(ErrInput, "existing-active")
		}
		sink.file, err = root.OpenFile(activeName, os.O_RDWR|os.O_APPEND, 0600)
		if err == nil {
			actual, statErr := sink.file.Stat()
			if statErr != nil {
				return sink, failure(ErrMaintenance, "stat-open-active", statErr)
			}
			if !os.SameFile(original, actual) {
				return sink, failure(ErrState, "active-replaced")
			}
			sink.size = actual.Size()
			if sink.size > 0 {
				var last [1]byte
				if _, err := sink.file.ReadAt(last[:], sink.size-1); err != nil {
					return sink, failure(ErrMaintenance, "active-tail", err)
				}
				if last[0] != '\n' {
					return sink, failure(ErrState, "incomplete-active")
				}
			}
		}
	}
	if err != nil {
		return sink, failure(ErrMaintenance, "open-active", err)
	}
	return sink, nil
}
func privateRegular(info os.FileInfo) bool {
	return info.Mode().IsRegular() && info.Mode().Perm()&0077 == 0
}
func archiveName(sequence uint64, compressed bool) string {
	name := fmt.Sprintf("log-%020d.jsonl", sequence)
	if compressed {
		name += ".gz"
	}
	return name
}
func (sink *fileSink) inventory() error {
	directory, err := sink.root.Open(".")
	if err != nil {
		return failure(ErrMaintenance, "inventory", err)
	}
	entries, readErr := directory.ReadDir(maxBackups + 4)
	closeErr := directory.Close()
	if readErr != nil && readErr != io.EOF || closeErr != nil {
		return joined(ErrMaintenance, "inventory", readErr, closeErr)
	}
	if len(entries) > maxBackups+2 {
		return failure(ErrLimit, "inventory")
	}
	for _, entry := range entries {
		if entry.Name() == lockName || entry.Name() == activeName {
			continue
		}
		name := entry.Name()
		suffix := ".jsonl"
		if sink.settings.Compress {
			suffix += ".gz"
		}
		number := strings.TrimSuffix(strings.TrimPrefix(name, "log-"), suffix)
		sequence, err := strconv.ParseUint(number, 10, 64)
		if err != nil || sequence == 0 || name != archiveName(sequence, sink.settings.Compress) {
			return failure(ErrState, "unrecognized-file")
		}
		info, err := entry.Info()
		if err != nil {
			return failure(ErrMaintenance, "archive-info", err)
		}
		limit := sink.settings.MaxBytes
		if sink.settings.Compress {
			limit = sink.archiveLimit()
		}
		if !privateRegular(info) || info.Size() > limit {
			return failure(ErrInput, "existing-archive")
		}
		sink.archives = append(sink.archives, archive{name, sequence})
		sink.sequence = max(sink.sequence, sequence)
	}
	if len(sink.archives) > sink.settings.Backups {
		return failure(ErrLimit, "existing-backups")
	}
	slices.SortFunc(sink.archives, func(left, right archive) int {
		if left.sequence < right.sequence {
			return -1
		}
		if left.sequence > right.sequence {
			return 1
		}
		return 0
	})
	return nil
}
func (sink *fileSink) archiveLimit() int64 { return 2*sink.settings.MaxBytes + 64<<10 }
func (sink *fileSink) stop(err error) error {
	if err != nil && sink.failed == nil {
		sink.failed = err
	}
	return err
}
func (sink *fileSink) write(ctx context.Context, data []byte, report *SinkResult) {
	if sink.closed || sink.failed != nil {
		report.WriteError = failure(ErrState, "file-stopped", sink.failed)
		return
	}
	if int64(len(data)) > sink.settings.MaxBytes {
		report.WriteError = failure(ErrLimit, "file-record")
		return
	}
	if sink.size+int64(len(data)) > sink.settings.MaxBytes {
		report.Rotated, report.MaintenanceError = sink.rotate(ctx)
		if report.MaintenanceError != nil {
			return
		}
	}
	if ctx.Err() != nil {
		report.WriteError = joined(ErrWrite, "canceled", ctx.Err(), context.Cause(ctx))
		return
	}
	report.Attempted = true
	count, err := sink.file.Write(data)
	sink.size += int64(count)
	report.Written, report.BytesKnown = count, true
	if count != len(data) {
		err = errors.Join(err, io.ErrShortWrite)
	}
	report.WriteError = sink.stop(joined(ErrWrite, "file", err))
	report.Accepted = report.WriteError == nil
}
func (sink *fileSink) sync() error {
	if sink.closed || sink.failed != nil || sink.file == nil {
		return failure(ErrState, "file-stopped", sink.failed)
	}
	return sink.stop(joined(ErrMaintenance, "sync", sink.file.Sync()))
}
func (sink *fileSink) rotate(ctx context.Context) (rotated bool, err error) {
	if sink.closed || sink.failed != nil || sink.file == nil {
		return false, failure(ErrState, "file-stopped", sink.failed)
	}
	if sink.size == 0 {
		return false, nil
	}
	if ctx.Err() != nil {
		return false, joined(ErrMaintenance, "canceled", ctx.Err(), context.Cause(ctx))
	}
	if sink.sequence == math.MaxUint64 {
		return false, sink.stop(failure(ErrLimit, "archive-sequence"))
	}
	defer func() { sink.stop(err) }()
	name := archiveName(sink.sequence+1, sink.settings.Compress)
	// Close renders the handle unusable even on a reported I/O error; no later
	// no-op close is used as evidence that the preceding maintenance succeeded.
	closeErr := sink.file.Close()
	sink.file = nil
	if closeErr != nil {
		return false, failure(ErrMaintenance, "rotate-close", closeErr)
	}
	if sink.settings.Compress {
		err = sink.compress(ctx, name)
	} else {
		err = sink.root.Link(activeName, name)
	}
	if err != nil {
		return false, failure(ErrMaintenance, "archive", err)
	}
	sink.sequence++
	sink.archives = append(sink.archives, archive{name, sink.sequence})
	if err := sink.root.Remove(activeName); err != nil {
		return false, failure(ErrMaintenance, "remove-active", err)
	}
	sink.file, err = sink.root.OpenFile(activeName, os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return false, failure(ErrMaintenance, "new-active", err)
	}
	sink.size = 0
	if len(sink.archives) > sink.settings.Backups {
		if err := sink.root.Remove(sink.archives[0].name); err != nil {
			return false, failure(ErrMaintenance, "retention", err)
		}
		sink.archives = sink.archives[1:]
	}
	return true, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader contextReader) Read(data []byte) (int, error) {
	if reader.ctx.Err() != nil {
		return 0, errors.Join(reader.ctx.Err(), context.Cause(reader.ctx))
	}
	return reader.reader.Read(data)
}

type boundedWriter struct {
	ctx       context.Context
	writer    io.Writer
	remaining int64
}

func (writer *boundedWriter) Write(data []byte) (int, error) {
	if writer.ctx.Err() != nil {
		return 0, errors.Join(writer.ctx.Err(), context.Cause(writer.ctx))
	}
	if int64(len(data)) > writer.remaining {
		return 0, failure(ErrLimit, "compressed-archive")
	}
	count, err := writer.writer.Write(data)
	writer.remaining -= int64(count)
	return count, err
}
func (sink *fileSink) compress(ctx context.Context, name string) error {
	source, err := sink.root.Open(activeName)
	if err != nil {
		return err
	}
	temporary := "." + name + ".tmp"
	destination, err := sink.root.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return errors.Join(err, source.Close())
	}
	target := &boundedWriter{ctx: ctx, writer: destination, remaining: sink.archiveLimit()}
	compressor, _ := gzip.NewWriterLevel(target, gzip.BestSpeed)
	count, copyErr := io.CopyBuffer(compressor, contextReader{ctx, io.LimitReader(source, sink.settings.MaxBytes+1)}, make([]byte, 32<<10))
	if count > sink.settings.MaxBytes {
		copyErr = errors.Join(copyErr, failure(ErrLimit, "archive-input"))
	}
	compressErr := compressor.Close()
	sourceErr := source.Close()
	syncErr := destination.Sync()
	closeErr := destination.Close()
	err = errors.Join(copyErr, compressErr, sourceErr, syncErr, closeErr)
	if err == nil {
		// Link refuses an existing destination instead of replacing an archive.
		// Publication happens only after gzip finalization and file close.
		err = sink.root.Link(temporary, name)
	}
	return errors.Join(err, sink.root.Remove(temporary))
}
func (sink *fileSink) close() error {
	if sink.closed {
		return nil
	}
	sink.closed = true
	var causes []error
	if sink.file != nil {
		causes = append(causes, sink.file.Sync(), sink.file.Close())
		sink.file = nil
	}
	if sink.lock != nil {
		causes = append(causes, sink.lock.Close())
		sink.lock = nil
	}
	err := joined(ErrCleanup, "file-close", causes...)
	if sink.root != nil {
		if sink.locked && sink.failed == nil && err == nil {
			err = joined(ErrCleanup, "unlock", sink.root.Remove(lockName))
		} else if sink.failed != nil {
			err = joined(ErrCleanup, "recovery-required", err, sink.failed)
		}
		err = joined(ErrCleanup, "directory-close", err, sink.root.Close())
		sink.root = nil
	}
	return err
}
