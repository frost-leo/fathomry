//go:build linux

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

package zap

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// All operations run under the source's single authoritative admission. The
// directory fd pins the namespace and flock excludes cooperating process owners.
type rotatingFile struct {
	options   outputSettings
	directory *os.File
	active    *os.File
	size      int64
	sequence  uint64
	archives  []string
	failed    error
	closed    bool
	closeErr  error
}

func openRotatingFile(options outputSettings) (*rotatingFile, error) {
	writer := &rotatingFile{options: options}
	descriptor, err := unix.Open(options.Directory, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return writer, err
	}
	writer.directory = os.NewFile(uintptr(descriptor), "zap-directory")
	var status unix.Stat_t
	if err = unix.Fstat(descriptor, &status); err != nil {
		return writer, err
	}
	if status.Mode&0077 != 0 || status.Uid != uint32(os.Geteuid()) {
		return writer, failure(ErrInput, "directory-permissions")
	}
	if err = unix.Flock(descriptor, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return writer, err
	}
	entries, err := writer.directory.ReadDir(options.MaxBackups + 2)
	if err != nil && !errors.Is(err, io.EOF) {
		return writer, err
	}
	if len(entries) > options.MaxBackups+1 {
		return writer, failure(ErrLimit, "directory-entries")
	}
	activeExists := false
	for _, entry := range entries {
		name := entry.Name()
		maximum := options.MaxFileBytes
		if name == "current.log" {
			activeExists = true
		} else {
			suffix := ".log"
			if options.Compress {
				suffix += ".gz"
				maximum = compressedLimit(maximum)
			}
			number := strings.TrimSuffix(strings.TrimPrefix(name, "archive-"), suffix)
			sequence, parseErr := strconv.ParseUint(number, 10, 64)
			if parseErr != nil || sequence == 0 || name != fmt.Sprintf("archive-%020d%s", sequence, suffix) {
				return writer, failure(ErrState, "directory-content")
			}
			writer.sequence = max(writer.sequence, sequence)
			writer.archives = append(writer.archives, name)
		}
		file, err := writer.open(name, unix.O_RDONLY, 0)
		if err != nil {
			return writer, err
		}
		var status unix.Stat_t
		statErr := unix.Fstat(int(file.Fd()), &status)
		if statErr == nil && (status.Mode&unix.S_IFMT != unix.S_IFREG || status.Nlink != 1 ||
			status.Mode&0077 != 0 || status.Uid != uint32(os.Geteuid()) || status.Size < 0 || status.Size > maximum) {
			statErr = failure(ErrState, "file-metadata")
		}
		if statErr == nil && name == "current.log" && status.Size > 0 {
			var end [1]byte
			_, statErr = file.ReadAt(end[:], status.Size-1)
			if statErr == nil && end[0] != '\n' {
				statErr = failure(ErrState, "incomplete-record")
			}
		}
		if err = errors.Join(statErr, file.Close()); err != nil {
			return writer, err
		}
	}
	if len(writer.archives) > options.MaxBackups {
		return writer, failure(ErrLimit, "archives")
	}
	sort.Strings(writer.archives)
	flags := unix.O_WRONLY | unix.O_APPEND
	if !activeExists {
		flags |= unix.O_CREAT | unix.O_EXCL
	}
	writer.active, err = writer.open("current.log", flags, 0600)
	if err != nil {
		return writer, err
	}
	info, err := writer.active.Stat()
	if err != nil {
		return writer, err
	}
	writer.size = info.Size()
	if !activeExists {
		err = writer.directory.Sync()
	}
	return writer, err
}

func (writer *rotatingFile) open(name string, flags int, mode uint32) (*os.File, error) {
	descriptor, err := unix.Openat(int(writer.directory.Fd()), name, flags|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, mode)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(descriptor), "zap-file"), nil
}
func (writer *rotatingFile) rename(from, to string) error {
	descriptor := int(writer.directory.Fd())
	return unix.Renameat2(descriptor, from, descriptor, to, unix.RENAME_NOREPLACE)
}
func (writer *rotatingFile) remove(name string) error {
	return unix.Unlinkat(int(writer.directory.Fd()), name, 0)
}
func (writer *rotatingFile) poison(err error) error {
	if err != nil && writer.failed == nil {
		writer.failed = failure(ErrFile, "maintenance-or-write", err)
	}
	return writer.failed
}

func (writer *rotatingFile) Write(data []byte) (int, error) {
	if writer.closed {
		return 0, failure(ErrState, "file-closed", writer.failed, writer.closeErr)
	}
	if writer.failed != nil {
		return 0, writer.failed
	}
	if int64(len(data)) > writer.options.MaxFileBytes {
		return 0, failure(ErrLimit, "file-entry")
	}
	if writer.size+int64(len(data)) > writer.options.MaxFileBytes {
		if err := writer.rotate(); err != nil {
			return 0, writer.poison(err)
		}
	}
	count, err := writer.active.Write(data)
	writer.size += int64(count)
	if count != len(data) && err == nil {
		err = io.ErrShortWrite
	}
	return count, writer.poison(err)
}

func (writer *rotatingFile) rotate() error {
	if writer.sequence == ^uint64(0) {
		return failure(ErrLimit, "archive-sequence")
	}
	if err := writer.active.Sync(); err != nil {
		return err
	}
	err := writer.active.Close()
	writer.active = nil
	if err != nil {
		return err
	}
	writer.sequence++
	raw := fmt.Sprintf("archive-%020d.log", writer.sequence)
	if err = writer.rename("current.log", raw); err != nil {
		return err
	}
	if err = writer.directory.Sync(); err != nil {
		return err
	}
	archive := raw
	if writer.options.Compress {
		archive = raw + ".gz"
		if err = writer.compress(raw, archive); err != nil {
			return err
		}
	}
	writer.archives = append(writer.archives, archive)
	for len(writer.archives) > writer.options.MaxBackups {
		if err = writer.remove(writer.archives[0]); err != nil {
			return err
		}
		writer.archives = writer.archives[1:]
		if err = writer.directory.Sync(); err != nil {
			return err
		}
	}
	writer.active, err = writer.open("current.log", unix.O_WRONLY|unix.O_APPEND|unix.O_CREAT|unix.O_EXCL, 0600)
	if err != nil {
		return err
	}
	writer.size = 0
	return writer.directory.Sync()
}

func compressedLimit(size int64) int64 { return size + size/100 + 64<<10 }

func (writer *rotatingFile) compress(raw, archive string) error {
	input, err := writer.open(raw, unix.O_RDONLY, 0)
	if err != nil {
		return err
	}
	output, err := writer.open(archive+".tmp", unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL, 0600)
	if err != nil {
		return errors.Join(err, input.Close())
	}
	err = compressGZIP(input, output, writer.options.MaxFileBytes)
	if err == nil {
		err = output.Sync()
	}
	err = errors.Join(err, output.Close(), input.Close())
	if err != nil {
		return err
	}
	if err = writer.rename(archive+".tmp", archive); err != nil {
		return err
	}
	if err = writer.directory.Sync(); err != nil {
		return err
	}
	if err = writer.remove(raw); err != nil {
		return err
	}
	return writer.directory.Sync()
}

type cappedWriter struct {
	writer    io.Writer
	remaining int64
}

func (writer *cappedWriter) Write(data []byte) (int, error) {
	if int64(len(data)) > writer.remaining {
		return 0, failure(ErrLimit, "compressed-bytes")
	}
	count, err := writer.writer.Write(data)
	if count < 0 || count > len(data) {
		return 0, failure(ErrWrite, "compression-count", err)
	}
	writer.remaining -= int64(count)
	if count != len(data) && err == nil {
		err = io.ErrShortWrite
	}
	return count, err
}
func compressGZIP(input io.Reader, output io.Writer, maximum int64) error {
	bounded := &cappedWriter{writer: output, remaining: compressedLimit(maximum)}
	compressor, err := gzip.NewWriterLevel(bounded, gzip.BestSpeed)
	if err != nil {
		return err
	}
	count, copyErr := io.CopyBuffer(compressor, io.LimitReader(input, maximum+1), make([]byte, 32<<10))
	if count > maximum {
		copyErr = errors.Join(copyErr, failure(ErrLimit, "compression-input"))
	}
	return errors.Join(copyErr, compressor.Close())
}

func (writer *rotatingFile) Sync() error {
	if writer.closed {
		return failure(ErrState, "file-closed", writer.failed, writer.closeErr)
	}
	if writer.failed != nil {
		return writer.failed
	}
	return writer.poison(writer.active.Sync())
}
func (writer *rotatingFile) Close() error {
	if writer.closed {
		return writer.closeErr
	}
	writer.closed = true
	var syncErr, closeErr, directoryErr error
	if writer.active != nil {
		syncErr = writer.active.Sync()
		closeErr = writer.active.Close()
		writer.active = nil
	}
	if writer.directory != nil {
		directoryErr = writer.directory.Close()
		writer.directory = nil
	}
	writer.closeErr = failureOrNil(ErrCleanup, "file-close", writer.failed, syncErr, closeErr, directoryErr)
	return writer.closeErr
}
