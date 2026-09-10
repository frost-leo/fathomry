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

package viper

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
)

func TestRegularFilesReleaseDescriptorsAndRejectFIFO(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "input.yaml")
	if err := os.WriteFile(path, []byte("value: static"), 0600); err != nil {
		t.Fatal("fixture write failed")
	}
	descriptors := func() int {
		t.Helper()
		files, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal("descriptor observation unavailable on this Linux fixture")
		}
		return len(files)
	}
	beforeFD, beforeGo := descriptors(), runtime.NumGoroutine()
	for range 100 {
		_, err := Load(context.Background(), []LoadInput{
			{Options: OptionsV1{Encoding: "yaml"}, File: path},
			{Options: OptionsV1{Encoding: "json"}, Reader: strings.NewReader("{")},
		})
		if err == nil {
			t.Fatal("malformed batch accepted")
		}
	}
	afterFD, afterGo := descriptors(), runtime.NumGoroutine()
	if afterFD != beforeFD || afterGo > beforeGo {
		t.Fatal("repeated file failures retained descriptors or goroutines")
	}
	t.Logf("100 failed two-input loads: descriptors %d -> %d; goroutines %d -> %d", beforeFD, afterFD, beforeGo, afterGo)
	fifo := filepath.Join(directory, "pipe")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal("FIFO fixture creation failed")
	}
	if got, err := Load(context.Background(), []LoadInput{{Options: OptionsV1{Encoding: "yaml"}, File: fifo}}); got != nil || !errors.Is(err, ErrInput) {
		t.Fatal("nonregular file was opened")
	}
	if os.Geteuid() != 0 {
		if err := os.Chmod(path, 0000); err != nil {
			t.Fatal("fixture permission change failed")
		}
		if got, err := Load(context.Background(), []LoadInput{{Options: OptionsV1{Encoding: "yaml"}, File: path}}); got != nil || !errors.Is(err, os.ErrPermission) {
			t.Fatal("unreadable selected file was accepted or lost its cause")
		}
	} else {
		t.Log("real permission-denial check is not applicable to root; injected ErrPermission coverage is separate")
	}
}
