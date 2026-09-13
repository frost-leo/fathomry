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
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestFileShortWriteStopsGrowthAndRetainsEvidence(t *testing.T) {
	const child = "FATHOMRY_ZEROLOG_FSIZE_CHILD"
	if os.Getenv(child) != "1" {
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, executable, "-test.run=^TestFileShortWriteStopsGrowthAndRetainsEvidence$", "-test.v")
		command.Env = append(os.Environ(), child+"=1")
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("isolated file-size fault failed: %v\n%s", err, output)
		}
		return
	}
	options := fileOptions(t, false, 2)
	directory := options.Sinks[0].File.Directory
	var healthy bytes.Buffer
	options.Sinks = append(options.Sinks, SinkV1{Name: "healthy", Writer: &healthy})
	f := bindFixture(t, options, 1)
	f.allowCloseError = true
	if result := emit(t, f, "original", Info, "original"); result.Err() != nil {
		t.Fatal(result.Err())
	}
	drain(t, f.inbox)
	before, err := os.ReadFile(filepath.Join(directory, activeName))
	if err != nil {
		t.Fatal(err)
	}
	var original syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &original); err != nil {
		t.Fatal(err)
	}
	limited := original
	limited.Cur = uint64(len(before) + 32)
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limited); err != nil {
		t.Fatal(err)
	}
	// Limit only this child and restore before test cleanup/coverage output.
	defer func() {
		if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &original); err != nil {
			t.Error(err)
		}
	}()
	result := emit(t, f, "partial", Info, "partial")
	sinks := result.Outcome.Value.SinksCopy()
	if !errors.Is(result.Err(), syscall.EFBIG) || sinks[0].Written != 32 || !sinks[0].BytesKnown ||
		!sinks[0].Attempted || sinks[0].Accepted || !sinks[1].Accepted {
		t.Fatal("partial-write evidence lost", result.Err())
	}
	drain(t, f.inbox)
	after, err := os.ReadFile(filepath.Join(directory, activeName))
	if err != nil || len(after) != len(before)+32 || !bytes.HasPrefix(after, before) || after[len(after)-1] == '\n' {
		t.Fatal("partial file effect changed")
	}
	result = emit(t, f, "later", Info, "later")
	if result.Outcome.Value.SinksCopy()[0].Attempted || !errors.Is(result.Err(), ErrState) || !result.Outcome.Value.SinksCopy()[1].Accepted {
		t.Fatal("failed file resumed or healthy output stopped")
	}
	drain(t, f.inbox)
	if len(decodeRecords(t, healthy.Bytes())) != 3 {
		t.Fatal("healthy sink lost an event")
	}
	if err := f.assembly.Close(context.Background()); !errors.Is(err, syscall.EFBIG) {
		t.Fatal("cleanup erased original failure", err)
	}
	if _, err := os.Stat(filepath.Join(directory, lockName)); err != nil {
		t.Fatal("partial output lost recovery marker")
	}
}
