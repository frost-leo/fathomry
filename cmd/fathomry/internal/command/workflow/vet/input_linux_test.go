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

package vet

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
)

func TestFIFOReplacement(t *testing.T) {
	if path := os.Getenv("FATHOMRY_WORKFLOW_FIFO_PROBE"); path != "" {
		_, err := readFile(context.Background(), path, 4096, func(name string) (inputFile, error) {
			if err := os.Rename(name, name+".original"); err != nil {
				return nil, err
			}
			if err := syscall.Mkfifo(name, 0600); err != nil {
				return nil, err
			}
			return openInput(name)
		})
		if !errors.Is(err, command.ErrExecution) {
			t.Fatal("special-file replacement not rejected", err)
		}
		return
	}
	path := filepath.Join(t.TempDir(), "input.json")
	if err := os.WriteFile(path, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	process := exec.CommandContext(ctx, binary, "-test.run=^TestFIFOReplacement$")
	process.Env = append(os.Environ(), "FATHOMRY_WORKFLOW_FIFO_PROBE="+path)
	output, err := process.CombinedOutput()
	if err != nil || ctx.Err() != nil {
		t.Fatalf("file open blocked or failed: %v %v %s", err, ctx.Err(), output)
	}
}
