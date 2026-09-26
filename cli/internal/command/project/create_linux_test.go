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

package project

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestLinuxSourceFIFOIsRefusedWithoutReading(t *testing.T) {
	input := inputFor(t)
	input.source = t.TempDir()
	if err := syscall.Mkfifo(filepath.Join(input.source, "go.mod"), 0o600); err != nil {
		t.Fatal(err)
	}
	effect, err := create(context.Background(), input)
	if effect != untouched || !errors.Is(err, errSource) {
		t.Fatalf("FIFO accepted: %v %v", effect, err)
	}
	assertAbsent(t, input.directory)
}

func TestLinuxPermissionFailures(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission denial requires an unprivileged user")
	}
	for _, stage := range []string{"parent", "source"} {
		t.Run(stage, func(t *testing.T) {
			input := inputFor(t)
			restricted := filepath.Dir(input.directory)
			mode := os.FileMode(0o500)
			if stage == "source" {
				input.source = t.TempDir()
				restricted = filepath.Join(input.source, "go.mod")
				mustWrite(t, restricted, "module "+frameworkModule+"\ngo 1.27.0\n")
				mode = 0
			}
			if err := os.Chmod(restricted, mode); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := os.Chmod(restricted, 0o700); err != nil {
					t.Error(err)
				}
			})
			effect, err := create(context.Background(), input)
			if effect != untouched || !errors.Is(err, os.ErrPermission) {
				t.Fatalf("permission failure: %v %v", effect, err)
			}
			assertAbsent(t, input.directory)
		})
	}
}
