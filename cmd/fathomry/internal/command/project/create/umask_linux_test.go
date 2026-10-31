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
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
)

func TestCreationRespectsUmask(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mask := range []string{"0000", "0077"} {
		t.Run(mask, func(t *testing.T) {
			directory := t.TempDir()
			command := exec.CommandContext(t.Context(), executable, "-test.run=^TestUmaskChild$", "-test.count=1")
			command.Env = append(os.Environ(), "FATHOMRY_SCAFFOLD_UMASK_CHILD="+directory, "FATHOMRY_SCAFFOLD_UMASK="+mask)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("umask subprocess: %v\n%s", err, output)
			}
		})
	}
}

func TestUmaskChild(t *testing.T) {
	directory := os.Getenv("FATHOMRY_SCAFFOLD_UMASK_CHILD")
	if directory == "" {
		return
	}
	mask, err := strconv.ParseUint(os.Getenv("FATHOMRY_SCAFFOLD_UMASK"), 8, 9)
	if err != nil {
		t.Fatal(err)
	}
	syscall.Umask(int(mask))
	parent, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	result, err := createInParent(context.Background(), parent, prepared(t), openFile)
	if err != nil || result.State != "complete" {
		t.Fatal(result, err)
	}
	for _, entry := range []struct {
		path string
		mode os.FileMode
	}{
		{"demo", 0755}, {"demo/internal", 0755}, {"demo/internal/bootstrap", 0755}, {"demo/config", 0755},
		{"demo/go.mod", 0644}, {"demo/main.go", 0644}, {"demo/.env.example", 0644}, {"demo/.env", 0600},
	} {
		info, err := os.Stat(filepath.Join(directory, entry.path))
		if err != nil || info.Mode().Perm() != entry.mode&^os.FileMode(mask) {
			t.Fatal("umask ignored", entry.path, err)
		}
	}
}
