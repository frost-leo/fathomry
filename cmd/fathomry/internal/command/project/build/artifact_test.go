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

package build

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPublication(t *testing.T) {
	if !supportedHost {
		t.Skip("native publication host not supported")
	}
	directory := t.TempDir()
	source, destination := filepath.Join(directory, "source"), filepath.Join(directory, "bin", "health")
	if err := os.WriteFile(source, []byte("inspected bytes"), 0700); err != nil {
		t.Fatal(err)
	}
	digest, err := executableHash(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	published, err := publish(context.Background(), source, destination, digest)
	if err != nil || !published {
		t.Fatal(published, err)
	}
	if raw, err := os.ReadFile(destination); err != nil || string(raw) != "inspected bytes" {
		t.Fatal(err, string(raw))
	}
	if info, err := os.Stat(destination); err != nil || info.Mode().Perm() != 0700 {
		t.Fatal("private executable permissions widened", info, err)
	}
	if err := os.WriteFile(source, []byte("changed bytes"), 0700); err != nil {
		t.Fatal(err)
	}
	published, err = publish(context.Background(), source, destination, digest)
	if err == nil || published {
		t.Fatal("mutated binary published")
	}
	if raw, _ := os.ReadFile(destination); string(raw) != "inspected bytes" {
		t.Fatal("old artifact overwritten")
	}
	entries, err := os.ReadDir(filepath.Dir(destination))
	if err != nil || len(entries) != 1 {
		t.Fatal("staging leaked", entries, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if published, err := publish(ctx, source, destination, digest); published || !errors.Is(err, context.Canceled) {
		t.Fatal(published, err)
	}
	if published, err := publish(context.Background(), source, source, digest); published || err == nil {
		t.Fatal("self-publication allowed")
	}
}

func TestPublicationPermissions(t *testing.T) {
	if !supportedHost {
		t.Skip("native publication host not supported")
	}
	for _, mode := range []fs.FileMode{0700, 0750, 0755, 0711, 0700 | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky} {
		t.Run(mode.String(), func(t *testing.T) {
			directory := t.TempDir()
			source, output := filepath.Join(directory, "source"), filepath.Join(directory, "output")
			if err := os.WriteFile(source, []byte("inspected bytes"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(source, mode); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(output, []byte("previous artifact"), 0755); err != nil {
				t.Fatal(err)
			}
			digest, err := executableHash(context.Background(), source)
			if err != nil {
				t.Fatal(err)
			}
			if published, err := publish(context.Background(), source, output, digest); !published || err != nil {
				t.Fatal(published, err)
			}
			info, err := os.Stat(output)
			if err != nil || info.Mode() != mode.Perm() {
				t.Fatal("ordinary permissions not preserved or special bits copied", info, err)
			}
		})
	}
}

func TestPublicationSpecialFiles(t *testing.T) {
	if !supportedHost {
		t.Skip("native publication host not supported")
	}
	directory := t.TempDir()
	source, output, target := filepath.Join(directory, "source"), filepath.Join(directory, "output"), filepath.Join(directory, "target")
	if err := os.WriteFile(source, []byte("binary"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("untouched"), 0600); err != nil {
		t.Fatal(err)
	}
	hash, err := executableHash(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, output); err != nil {
		t.Fatal(err)
	}
	if ok, err := publish(context.Background(), source, output, hash); ok || err == nil {
		t.Fatal("symlink accepted")
	}
	if raw, _ := os.ReadFile(target); string(raw) != "untouched" {
		t.Fatal("symlink followed")
	}
	if err := os.Remove(output); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(source, output); err != nil {
		t.Fatal(err)
	}
	if ok, err := publish(context.Background(), source, output, hash); ok || err == nil {
		t.Fatal("hard-link alias accepted")
	}
	if err := os.Remove(output); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	if ok, err := publish(context.Background(), source, output, hash); ok || err == nil {
		t.Fatal("directory accepted")
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".fathomry-") {
			t.Fatal("staging leaked")
		}
	}
	if _, err := os.Stat(filepath.Join(output, "health")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
}
