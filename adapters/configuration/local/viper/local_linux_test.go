/*
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

package viper_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	local "github.com/frost-leo/fathomry/adapters/configuration/local/viper"
	"github.com/frost-leo/fathomry/framework/configuration"
)

func TestNonregularFileDoesNotBlock(t *testing.T) {
	root := t.TempDir()
	if err := syscall.Mkfifo(filepath.Join(root, "pipe"), 0600); err != nil {
		t.Fatal(err)
	}
	provider := source(t, root, local.File{Name: "base", Path: "pipe", Layer: configuration.Base})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	input, err := provider.ReadConfiguration(ctx)
	if !errors.Is(err, configuration.Invalid) || ctx.Err() != nil {
		t.Fatal("nonregular input was opened or accepted")
	}
	assertEmpty(t, input)
}

func TestPermissionDeniedIsNotOptionalAbsence(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission refusal requires a non-root identity")
	}
	root := t.TempDir()
	write(t, root, "denied.yaml", "value: private")
	if err := os.Chmod(filepath.Join(root, "denied.yaml"), 0); err != nil {
		t.Fatal(err)
	}
	provider := source(t, root, local.File{Name: "base", Path: "denied.yaml", Layer: configuration.Base, Optional: true})
	input, err := provider.ReadConfiguration(context.Background())
	if !errors.Is(err, configuration.Unavailable) || !errors.Is(err, fs.ErrPermission) {
		t.Fatal("permission denial was treated as optional absence")
	}
	assertEmpty(t, input)
}
