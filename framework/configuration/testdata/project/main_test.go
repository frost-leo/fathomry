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

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/framework/configuration"
)

func fixture(t *testing.T, root, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

// This consumer intentionally retains the original declarations and expected
// defaults. A library-only change must not silently change their interpretation.
func TestConsumerContract(t *testing.T) {
	root := t.TempDir()
	t.Setenv("FATHOMRY_EXAMPLE_LABEL", "")
	fixture(t, root, "settings.yaml", "label: base\nheaders: {X-Tenant: preserved}\nitems: [base]")
	fixture(t, root, "environment.yaml", "timeout_seconds: 60")
	fixture(t, root, "settings.local.yaml", "headers: {Local: added}\nitems: []")
	loaded, err := loadProject(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	value, err := loaded.Value()
	if err != nil || value.Label != "" || value.TimeoutSeconds != 60 || value.Items == nil || len(value.Items) != 0 ||
		!reflect.DeepEqual(value.Headers, map[string]string{"Default": "kept", "X-Tenant": "preserved", "Local": "added"}) {
		t.Fatal("original interpretation changed")
	}
	value.Headers["Default"] = "changed"
	again, _ := loaded.Value()
	if again.Headers["Default"] != "kept" {
		t.Fatal("consumer received shared mutable settings")
	}
	if loaded.Description().SchemaVersion != 1 || loaded.Description().Provider != "viper" {
		t.Fatal("data schema and acquisition identity changed")
	}
}

func TestConsumerRefusalsAndDefaults(t *testing.T) {
	t.Setenv("FATHOMRY_EXAMPLE_LABEL", "")
	root := t.TempDir()
	_, err := loadProject(context.Background(), root)
	if !errors.Is(err, configuration.Unavailable) {
		t.Fatal("required base became optional")
	}
	fixture(t, root, "settings.yaml", "{}")
	loaded, err := loadProject(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	value, _ := loaded.Value()
	if value.TimeoutSeconds != 30 || len(value.Items) != 1 || value.Headers["Default"] != "kept" {
		t.Fatal("defaults changed")
	}
	fixture(t, root, "settings.yaml", "unknown: 1")
	_, err = loadProject(context.Background(), root)
	if !errors.Is(err, configuration.Invalid) {
		t.Fatal("loader ignored an unknown field")
	}
	fixture(t, root, "settings.yaml", "timeout_seconds: 0")
	_, err = loadProject(context.Background(), root)
	if !errors.Is(err, configuration.ValidationFailed) || !errors.Is(err, failure.Code("example.settings.invalid_timeout")) {
		t.Fatal("validator identity changed")
	}
}
