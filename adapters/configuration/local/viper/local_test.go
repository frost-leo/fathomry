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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	local "github.com/frost-leo/fathomry/adapters/configuration/local/viper"
	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/framework/configuration"
)

func write(t *testing.T, root, path, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, path), []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}
func source(t *testing.T, root string, files ...local.File) *local.Provider {
	t.Helper()
	provider, err := local.New(local.Options{Root: root, Files: files})
	if err != nil {
		t.Fatal(err)
	}
	return provider
}
func assertEmpty(t *testing.T, input configuration.Input) {
	t.Helper()
	if input.Provider != "" || input.SchemaVersion != 0 || input.Documents != nil {
		t.Fatal("failed acquisition returned a prefix")
	}
}

func TestOptionsFreezeAndIndependentReads(t *testing.T) {
	root := t.TempDir()
	const original = "{\"Headers\":{\"X-Tenant\":\"selected\"},\"large\":18446744073709551615}"
	write(t, root, "settings.json", original)
	options := local.Options{Root: root, SchemaVersion: 2, Files: []local.File{{Name: "base", Path: "settings.json", Layer: configuration.Base, Encoding: "json"}}}
	provider, err := local.New(options)
	if err != nil {
		t.Fatal(err)
	}
	options.Files[0].Path = "missing"
	options.Files[0].Name = "modified"
	options.SchemaVersion = 3
	input, err := provider.ReadConfiguration(context.Background())
	if err != nil || input.SchemaVersion != 2 || input.Provider != local.ProviderID || len(input.Documents) != 1 || string(input.Documents[0].Data) != original || input.Documents[0].Name != "base" {
		t.Fatal("adapter changed raw bytes or aliased declarations")
	}
	input.Documents[0].Data[0] = 'x'
	next, err := provider.ReadConfiguration(context.Background())
	if err != nil || string(next.Documents[0].Data) != original {
		t.Fatal("independent acquisition reused mutable data")
	}
	write(t, root, "settings.json", "{}")
	next, err = provider.ReadConfiguration(context.Background())
	if err != nil || string(next.Documents[0].Data) != "{}" {
		t.Fatal("adapter reused stale configuration")
	}
}

func TestOptionalAbsenceIsNotInvalidContent(t *testing.T) {
	root := t.TempDir()
	write(t, root, "base.yaml", "name: base")
	provider := source(t, root,
		local.File{Name: "optional", Path: "override.yaml", Layer: configuration.Local, Optional: true},
		local.File{Name: "base", Path: "base.yaml", Layer: configuration.Base},
	)
	input, err := provider.ReadConfiguration(context.Background())
	if err != nil || len(input.Documents) != 2 || input.Documents[0].Name != "base" || !input.Documents[1].Absent || len(input.Documents[1].Data) != 0 {
		t.Fatal("optional absence or order lost")
	}
	const secret = "private-adapter-canary"
	write(t, root, "override.yaml", "secret: "+secret+"\nbroken: [")
	input, err = provider.ReadConfiguration(context.Background())
	if !errors.Is(err, configuration.Invalid) || strings.Contains(fmt.Sprintf("%#v", err), secret) || strings.Contains(fmt.Sprintf("%+v", err), root) {
		t.Fatal("malformed optional source accepted or disclosed")
	}
	assertEmpty(t, input)
	occurrence, ok := failure.Inspect(err)
	if !ok || !reflect.DeepEqual(occurrence.Diagnostic().Attributes, []failure.Attribute{{Name: "provider", Value: "viper"}, {Name: "source", Value: "optional"}}) {
		t.Fatal("safe failure source missing")
	}
}

func TestRequiredDirectoryBoundsAndCancellation(t *testing.T) {
	root := t.TempDir()
	provider := source(t, root, local.File{Name: "base", Path: "absent", Layer: configuration.Base})
	input, err := provider.ReadConfiguration(context.Background())
	if !errors.Is(err, configuration.Unavailable) || !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("required absence not classified")
	}
	var pathError *fs.PathError
	if errors.As(err, &pathError) {
		t.Fatal("native filesystem path escaped")
	}
	assertEmpty(t, input)
	if err := os.Mkdir(filepath.Join(root, "directory"), 0700); err != nil {
		t.Fatal(err)
	}
	provider = source(t, root, local.File{Name: "base", Path: "directory", Layer: configuration.Base, Optional: true})
	input, err = provider.ReadConfiguration(context.Background())
	if !errors.Is(err, configuration.Invalid) {
		t.Fatal("directory accepted as an optional missing file")
	}
	assertEmpty(t, input)
	write(t, root, "large.yaml", strings.Repeat("x", configuration.MaxDocumentBytes+1))
	provider = source(t, root, local.File{Name: "base", Path: "large.yaml", Layer: configuration.Base})
	input, err = provider.ReadConfiguration(context.Background())
	if !errors.Is(err, configuration.LimitExceeded) {
		t.Fatal("oversized file accepted")
	}
	assertEmpty(t, input)
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("caller cancellation")
	cancel(cause)
	input, err = provider.ReadConfiguration(ctx)
	if !errors.Is(err, configuration.Cancelled) || !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
		t.Fatal("cancellation evidence lost")
	}
	assertEmpty(t, input)
	input, err = provider.ReadConfiguration(nil)
	if !errors.Is(err, configuration.InvalidInput) {
		t.Fatal("nil Context accepted")
	}
	assertEmpty(t, input)
	var absent *local.Provider
	input, err = absent.ReadConfiguration(context.Background())
	if !errors.Is(err, configuration.InvalidInput) {
		t.Fatal("nil Provider accepted")
	}
	assertEmpty(t, input)
}

func TestDeclarationsRejectBeforeAcquisition(t *testing.T) {
	root := t.TempDir()
	valid := local.File{Name: "base", Path: "never-created.yaml", Layer: configuration.Base}
	cases := []local.Options{
		{Root: root + string(filepath.Separator) + "child" + string(filepath.Separator) + ".."},
		{}, {Root: "relative"}, {Root: root, Files: []local.File{{Name: "bad/path", Path: "x", Layer: configuration.Base}}},
		{Root: root, Files: []local.File{{Name: "base", Path: "../outside", Layer: configuration.Base}}},
		{Root: root, Files: []local.File{{Name: "base", Path: "/absolute", Layer: configuration.Base}}},
		{Root: root, Files: []local.File{{Name: "base", Path: "a\\b", Layer: configuration.Base}}},
		{Root: root, Files: []local.File{{Name: "base", Path: ".", Layer: configuration.Base}}},
		{Root: root, Files: []local.File{{Name: "base", Path: "x", Layer: configuration.Defaults}}},
		{Root: root, Files: []local.File{{Name: "base", Path: "x", Layer: configuration.Variables}}},
		{Root: root, Files: []local.File{{Name: "base", Path: "x", Layer: configuration.Base, Encoding: "toml"}}},
		{Root: root, Files: []local.File{valid, valid}},
		{Root: root, Files: make([]local.File, 4)},
	}
	for index, options := range cases {
		provider, err := local.New(options)
		if provider != nil || !errors.Is(err, configuration.InvalidInput) {
			t.Fatalf("invalid declaration %d accepted", index)
		}
	}
	provider, err := local.New(local.Options{Root: filepath.Join(root, "nonexistent"), Files: []local.File{valid}})
	if err != nil || provider == nil {
		t.Fatal("constructor opened files")
	}
	defaults := source(t, filepath.Join(root, "not-a-real-project"))
	input, err := defaults.ReadConfiguration(context.Background())
	if err != nil || input.SchemaVersion != 1 || len(input.Documents) != 0 {
		t.Fatal("explicit defaults-only provider changed")
	}
}

func TestAdapterRuntimePrivacy(t *testing.T) {
	const secret = "private-path-canary"
	provider := source(t, t.TempDir(), local.File{Name: "base", Path: secret, Layer: configuration.Base})
	for _, value := range []any{local.Options{Root: secret}, local.File{Path: secret}, provider} {
		if _, ok := value.(fmt.Formatter); !ok {
			t.Fatalf("formatter missing: %T", value)
		}
		if strings.Contains(fmt.Sprintf("%#v", value), secret) {
			t.Fatal("runtime path exposed")
		}
		var output bytes.Buffer
		slog.New(slog.NewJSONHandler(&output, nil)).Info("probe", "value", value)
		if strings.Contains(output.String(), secret) {
			t.Fatal("runtime path logged")
		}
		if _, err := json.Marshal(value); err == nil {
			t.Fatal("runtime JSON accepted")
		}
	}
	options := local.Options{Root: "original"}
	if err := json.Unmarshal([]byte("{}"), &options); err == nil || options.Root != "original" {
		t.Fatal("runtime reconstruction changed options")
	}
}

func TestConcurrentPublicLoads(t *testing.T) {
	root := t.TempDir()
	write(t, root, "base.yaml", "connection: {host: base, port: 443}\nlarge: 18446744073709551615")
	type settings struct {
		Connection struct {
			Host string `json:"host"`
			Port int    `json:"port"`
		} `json:"connection"`
		Large uint64 `json:"large"`
	}
	provider := source(t, root, local.File{Name: "base", Path: "base.yaml", Layer: configuration.Base})
	var wait sync.WaitGroup
	for range 12 {
		wait.Go(func() {
			for range 4 {
				loaded, err := configuration.Load(context.Background(), configuration.Schema[settings]{SchemaVersion: 1}, configuration.Request{Provider: provider})
				if err != nil {
					t.Error(err)
					return
				}
				actual, err := loaded.Value()
				if err != nil || actual.Large != ^uint64(0) || actual.Connection.Port != 443 {
					t.Error("native load changed exact settings")
				}
			}
		})
	}
	wait.Wait()
}
