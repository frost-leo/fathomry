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

package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	remote "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1"
	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
	local "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
)

func sourcePrivate(t testing.TB, value any) {
	t.Helper()
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%d"} {
		output := fmt.Sprintf(format, value)
		if strings.Contains(output, "private-canary") || strings.Contains(output, "PANIC") {
			t.Errorf("unsafe source formatting: %T", value)
		}
	}
	for _, asJSON := range []bool{false, true} {
		var output bytes.Buffer
		var handler slog.Handler = slog.NewTextHandler(&output, nil)
		if asJSON {
			handler = slog.NewJSONHandler(&output, nil)
		}
		slog.New(handler).Info("privacy", "value", value)
		if strings.Contains(output.String(), "private-canary") || strings.Contains(output.String(), "panicked") {
			t.Errorf("unsafe source logging: %T", value)
		}
	}
}
func TestSourcePrivacyCopyingAndNativeCauseAccess(t *testing.T) {
	directory := t.TempDir()
	path := file(t, directory, "private-canary.yaml", "private-canary")
	selected := selected(t, "source", path, time.Second)
	batch, err := selected.Capture(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	observer, err := selected.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeOwner(t, observer.Close) })
	state := awaitRaw(t, observer, func(state source.State) bool { return state.Status == source.Available })
	remoteSelection, err := remote.Select(remote.Settings{Name: "remote", Servers: []remote.Server{{HTTPURL: "http://private-canary.invalid/nacos", GRPCAddress: "private-canary.invalid:9848"}}, Documents: []remote.Document{{Name: "document", DataID: "private-canary"}}, AllowInsecure: true, Username: "reader", Password: "private-canary"})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{selected, remoteSelection, batch, observer, state, &state, state.Cursor} {
		sourcePrivate(t, value)
		if _, err := json.Marshal(value); err == nil {
			t.Errorf("source runtime serialized: %T", value)
		}
		kind := reflect.TypeOf(value)
		if kind.Kind() == reflect.Pointer {
			target := reflect.New(kind.Elem()).Interface()
			if err := json.Unmarshal([]byte("{}"), target); err == nil {
				t.Errorf("source runtime reconstructed: %T", value)
			}
			nilValue := reflect.Zero(kind).Interface()
			sourcePrivate(t, nilValue)
		}
	}
	nilSelection := reflect.Zero(reflect.TypeOf(selected)).Interface().(source.Selection)
	if _, err := nilSelection.Capture(context.Background()); !errors.Is(err, source.ErrValue) {
		t.Fatal("nil selection Capture", err)
	}
	nilObserver := reflect.Zero(reflect.TypeOf(observer)).Interface().(source.Observer)
	if _, err := nilObserver.Current(); !errors.Is(err, source.ErrValue) {
		t.Fatal("nil observer Current", err)
	}
	nilCursor := reflect.Zero(reflect.TypeOf(state.Cursor)).Interface().(source.Cursor)
	if _, err := observer.Next(context.Background(), nilCursor); !errors.Is(err, source.ErrCursor) {
		t.Fatal("typed nil cursor admitted", err)
	}
	blocked := filepath.Join(path, "private-canary")
	bad, err := local.Select(local.Settings{Name: "blocked", Documents: []local.File{{Name: "slot", Path: blocked, Encoding: "yaml"}}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = bad.Capture(context.Background())
	if err == nil {
		t.Fatal("invalid filesystem path accepted")
	}
	sourcePrivate(t, err)
	var native *os.PathError
	if !errors.As(err, &native) || native.Path != blocked {
		t.Fatal("documented sensitive OS cause missing")
	}
	settings := remote.Settings{Password: "private-canary"}
	data, err := json.Marshal(settings)
	if err != nil || !strings.Contains(string(data), "private-canary") {
		t.Fatal("plain bootstrap DTO was guarded", err)
	}
}
