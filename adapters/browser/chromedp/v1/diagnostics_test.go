/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package chromedp_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	browser "github.com/frost-leo/fathomry/adapters/browser/chromedp/v1"
)

func TestPublicDiagnosticsDoNotDiscloseRuntimeInputs(t *testing.T) {
	const canary = "private-browser-diagnostic"
	prepared, err := browser.Prepare(browser.Settings{Name: "diagnostics", ExecPath: "/browser", Flags: map[string]string{"user-agent": canary}}, browser.NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	values := []any{
		prepared, browser.Settings{RemoteURL: "wss://" + canary + "/devtools/browser/id", Headers: map[string][]string{"Authorization": {canary}}},
		browser.ContextOptions{ProxyServer: "http://" + canary, ContextID: canary, TargetID: canary},
		browser.Evaluate[string](canary), browser.Result{}, browser.Chunk{}, browser.Context{}, browser.Stream{},
		browser.ValueArgument(canary), browser.UnserializableArgument(canary), browser.ObjectArgument(nil),
		browser.StorageScope{}, browser.StorageCache{}, browser.Trace{}, browser.KeptBrowser{}, browser.Node{},
		browser.Download{}, browser.DownloadOptions{LocalDirectory: canary},
		browser.Frame{}, browser.ExecutionContext{},
	}
	for _, value := range values {
		for _, verb := range []string{"%v", "%+v", "%#v"} {
			if strings.Contains(fmt.Sprintf(verb, value), canary) {
				t.Fatalf("ordinary diagnostics disclosed %T", value)
			}
		}
		var output bytes.Buffer
		slog.New(slog.NewJSONHandler(&output, nil)).Info("diagnostics", "value", value)
		if strings.Contains(output.String(), canary) {
			t.Fatalf("slog disclosed %T", value)
		}
	}
	for _, value := range []any{prepared, browser.Result{}, browser.Chunk{}, browser.Context{}, browser.Stream{}, browser.Evaluate[int]("1")} {
		if _, err := json.Marshal(value); !errors.Is(err, browser.ErrSerialization) {
			t.Fatalf("runtime value serialized: %T: %v", value, err)
		}
	}
}
