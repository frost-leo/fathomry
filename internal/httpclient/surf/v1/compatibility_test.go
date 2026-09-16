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

package surf

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/conformance"
)

func TestConsumingBinaryReportsExactSelectionsAndIndependentImport(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("source absent")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "../../../.."))
	goBinary := filepath.Join(runtime.GOROOT(), "bin", "go")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	environment := append(os.Environ(), "GOTOOLCHAIN=local", "GOWORK=off", "GOPROXY=off", "GOSUMDB=off")
	binary := filepath.Join(t.TempDir(), "consumer")
	build := exec.CommandContext(ctx, goBinary, "build", "-mod=readonly", "-o", binary, "./internal/httpclient/surf/v1/testdata/consumer")
	build.Dir, build.Env = root, environment
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("consumer build: %v\n%s", err, output)
	}
	output, err := exec.CommandContext(ctx, binary).Output()
	if err != nil {
		t.Fatal(err)
	}
	var value struct {
		Go, Provider, Framework string
		SDKs                    map[string]string
		Replaced                map[string]bool
	}
	if err := json.Unmarshal(output, &value); err != nil {
		t.Fatal(err)
	}
	if value.Provider != ProviderID || value.Framework != compatibility.FrameworkModule || !strings.HasPrefix(value.Go, "go1.27") {
		t.Fatal("consumer identity changed", value.Go)
	}
	for module, version := range map[string]string{"github.com/enetx/surf": "v1.0.206", "github.com/enetx/http2": "v1.0.26", "github.com/enetx/http3": "v1.0.9"} {
		if value.SDKs[module] != version || !value.Replaced[module] {
			t.Fatal("actual native selection/replacement absent", module)
		}
	}
	inventory := exec.CommandContext(ctx, goBinary, "list", "-deps", "./internal/httpclient/surf/v1")
	inventory.Dir, inventory.Env = root, environment
	data, err := inventory.Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Fields(string(data)) {
		if strings.Contains(line, "bogdanfinn") || strings.Contains(line, "httpclient/nethttp") || strings.Contains(line, "httpclient/tlsclient") {
			t.Fatal("independent Surf import selected another provider", line)
		}
	}
}
func TestProfileDisclosesControlsNotNativeSecrets(t *testing.T) {
	options := OptionsV1{Name: "profile", ProxyURL: "http://owner:profile-secret@127.0.0.1:1"}
	fixture := newFixture(t, options, 1)
	profile := fixture.client.Profile()
	conformance.Private(t, profile, "profile-secret", "127.0.0.1")
	fields := make(map[string]string)
	for _, option := range profile.Options {
		fields[option.Name] = option.Value
	}
	if fields["surf-compatibility"] != "v1" || fields["http2-compatibility"] != "v1" || fields["http3-compatibility"] != "v1" || fields["h3-ja-fingerprinting"] != "false" {
		t.Fatal("profile lost native compatibility axes")
	}
}
