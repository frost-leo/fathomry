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

package tlsclient

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

	sdk "github.com/bogdanfinn/tls-client"
	"github.com/frost-leo/fathomry/internal/compatibility"
)

func TestProviderConsumingBuildAndIndependentImports(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "consumer")
	goBinary := filepath.Join(runtime.GOROOT(), "bin", "go")
	command := exec.CommandContext(ctx, goBinary, "build", "-o", binary, "./testdata/consumer")
	command.Env = append(os.Environ(), "GOTOOLCHAIN=local", "GOWORK=off", "GOPROXY=off", "GOSUMDB=off", "GOFLAGS=")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("consumer build: %v %s", err, output)
	}
	output, err := exec.CommandContext(ctx, binary).Output()
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Go, Provider, Framework, Patch string
		SDKs                           map[string]string
		Replaced                       bool
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatal(err)
	}
	if result.Go != runtime.Version() || result.Provider != ProviderID || result.Framework != compatibility.FrameworkModule ||
		result.Patch != sdk.FathomryCompatibilityRevision || result.SDKs["github.com/bogdanfinn/tls-client"] != "v1.16.0" || len(result.SDKs) != 4 || !result.Replaced {
		t.Fatal("actual build/replacement facts differ")
	}
	command = exec.CommandContext(ctx, goBinary, "list", "-deps", "-f", "{{.ImportPath}}", "./testdata/consumer")
	command.Env = append(os.Environ(), "GOTOOLCHAIN=local", "GOWORK=off", "GOPROXY=off", "GOSUMDB=off", "GOFLAGS=")
	output, err = command.Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"/httpclient/nethttp/", "httpcloak", "chromedp", "nukilabs", "enetx"} {
		if strings.Contains(string(output), name) {
			t.Fatal("unselected Provider imported", name)
		}
	}
}
func TestProviderProfileDoesNotCertifyServicesOrDiscloseSecrets(t *testing.T) {
	options := providerOptions()
	options.ProxyURL = "http://private-user:private-secret@invalid.example:8080"
	fixture := bindProvider(t, options, 1)
	profile := fixture.client.Profile()
	if profile.ServiceVersion.Kind != compatibility.UnknownFact || profile.Protocol.Kind != compatibility.Declared {
		t.Fatal("unobserved service/protocol certified")
	}
	for _, option := range profile.Options {
		if strings.Contains(option.Value, "private") || strings.Contains(option.Value, "invalid.example") {
			t.Fatal("private native configuration in compatibility profile")
		}
	}
}
