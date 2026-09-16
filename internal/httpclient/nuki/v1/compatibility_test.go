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

package nuki

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
)

func consumerEnvironment() []string {
	return append(os.Environ(), "GOTOOLCHAIN=local", "GOWORK=off", "GOPROXY=off", "GOSUMDB=off", "GOFLAGS=-p=4")
}
func TestProviderActualConsumerAndIndependentImports(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	goBinary := filepath.Join(runtime.GOROOT(), "bin", "go")
	binary := filepath.Join(t.TempDir(), "consumer")
	command := exec.CommandContext(ctx, goBinary, "build", "-mod=readonly", "-o", binary, "./testdata/consumer")
	command.Env = consumerEnvironment()
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("consumer build: %v\n%s", err, output)
	}
	output, err := exec.CommandContext(ctx, binary).Output()
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		Go, Provider, Patch string
		Modules             map[string]string
		Replacements        map[string]bool
	}
	if err := json.Unmarshal(output, &report); err != nil {
		t.Fatal(err)
	}
	wanted := map[string]string{"github.com/nukilabs/tlsclient": "v1.8.8", "github.com/nukilabs/http": "v1.3.2", "github.com/nukilabs/utls": "v1.3.3", "github.com/nukilabs/quic-go": "v1.3.0", "github.com/nukilabs/qpack": "v0.7.0", "github.com/nukilabs/socks": "v1.0.1"}
	if report.Go != runtime.Version() || report.Provider != ProviderID || report.Patch != "v1" || len(report.Modules) != len(wanted) {
		t.Fatal("consuming identity changed")
	}
	for module, version := range wanted {
		if report.Modules[module] != version {
			t.Fatal("actual SDK version changed", module)
		}
		replacement := module != "github.com/nukilabs/http" && module != "github.com/nukilabs/utls"
		if report.Replacements[module] != replacement {
			t.Fatal("replacement fact changed", module)
		}
	}
	command = exec.CommandContext(ctx, goBinary, "list", "-mod=readonly", "-deps", "./testdata/consumer")
	command.Env = consumerEnvironment()
	output, err = command.Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, unselected := range []string{"bogdanfinn", "httpclient/nethttp", "httpcloak", "chromedp", "enetx"} {
		if strings.Contains(string(output), unselected) {
			t.Fatal("unselected Provider imported", unselected)
		}
	}
}
func TestProviderSameProcessWithMergedProviders(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, filepath.Join(runtime.GOROOT(), "bin", "go"), "run", "-mod=readonly", "./testdata/coexist")
	command.Env = consumerEnvironment()
	output, err := command.CombinedOutput()
	if err != nil || strings.TrimSpace(string(output)) != "five-http-providers-and-chromedp-build-and-cleanup" {
		t.Fatalf("combined consuming process: %v\n%s", err, output)
	}
}
