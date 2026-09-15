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

package httpcloak

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

func TestConsumingBinaryAndIndependentProviderImports(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	goBinary := filepath.Join(runtime.GOROOT(), "bin", "go")
	binary := filepath.Join(t.TempDir(), "consumer")
	env := append(os.Environ(), "GOTOOLCHAIN=local", "GOWORK=off", "GOPROXY=off", "GOSUMDB=off", "GOFLAGS=")
	command := exec.CommandContext(ctx, goBinary, "build", "-o", binary, "./testdata/consumer")
	command.Env = env
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("consumer build: %v\n%s", err, output)
	}
	output, err := exec.CommandContext(ctx, binary).Output()
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Go, Provider, Framework, Patch string
		SDKs                           map[string]string
		Replacements                   map[string]bool
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatal(err)
	}
	if result.Go != runtime.Version() || result.Provider != ProviderID || result.Patch != "v1" || result.SDKs["github.com/sardanioss/httpcloak"] != "v1.7.2" || len(result.SDKs) != 6 {
		t.Fatal("actual consuming build facts changed")
	}
	for _, module := range []string{"github.com/sardanioss/httpcloak", "github.com/sardanioss/quic-go", "github.com/sardanioss/net", "github.com/sardanioss/udpbara"} {
		if !result.Replacements[module] {
			t.Fatal("replacement not observed")
		}
	}
	command = exec.CommandContext(ctx, goBinary, "list", "-deps", "-f", "{{.ImportPath}}", "./testdata/consumer")
	command.Env = env
	output, err = command.Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, unselected := range []string{"/httpclient/nethttp/", "/httpclient/tlsclient/", "github.com/bogdanfinn/", "chromedp", "nukilabs", "enetx"} {
		if strings.Contains(string(output), unselected) {
			t.Fatal("unselected provider imported")
		}
	}
}
