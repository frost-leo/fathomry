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

package chromedp

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

func TestActualConsumerDependenciesAndIndependentImports(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "consumer")
	goBinary := filepath.Join(runtime.GOROOT(), "bin", "go")
	run := func(args ...string) []byte {
		t.Helper()
		command := exec.CommandContext(ctx, goBinary, args...)
		command.Env = append(os.Environ(), "GOTOOLCHAIN=local", "GOWORK=off", "GOPROXY=off", "GOSUMDB=off", "GOFLAGS=")
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("consuming build: %v\n%s", err, output)
		}
		return output
	}
	run("build", "-o", binary, "./testdata/consumer")
	output, err := exec.CommandContext(ctx, binary).Output()
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Go, Provider, Framework string
		SDKs                    map[string]string
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatal(err)
	}
	if result.Go != runtime.Version() || result.Provider != ProviderID || result.Framework != compatibility.FrameworkModule || len(result.SDKs) != 3 || result.SDKs["github.com/chromedp/chromedp"] != "v0.16.0" || result.SDKs["github.com/chromedp/cdproto"] != "v0.0.0-20260714215040-dc233986426f" || result.SDKs["github.com/go-json-experiment/json"] != "v0.0.0-20260623181947-01eb4420fa68" {
		t.Fatal("consuming dependency facts differ")
	}
	graph := string(run("list", "-deps", "./testdata/consumer"))
	for _, name := range []string{"/httpclient/", "tls-client", "httpcloak", "nukilabs", "enetx", "/database/", "/tableformat/", "/sqlengine/"} {
		if strings.Contains(graph, name) {
			t.Fatal("unselected SDK imported", name)
		}
	}
}
func TestMethodOnlyFacades(t *testing.T) {
	f := bindFixture(t, inertOptions(), 1)
	if f.client.Profile().Native.Kind != compatibility.UnknownFact {
		t.Fatal("unattested native launch configuration could be certified")
	}
	diagnostic := []string{"Format", "LogValue", "MarshalJSON", "UnmarshalJSON"}
	check := func(value any, methods ...string) {
		t.Helper()
		conformance.Facade(t, value, append(append([]string(nil), diagnostic...), methods...)...)
	}
	check(Source{})
	check(f.client, "EvidenceBytes", "Run", "Profile", "LaunchArgumentsCopy")
	check(unitSession(t), "Context", "Actions", "Navigate", "Save", "NextEvent")
	check(Result{}, "CallbackCompleted", "ContextReleased", "Commands", "Navigations", "RequestEvents", "EventOverflow", "DataCopy")
	check(Event{}, "Type", "DataCopy")
	check(&executorGrant{}, "Execute")
	conformance.Facade(t, bareContext{context.Background()}, "Deadline", "Done", "Err", "Value")
}
