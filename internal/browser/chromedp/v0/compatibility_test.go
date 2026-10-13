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
	base := strings.TrimSpace(string(run("env", "GOTMPDIR")))
	if base == "" {
		base = filepath.Join(os.TempDir(), "fathomry")
	}
	jobs := filepath.Join(filepath.Dir(base), "jobs")
	if err := os.MkdirAll(jobs, 0700); err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp(jobs, "build.")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(directory); err != nil {
			t.Error(err)
		}
	})
	binary := filepath.Join(directory, "consumer")
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
	if result.Go != runtime.Version() || result.Provider != ProviderID || result.Framework != compatibility.FrameworkModule || len(result.SDKs) != 2 || result.SDKs["github.com/chromedp/chromedp"] != "v0.21.0" || result.SDKs["github.com/chromedp/cdproto"] != "v0.157.9" {
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
	check(Source{}, "Profile")
	check(f.client, "EvidenceBytes", "Run", "Retain", "Profile", "LaunchArgumentsCopy")
	check(unitSession(t), "Context", "Actions", "Protocol", "Navigate", "Save", "NextEvent", "PDFStream", "LoadResource", "Control", "NewObjectGroup", "Object", "Storage", "StoragePartition", "PageClosed", "ExecutionContexts", "Frames", "CreateWorld")
	check(&StorageScope{}, "Cache", "StorageKey", "Bucket")
	check(&StorageCache{})
	check(&Node{}, "DataCopy")
	check(&ExecutionContext{}, "DataCopy")
	check(&Frame{}, "DataCopy")
	check(&Trace{}, "Stop", "Read", "Close", "Closed", "Stopping", "Err", "Completion")
	check(&KeptBrowser{}, "Endpoint", "ProfileDirectory", "ProcessExited", "Close", "Closed")
	check(&Context{}, "Context", "Page", "NewPage", "Targets", "Attach", "Receipt", "Close", "ClosePage", "Cookies", "SetCookies", "ClearCookies", "SetPermission", "ResetPermissions")
	check(&TargetRef{}, "DataCopy")
	check(&Stream{}, "Read", "ReadAt", "Close", "Closed", "Bytes")
	check(&Subscription{}, "Close", "Closed", "Done", "Err")
	check(&Binding{}, "Cancel", "Close", "Closed", "Stopping", "Err")
	check(&ObjectGroup{}, "Cancel", "Close", "Closed", "Stopping", "Err", "Settled", "EvaluateIn", "EvaluateFrame", "ReadIndexedDB")
	check(&Object{}, "Call", "Properties", "Close", "Closed", "DataCopy", "NativeValue", "Group")
	check(&Control{}, "Next", "Close", "Closed", "Stopping", "Err", "EvidenceCopy", "StopRecording", "ReadRecording", "ReadRecordingAt", "SetBlackboxContexts")
	check(Pending{}, "Method", "DataCopy", "Reply", "Download", "ResponseBody", "ResponseBodyStream", "Request", "ServiceWorker", "Script", "CallFrame")
	check(Script{}, "DataCopy")
	check(CallFrame{}, "DataCopy", "SetVariable", "SetReturnValue")
	check(Request{})
	check(ServiceWorker{}, "DataCopy")
	check(&Window{}, "DataCopy")
	check(Download{}, "Progress", "Read")
	check(Reply{})
	check(Chunk{}, "DataCopy", "EOF")
	check(Result{}, "CallbackCompleted", "ContextReleased", "ContextDetached", "Commands", "Navigations", "PagesClosed", "PagesDetached", "RequestEvents", "EventOverflow", "DataCopy")
	check(Event{}, "Type", "DataCopy")
	check(&executorGrant{}, "Call", "Subscribe")
	conformance.Facade(t, bareContext{context.Background()}, "Deadline", "Done", "Err", "Value")
}

func TestProfileRejectsMalformedObservedVersion(t *testing.T) {
	for _, value := range []string{"", ".", ".1", "1.", "1..3", "1a.3", "private-version"} {
		if numericVersion(value) {
			t.Fatal("malformed version certified", value)
		}
	}
	for _, value := range []string{"0", "1.3", "150.0.7871.114"} {
		if !numericVersion(value) {
			t.Fatal("valid observed version rejected", value)
		}
	}
}
