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

package version

import (
	"bytes"
	"context"
	"encoding/json"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/cli"
	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/i18n"
	buildversion "github.com/frost-leo/fathomry/version"
	"github.com/spf13/cobra"
)

func TestVersionProjectionPrivacyAndUnknowns(t *testing.T) {
	const private = "private-canary-path"
	info := &debug.BuildInfo{
		GoVersion: "go1.27.0", Path: "example.org/app/cmd/app",
		Main: debug.Module{Path: "example.org/app", Version: "(devel)"},
		Deps: []*debug.Module{
			{Path: buildversion.FrameworkModule, Version: "v1.2.3", Replace: &debug.Module{Path: "/" + private}},
			{Path: "example.org/" + private, Version: "v4.5.6"},
		},
		Settings: []debug.BuildSetting{
			{Key: "GOOS", Value: "linux"}, {Key: "GOARCH", Value: "amd64"},
			{Key: "-ldflags", Value: private},
		},
	}
	build, err := buildversion.FromBuildInfo(info, buildversion.Request{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := versionJSON(build)
	if err != nil || strings.Contains(output, private) || strings.Contains(output, "ldflags") || strings.Contains(output, "dependencies") {
		t.Fatalf("privacy projection: %v %q", err, output)
	}
	var record wireVersion
	if err := json.Unmarshal([]byte(output), &record); err != nil {
		t.Fatal(err)
	}
	if record.Framework.Replacement == nil || record.Framework.Replacement.Kind != "local" ||
		record.Framework.Replacement.Path.State != "redacted" || record.Framework.Replacement.Path.Value != nil ||
		record.Application.Version.State != "development" || record.Application.Version.Value != nil ||
		record.Source.Tree != "unknown" || record.Source.CommitTime != nil {
		t.Fatalf("incorrect fact states: %+v", record)
	}
	if _, err := json.Marshal(build); err == nil {
		t.Fatal("public version serialization guard weakened")
	}
	resources := append(cli.Resources(), Resources()...)
	catalog, err := i18n.Prepare(resources)
	if err != nil {
		t.Fatal(err)
	}
	for _, locale := range []string{"en", "zh-CN"} {
		human, err := versionText(catalog, locale, build)
		if err != nil || strings.Contains(human, private) || strings.Contains(human, "ldflags") {
			t.Fatalf("private text projection locale=%s error=%v text=%q", locale, err, human)
		}
	}
	unknown, err := versionJSON(buildversion.Build{})
	if err != nil || !strings.Contains(unknown, `"origin":"unknown"`) || !strings.Contains(unknown, `"tree":"unknown"`) {
		t.Fatalf("unknown build projection: %v %s", err, unknown)
	}
}

func testRoot(call *cli.Invocation, command *cobra.Command) *cobra.Command {
	root := call.NewRoot(cli.RootText{
		HeaderID: "fathomry.cli.help.help", FooterID: "fathomry.cli.help.hint",
	})
	call.Add(root, command, HelpID, SummaryID, Resources()...)
	return root
}

func TestInspectionIsLazyAndFailureDoesNotPublish(t *testing.T) {
	inspections := 0
	inspect := func(buildversion.Request) (buildversion.Build, error) {
		inspections++
		return buildversion.Build{}, failure.New(buildversion.InvalidMetadata, nil)
	}
	build := func(call *cli.Invocation) *cobra.Command {
		command := &cobra.Command{Use: "version", RunE: func(*cobra.Command, []string) error {
			return report(call, "json", inspect)
		}}
		return testRoot(call, command)
	}
	var out, diagnostic bytes.Buffer
	status := cli.Run(context.Background(), []string{"version", "--help"}, strings.NewReader(""), &out, &diagnostic, build)
	if status != 0 || inspections != 0 {
		t.Fatalf("help inspected metadata: status=%d inspections=%d", status, inspections)
	}
	out.Reset()
	diagnostic.Reset()
	status = cli.Run(context.Background(), []string{"version"}, strings.NewReader(""), &out, &diagnostic, build)
	if status != 1 || out.Len() != 0 || !strings.Contains(diagnostic.String(), "build metadata is invalid") || inspections != 1 {
		t.Fatalf("inspection failure status=%d stdout=%q stderr=%q inspections=%d", status, out.String(), diagnostic.String(), inspections)
	}
}

func TestJSONDoesNotPrepareHumanResources(t *testing.T) {
	saved := english
	english = "not JSON"
	defer func() { english = saved }()
	build := func(call *cli.Invocation) *cobra.Command {
		return testRoot(call, New(call))
	}
	var out, diagnostic bytes.Buffer
	status := cli.Run(context.Background(), []string{"version", "--output=json"}, strings.NewReader(""), &out, &diagnostic, build)
	if status != 0 || diagnostic.Len() != 0 || !strings.Contains(out.String(), `"schema":"fathomry.cli.version/v1"`) {
		t.Fatalf("catalog-free JSON: status=%d stdout=%q stderr=%q", status, out.String(), diagnostic.String())
	}
	out.Reset()
	diagnostic.Reset()
	status = cli.Run(context.Background(), []string{"version"}, strings.NewReader(""), &out, &diagnostic, build)
	if status != 1 || out.Len() != 0 || !strings.Contains(diagnostic.String(), "Could not prepare command output") {
		t.Fatalf("broken text resource: status=%d stdout=%q stderr=%q", status, out.String(), diagnostic.String())
	}
}

func TestVersionTextResources(t *testing.T) {
	resources := append(cli.Resources(), Resources()...)
	catalog, err := i18n.Prepare(resources)
	if err != nil || len(catalog.Snapshot().Stale) != 0 {
		t.Fatalf("version catalog: %v stale=%v", err, catalog.Snapshot().Stale)
	}
	build, err := buildversion.Inspect(buildversion.Request{})
	if err != nil {
		t.Fatal(err)
	}
	for _, locale := range []string{"en", "zh-CN", "fr"} {
		text, err := versionText(catalog, locale, build)
		if err != nil || !strings.HasSuffix(text, "\n") || !strings.Contains(text, build.Snapshot().Main.Path.Value) {
			t.Fatalf("locale=%s text=%q error=%v", locale, text, err)
		}
	}
}
