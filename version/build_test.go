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

package version_test

import (
	"fmt"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/version"
)

const (
	appPath        = "example.org/application"
	dependencyPath = "example.org/selected"
	privateCanary  = "private-path-canary"
	revision       = "1234567890abcdef1234567890abcdef12345678"
)

func request() version.Request {
	return version.Request{Dependencies: []string{dependencyPath, "example.org/absent"}, DisclosePaths: []string{appPath}}
}

func metadata() *debug.BuildInfo {
	return &debug.BuildInfo{
		GoVersion: "go1.27.0", Path: appPath + "/cmd/app",
		Main: debug.Module{Path: appPath, Version: "v0.0.0-20260918123456-1234567890ab"},
		Deps: []*debug.Module{
			{Path: version.FrameworkModule, Version: "v0.0.0", Replace: &debug.Module{Path: "/" + privateCanary, Version: ""}},
			{Path: dependencyPath, Version: "v2.3.4", Replace: &debug.Module{Path: "example.org/fork", Version: "v2.3.5"}},
			{Path: "example.org/" + privateCanary, Version: "v1.0.0"},
		},
		Settings: []debug.BuildSetting{
			{Key: "GOOS", Value: "windows"}, {Key: "GOARCH", Value: "arm64"},
			{Key: "CGO_ENABLED", Value: "0"},
			{Key: "vcs", Value: "git"}, {Key: "vcs.revision", Value: revision},
			{Key: "vcs.modified", Value: "false"}, {Key: "vcs.time", Value: "1970-01-01T00:00:00Z"},
			{Key: "-ldflags", Value: privateCanary}, {Key: "private-setting", Value: privateCanary},
		},
	}
}

func normalize(t testing.TB, info *debug.BuildInfo, input version.Request) version.Build {
	t.Helper()
	build, err := version.FromBuildInfo(info, input)
	if err != nil {
		t.Fatal(err)
	}
	return build
}

func declaration() version.Declaration {
	return version.Declaration{Release: "v9.8.7+declared", GitRevision: revision, Tree: version.Clean, BuildTime: "2026-09-19T00:00:00Z"}
}

func TestBuildOwnershipDisclosureAndMissingFacts(t *testing.T) {
	info, input := metadata(), request()
	build := normalize(t, info, input)
	before := build.Snapshot()
	if before.Origin != version.Supplied || !before.Metadata || before.Go.Value != info.GoVersion ||
		before.Main.Path.Value != appPath || !before.Main.Main || before.Framework.Main ||
		before.Framework.Version.Value != "v0.0.0" || before.Framework.Replacement.Kind != version.LocalReplacement ||
		before.Framework.Replacement.Path.Evidence != version.Redacted || before.Framework.Replacement.Path.Value != "" ||
		before.Framework.Replacement.Version.Evidence != version.DevelopmentEvidence {
		t.Fatal("module ownership or local replacement misreported", before)
	}
	if len(before.Dependencies) != 2 || before.Dependencies[0].Present ||
		before.Dependencies[0].Path.Evidence != version.Requested ||
		before.Dependencies[1].Replacement.Path.Evidence != version.Redacted ||
		before.Dependencies[1].Version.Value != "v2.3.4" || before.Dependencies[1].Replacement.Version.Value != "v2.3.5" {
		t.Fatal("requested/module replacement facts lost", before)
	}
	if before.Source.Tree != version.Clean || before.Source.Revision.Value != revision || before.Source.CommitTime.String() != "1970-01-01T00:00:00Z" {
		t.Fatal("main source facts lost")
	}
	if strings.Contains(fmt.Sprintf("%#v", before), privateCanary) {
		t.Fatal("private data escaped projection")
	}
	info.Main.Version = "(devel)"
	info.Deps[0].Replace.Path = "mutated"
	info.Settings[0].Value = "mutated"
	input.Dependencies[0] = "mutated"
	detached := build.Snapshot()
	detached.Dependencies[0] = version.Module{}
	detached.Settings[0].Value = "mutated"
	if !reflect.DeepEqual(build.Snapshot(), before) {
		t.Fatal("snapshot aliases borrowed inputs or caller output")
	}
	var workers sync.WaitGroup
	for range 24 {
		workers.Go(func() {
			for range 50 {
				copy := build.Snapshot()
				if !reflect.DeepEqual(copy, before) {
					t.Error("concurrent read changed")
				}
				copy.Dependencies[0] = version.Module{}
				copy.Settings[0].Value = "private mutation"
			}
		})
	}
	workers.Wait()
	missing := normalize(t, nil, request()).Snapshot()
	if missing.Metadata || missing.Go != (version.Fact{}) || missing.Source.Tree != version.UnknownTree ||
		missing.Framework.Present || missing.Declaration.Origin != version.NoDeclaration {
		t.Fatal("invented missing metadata")
	}
	info = metadata()
	info.Main.Version, info.GoVersion, info.Settings = "", "", nil
	missing = normalize(t, info, request()).Snapshot()
	if missing.Main.Version.Evidence != version.UnknownEvidence || missing.Go != (version.Fact{}) || missing.Source != (version.Source{}) {
		t.Fatal("empty metadata became development, clean or inspector facts")
	}
	info.Main.Version = "(devel)"
	if normalize(t, info, request()).Snapshot().Main.Version.Evidence != version.DevelopmentEvidence {
		t.Fatal("explicit development lost")
	}
	info = metadata()
	info.Main.Path = version.FrameworkModule
	info.Deps = info.Deps[1:]
	owned := normalize(t, info, request()).Snapshot()
	if !owned.Framework.Main || owned.Framework != owned.Main {
		t.Fatal("framework-as-main was not recognized")
	}
	input = request()
	input.DisclosePaths = append(input.DisclosePaths, "example.org/fork")
	if normalize(t, metadata(), input).Snapshot().Dependencies[1].Replacement.Path.Value != "example.org/fork" {
		t.Fatal("explicit fork disclosure ignored")
	}
}

func TestMetadataRejectsMalformedConflictingAndOversizedInputs(t *testing.T) {
	cases := []struct {
		name   string
		change func(*debug.BuildInfo)
		code   string
	}{
		{"duplicate-target", func(info *debug.BuildInfo) { info.Settings = append(info.Settings, info.Settings[0]) }, string(version.Conflict)},
		{"duplicate-source", func(info *debug.BuildInfo) {
			info.Settings = append(info.Settings, debug.BuildSetting{Key: "vcs.modified", Value: "true"})
		}, string(version.Conflict)},
		{"duplicate-module", func(info *debug.BuildInfo) { info.Deps = append(info.Deps, info.Deps[0]) }, string(version.Conflict)},
		{"module-v-main", func(info *debug.BuildInfo) { info.Deps = append(info.Deps, &info.Main) }, string(version.Conflict)},
		{"short-revision", func(info *debug.BuildInfo) { info.Settings[4].Value = "1234567" }, string(version.InvalidMetadata)},
		{"uppercase-revision", func(info *debug.BuildInfo) { info.Settings[4].Value = strings.ToUpper(revision) }, string(version.InvalidMetadata)},
		{"invalid-tree", func(info *debug.BuildInfo) { info.Settings[5].Value = "maybe" }, string(version.InvalidMetadata)},
		{"invalid-time", func(info *debug.BuildInfo) { info.Settings[6].Value = "yesterday" }, string(version.InvalidMetadata)},
		{"missing-vcs-owner", func(info *debug.BuildInfo) { info.Settings[3].Value = "" }, string(version.InvalidMetadata)},
		{"missing-main-owner", func(info *debug.BuildInfo) { info.Main.Path = "" }, string(version.InvalidMetadata)},
		{"bad-module", func(info *debug.BuildInfo) { info.Deps[1].Version = "vbad" }, string(version.InvalidMetadata)},
		{"bad-toolchain", func(info *debug.BuildInfo) { info.GoVersion = "private-toolchain" }, string(version.InvalidMetadata)},
		{"control-in-toolchain", func(info *debug.BuildInfo) { info.GoVersion = "go1.27.0-\x1bprivate" }, string(version.InvalidMetadata)},
		{"space-in-toolchain", func(info *debug.BuildInfo) { info.GoVersion = "go1.27.0- private" }, string(version.InvalidMetadata)},
		{"bad-target", func(info *debug.BuildInfo) { info.Settings[0].Value = "/private/target" }, string(version.InvalidMetadata)},
		{"control-in-target", func(info *debug.BuildInfo) { info.Settings[0].Value = "linux\x1b" }, string(version.InvalidMetadata)},
		{"comma-in-target", func(info *debug.BuildInfo) { info.Settings[0].Value = "linux,other" }, string(version.InvalidMetadata)},
		{"bad-cgo", func(info *debug.BuildInfo) { info.Settings[2].Value = "yes" }, string(version.InvalidMetadata)},
		{"replacement-cycle", func(info *debug.BuildInfo) { info.Deps[0].Replace = info.Deps[0] }, string(version.InvalidMetadata)},
		{"settings-limit", func(info *debug.BuildInfo) { info.Settings = make([]debug.BuildSetting, version.MaxSettings+1) }, string(version.LimitExceeded)},
		{"module-limit", func(info *debug.BuildInfo) { info.Deps = make([]*debug.Module, version.MaxModules+1) }, string(version.LimitExceeded)},
		{"byte-limit", func(info *debug.BuildInfo) { info.Settings[7].Value = strings.Repeat("x", version.MaxMetadataBytes) }, string(version.LimitExceeded)},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			info := metadata()
			item.change(info)
			build, err := version.FromBuildInfo(info, request())
			if err == nil || err.Error() != item.code || !reflect.DeepEqual(build.Snapshot(), version.Snapshot{}) {
				t.Fatalf("bad metadata accepted/partially returned: %v", err)
			}
		})
	}
	for _, input := range []version.Request{
		{DisclosePaths: []string{"/private/path"}},
		{Dependencies: []string{"example.org/../private"}},
		{Dependencies: []string{strings.Repeat("x", 257)}},
		{Dependencies: make([]string, 65)}, {DisclosePaths: make([]string, 65)},
	} {
		_, err := version.FromBuildInfo(nil, input)
		requireCode(t, err, version.InvalidRequest)
	}
	info := metadata()
	info.Settings[4].Value = strings.Repeat("a", 64)
	if normalize(t, info, request()).Snapshot().Source.Revision.Value != strings.Repeat("a", 64) {
		t.Fatal("SHA-256 revision was truncated")
	}
	info.Settings[3].Value = "hg"
	info.Settings[4].Value = privateCanary
	source := normalize(t, info, request()).Snapshot().Source
	if source.VCS.Value != "hg" || source.Revision.Evidence != version.Redacted || source.Revision.Value != "" {
		t.Fatal("unsupported VCS revision leaked or became Git")
	}
}

func FuzzMetadata(f *testing.F) {
	for _, seed := range [][4]string{
		{"v1.2.3", revision, "linux", "1970-01-01T00:00:00Z"},
		{"", "", "", ""},
		{"v1", "bad", "/private", "yesterday"},
	} {
		f.Add(seed[0], seed[1], seed[2], seed[3])
	}
	f.Fuzz(func(t *testing.T, release, revision, target, timestamp string) {
		info := metadata()
		info.Main.Version, info.Settings[4].Value = release, revision
		info.Settings[0].Value, info.Settings[6].Value = target, timestamp
		build, err := version.FromBuildInfo(info, request())
		if err != nil {
			if !reflect.DeepEqual(build.Snapshot(), version.Snapshot{}) {
				t.Fatal("malformed metadata returned partial claims")
			}
			return
		}
		result := build.Snapshot()
		if result.Declaration.Origin != version.NoDeclaration || result.Framework.Main ||
			result.Framework.Replacement.Path.Value != "" || result.Source.Revision.Value != revision {
			t.Fatal("normalization invented or misattributed facts")
		}
	})
}

func TestDeclarationsAreSeparateCompleteAndConflictChecked(t *testing.T) {
	base := normalize(t, metadata(), request())
	before := base.Snapshot()
	declared, err := version.WithDeclaration(base, declaration())
	if err != nil {
		t.Fatal(err)
	}
	result := declared.Snapshot()
	if result.Declaration.Origin != version.Caller || result.Declaration.Release.String() != declaration().Release ||
		result.Main.Version != before.Main.Version || result.Framework != before.Framework ||
		result.Source.CommitTime == result.Declaration.BuildTime || !reflect.DeepEqual(base.Snapshot(), before) {
		t.Fatal("claim overwrote native facts or its input")
	}
	if err := declared.CheckDeclaration(declaration()); err != nil {
		t.Fatal(err)
	}
	same, err := version.WithDeclaration(declared, declaration())
	if err != nil || !reflect.DeepEqual(same.Snapshot(), result) {
		t.Fatal("identical declaration was not idempotent")
	}
	requireCode(t, base.CheckDeclaration(declaration()), version.MissingDeclaration)
	for _, change := range []func(*version.Declaration){
		func(value *version.Declaration) { value.Release = "" },
		func(value *version.Declaration) { value.Release = "(devel)" },
		func(value *version.Declaration) { value.Release = "v1.2" },
		func(value *version.Declaration) { value.GitRevision = "" },
		func(value *version.Declaration) { value.GitRevision = "1234" },
		func(value *version.Declaration) { value.Tree = version.UnknownTree },
		func(value *version.Declaration) { value.Tree = "false" },
		func(value *version.Declaration) { value.BuildTime = "" },
		func(value *version.Declaration) { value.BuildTime = "2026-09-19T00:00:00+00:00" },
	} {
		input := declaration()
		change(&input)
		_, err := version.WithDeclaration(base, input)
		requireCode(t, err, version.InvalidDeclaration)
	}
	for _, change := range []func(*version.Declaration){
		func(value *version.Declaration) { value.GitRevision = strings.Repeat("b", 40) },
		func(value *version.Declaration) { value.Tree = version.Dirty },
	} {
		input := declaration()
		change(&input)
		_, err := version.WithDeclaration(base, input)
		requireCode(t, err, version.Conflict)
	}
	other := declaration()
	other.Release = "v9.8.7+different"
	requireCode(t, declared.CheckDeclaration(other), version.Conflict)
	_, err = version.WithDeclaration(declared, other)
	requireCode(t, err, version.Conflict)
	unknown := normalize(t, nil, request())
	claimed, err := version.WithDeclaration(unknown, declaration())
	if err != nil || claimed.Snapshot().Source != (version.Source{}) {
		t.Fatal("caller claim filled missing native source", err)
	}
	info := metadata()
	info.Settings[3].Value = "hg"
	_, err = version.WithDeclaration(normalize(t, info, request()), declaration())
	requireCode(t, err, version.Conflict)
}

func TestConcurrentRuntimeInspection(t *testing.T) {
	want, err := version.Inspect(version.Request{})
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for range 12 {
		workers.Go(func() {
			for range 10 {
				build, err := version.Inspect(version.Request{})
				if err != nil || !reflect.DeepEqual(build.Snapshot(), want.Snapshot()) {
					t.Error("concurrent runtime inspection changed", err)
				}
			}
		})
	}
	workers.Wait()
}

func TestSuppliedBuildPreservesDifferentToolchain(t *testing.T) {
	info := metadata()
	info.GoVersion = "go1.20.3"
	result := normalize(t, info, request()).Snapshot()
	if result.Go.Value != info.GoVersion || result.Go.Evidence != version.Reported {
		t.Fatal("inspector toolchain substituted for supplied metadata")
	}
	info.GoVersion = "devel go1.28-abcdef " + privateCanary
	result = normalize(t, info, request()).Snapshot()
	if result.Go.Value != "" || result.Go.Evidence != version.DevelopmentEvidence {
		t.Fatal("development toolchain description leaked or became a release")
	}
}

func TestMetadataLimitPositiveControlsAndOmittedVariants(t *testing.T) {
	input := version.Request{}
	for index := range version.MaxRequestedPaths {
		path := fmt.Sprintf("example.org/module%d", index)
		input.Dependencies = append(input.Dependencies, path)
		input.DisclosePaths = append(input.DisclosePaths, path)
	}
	if len(normalize(t, nil, input).Snapshot().Dependencies) != version.MaxRequestedPaths {
		t.Fatal("maximum request size was not accepted")
	}
	path := "example.org/" + strings.Repeat("a", version.MaxPathBytes-len("example.org/"))
	input = version.Request{Dependencies: []string{path, path}}
	if len(normalize(t, nil, input).Snapshot().Dependencies) != 1 {
		t.Fatal("maximum path or duplicate normalization failed")
	}
	for _, info := range []*debug.BuildInfo{
		{Deps: make([]*debug.Module, version.MaxModules)},
		{Settings: make([]debug.BuildSetting, version.MaxSettings)},
		{Settings: []debug.BuildSetting{{Key: "x", Value: strings.Repeat("a", version.MaxMetadataBytes-1)}}},
	} {
		result := normalize(t, info, version.Request{}).Snapshot()
		if !result.Metadata || result.Go.Value != "" || len(result.Settings) != 0 {
			t.Fatal("input limit control invented disclosed facts")
		}
	}
	info := metadata()
	info.Settings = append(info.Settings, debug.BuildSetting{Key: "GOARM64", Value: "v8.0,lse"})
	info.Deps[2].Version = privateCanary
	info.Deps = append(info.Deps, info.Deps[2])
	info.Deps[0].Replace.Sum = privateCanary
	result := normalize(t, info, request()).Snapshot()
	for _, setting := range result.Settings {
		if setting.Name == "GOARM64" {
			t.Fatal("unsupported variant was presented as a complete target fact")
		}
	}
	if strings.Contains(fmt.Sprintf("%#v", result), privateCanary) {
		t.Fatal("unrequested malformed dependency or local-replacement checksum leaked")
	}
}
