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

package compatibility_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/resource"
)

const sdkPath = "example.org/fixture-sdk"
const moduleSum = "h1:N6y/pJk8buWs9NY5ERU2HSMfm+IuD/OtfdAnq6kESPw="

func observed(value string) compatibility.Fact {
	return compatibility.Fact{Kind: compatibility.Observed, Value: value}
}

func buildInfo() *debug.BuildInfo {
	return &debug.BuildInfo{
		GoVersion: "go1.26.4",
		Main:      debug.Module{Path: "private.example/application", Version: "(devel)"},
		Deps: []*debug.Module{
			{Path: compatibility.FrameworkModule, Version: "v0.1.0", Sum: moduleSum},
			{Path: sdkPath, Version: "v1.0.0", Sum: moduleSum},
		},
		Settings: []debug.BuildSetting{
			{Key: "GOOS", Value: "linux"}, {Key: "GOARCH", Value: "amd64"},
			{Key: "vcs", Value: "git"}, {Key: "vcs.revision", Value: strings.Repeat("a", 40)},
			{Key: "vcs.modified", Value: "true"},
		},
	}
}

func inspected(t testing.TB, info *debug.BuildInfo, disclose ...string) compatibility.Build {
	t.Helper()
	build, err := compatibility.FromBuildInfo(info, compatibility.BuildRequest{SDKModules: []string{sdkPath}, DisclosePaths: disclose})
	if err != nil {
		t.Fatal(err)
	}
	return build
}

func TestBuildFactsSeparateMainFrameworkAndSelectedSDK(t *testing.T) {
	info := buildInfo()
	info.Settings = append(info.Settings, debug.BuildSetting{Key: "-ldflags", Value: "-X private.token=secret-canary /home/private-canary"},
		debug.BuildSetting{Key: "CGO_CFLAGS", Value: "-I/home/private-canary"})
	build := inspected(t, info)
	if len(build.OpaqueSettings) != 2 {
		t.Fatal("redacted behavior-affecting flags were silently discarded")
	}
	if build.Go != observed("go1.26.4") || build.Main.Path.Kind != compatibility.Redacted ||
		build.Main.Version.Kind != compatibility.Development || build.Main.VCS.Modified != observed("true") ||
		build.Framework.Main || build.Framework.Version != observed("v0.1.0") ||
		build.Framework.VCS != (compatibility.VCS{}) || build.SDKs[0].VCS != (compatibility.VCS{}) {
		t.Fatal("build attribution or unknown/development facts changed")
	}
	info.Deps[1].Version = "v1.1.0"
	info.Settings[0].Value = "changed"
	if build.SDKs[0].Version != observed("v1.0.0") || build.Platform[1].Value != "linux" {
		t.Fatal("build aliases native metadata")
	}
	conformance.Runtime(t, build, new(compatibility.Build), "private.example", "secret-canary", "/home/private-canary")
	conformance.Runtime(t, build.SDKs[0], new(compatibility.Module), "/home/private-canary")
	copy := build.Clone()
	copy.SDKs[0].Version.Value = "changed"
	copy.Platform[0].Value = "changed"
	if reflect.DeepEqual(copy, build) || build.SDKs[0].Version.Value != "v1.0.0" {
		t.Fatal("build clone shares slices")
	}
	running, err := compatibility.Inspect(compatibility.BuildRequest{})
	if err != nil || running.Go.Value != runtime.Version() {
		t.Fatal("running Go fact is not the actual runtime")
	}
}

func TestReplacementsUnavailableMainAndMalformedMetadata(t *testing.T) {
	for _, path := range []string{"/home/private-canary/sdk", "../private-canary", "C:\\private-canary", "\\\\server\\private-canary", "./sdk"} {
		for _, version := range []string{"", "(devel)"} {
			info := buildInfo()
			info.Deps[1].Replace = &debug.Module{Path: path, Version: version}
			build := inspected(t, info)
			if build.SDKs[0].Replacement.Kind != compatibility.LocalReplacement ||
				build.SDKs[0].Replacement.Path.Value != "" || build.SDKs[0].Version.Value != "v1.0.0" {
				t.Fatal("local replacement was lost or reported as selected release content")
			}
			conformance.Private(t, build, "private-canary", "\\\\server")
		}
	}
	info := buildInfo()
	info.Deps[1].Replace = &debug.Module{Path: "example.org/fork", Version: "v1.2.0", Sum: moduleSum}
	hidden := inspected(t, info)
	disclosed := inspected(t, info, "example.org/fork")
	if hidden.SDKs[0].Replacement.Path.Kind != compatibility.Redacted ||
		disclosed.SDKs[0].Replacement.Path.Value != "example.org/fork" ||
		disclosed.SDKs[0].Replacement.Version.Value != "v1.2.0" || len(disclosed.SDKs) != 1 {
		t.Fatal("replacement/disclosure conflated with module selection")
	}
	info.Deps[1].Replace.Replace = info.Deps[1]
	cyclic := inspected(t, info)
	if cyclic.SDKs[0].Replacement.Kind != compatibility.UnknownReplacement {
		t.Fatal("nested/cyclic replacement invented certainty")
	}
	info = buildInfo()
	info.Main = debug.Module{Path: compatibility.FrameworkModule, Version: "v0.1.1-0.20260909000000-aaaaaaaaaaaa+dirty"}
	info.Deps = info.Deps[1:]
	main := inspected(t, info)
	if !main.Framework.Main || main.Framework.Version != observed(info.Main.Version) || main.Framework.VCS != main.Main.VCS {
		t.Fatal("framework main-module VCS facts missing")
	}
	unavailable := inspected(t, nil)
	if unavailable.Metadata != compatibility.UnknownFact || unavailable.Go != (compatibility.Fact{}) ||
		unavailable.Framework.Present || unavailable.SDKs[0].Present {
		t.Fatal("missing build information invented facts")
	}
	info = buildInfo()
	info.Main.Path = "consumer"
	bare := inspected(t, info, "consumer")
	if bare.Main.Path != observed("consumer") {
		t.Fatal("valid non-downloadable main module was mistaken for a local replacement path")
	}
	info = buildInfo()
	info.GoVersion = "devel go1.27-private /home/private-canary"
	info.Deps[1].Version, info.Deps[1].Sum = "v1.0.0/private-canary", "h1:private-canary"
	info.Settings = append(info.Settings, debug.BuildSetting{Key: "GOOS", Value: "linux"})
	malformed := inspected(t, info)
	if malformed.Go.Kind != compatibility.Development || malformed.SDKs[0].Version.Kind != compatibility.Redacted ||
		malformed.SDKs[0].Sum.Kind != compatibility.Redacted {
		t.Fatal("unsafe metadata presented as an observed version")
	}
	conformance.Private(t, malformed, "private-canary")
	info.Deps = append(info.Deps, info.Deps[1])
	if inspected(t, info).SDKs[0].Present {
		t.Fatal("duplicate module was silently selected")
	}
	for _, path := range []string{"/tmp/private", "../private", "C:\\private", "https://user:secret@example.com/sdk", "", ".", "~/private", "example.org//sdk"} {
		if _, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{path}}); !errors.Is(err, compatibility.ErrInvalid) {
			t.Fatal("unsafe build request accepted")
		}
	}
}

func sourceAccess(t testing.TB) (*resource.Assembly, resource.Selection[struct{}], *resource.Access) {
	t.Helper()
	prepared, err := resource.Prepare(resource.Schema[struct{}]{Format: 7},
		resource.Input{Identity: resource.Identity{Provider: "fixture.local", Name: "output"}, Format: 7})
	if err != nil {
		t.Fatal(err)
	}
	selection := resource.WithLimits(resource.Select(prepared, func(context.Context, struct{}) (resource.Resource[struct{}], error) {
		return resource.Resource[struct{}]{Acquired: true, Capability: struct{}{}, Release: func(context.Context) resource.ReleaseResult {
			return resource.ReleaseResult{Quiescent: true, Released: true}
		}}, nil
	}), resource.Limits{Active: 1, Bytes: 128, MaxLeases: 2})
	assembly, err := resource.Assemble(context.Background(), context.Background(), "owner", selection)
	if err != nil {
		t.Fatal(err)
	}
	access, err := resource.AccessFor(assembly, selection)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := assembly.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return assembly, selection, access
}

func profile() compatibility.Profile {
	return compatibility.Profile{
		ImplementationModule: compatibility.FrameworkModule,
		SDKMode:              "manual-ack", ServiceMode: observed("single-node"),
		ServiceVersion: observed("1.2.3"), Protocol: observed("protocol-2"),
		Native:  compatibility.Fact{Kind: compatibility.NotApplicable},
		Options: []compatibility.Option{{Name: "retries", Value: "0"}, {Name: "buffer-bytes", Value: "128"}},
	}
}

func allBehaviors() []compatibility.Behavior {
	return []compatibility.Behavior{compatibility.Defaults, compatibility.Retries, compatibility.Cancellation,
		compatibility.ErrorIdentity, compatibility.Results, compatibility.Resources}
}

func requirements() []compatibility.Requirement {
	return []compatibility.Requirement{{Guarantee: "confirmed-output", Layers: []compatibility.Layer{compatibility.Mechanism, compatibility.Capability, compatibility.SDK, compatibility.Service}}}
}

func TestAssessmentUsesAuthoritativeSourceMetadata(t *testing.T) {
	_, _, access := sourceAccess(t)
	original := access.Info()
	for _, layers := range [][]resource.LayerInfo{
		{{Kind: 255, Fields: []string{"/buffer"}}},
		{{Kind: resource.Base}, {Kind: resource.Base}},
		{{Kind: resource.Base, Fields: []string{"postgres://private-canary@example.invalid/data"}}},
		{{Kind: resource.Base, Fields: []string{strings.Repeat("x", 2<<20)}}},
	} {
		copy := access.Info()
		copy.Scope = "private/path"
		copy.Configuration.Provenance[0] = layers[0]
		copy.Configuration.Provenance = layers
		report, err := compatibility.Assess(inspected(t, buildInfo()), access, profile(), requirements(), nil)
		if err != nil || !reflect.DeepEqual(report.Source, original) {
			t.Fatal("caller-owned source metadata replaced the authoritative resource")
		}
		conformance.Private(t, report, "private/path", "private-canary")
	}
	for _, invalid := range []*resource.Access{nil, {}} {
		if _, err := compatibility.Assess(inspected(t, buildInfo()), invalid, profile(), requirements(), nil); !errors.Is(err, compatibility.ErrInvalid) {
			t.Fatal("missing resource access established source facts")
		}
	}
}

func assess(t testing.TB, build compatibility.Build, access *resource.Access, mode compatibility.Profile, records ...compatibility.Record) compatibility.Report {
	t.Helper()
	report, err := compatibility.Assess(build, access, mode, requirements(), records)
	if err != nil {
		t.Fatal(err)
	}
	return report
}

func record(baseline compatibility.Combination) compatibility.Record {
	evidence := []compatibility.Evidence{}
	for _, layer := range []compatibility.Layer{compatibility.Mechanism, compatibility.Capability, compatibility.SDK, compatibility.Service} {
		method := compatibility.Executed
		if layer == compatibility.Mechanism || layer == compatibility.Capability {
			method = compatibility.TestDouble
		}
		if layer == compatibility.Service {
			method = compatibility.IsolatedService
		}
		evidence = append(evidence, compatibility.Evidence{Layer: layer, Method: method, Status: compatibility.Passed,
			Reference: "fixture-contract-test", Covers: allBehaviors()})
	}
	return compatibility.Record{ID: "synthetic-record", Guarantee: "confirmed-output",
		StandardRevision: strings.Repeat("a", 40), Implementation: "synthetic-implementation",
		Baseline: baseline, Evidence: evidence, Limitations: []string{"unit-test-record-not-service-support"}}
}

func TestAssessmentKeepsBaselinesDifferencesAndPolicyExplicit(t *testing.T) {
	_, _, access := sourceAccess(t)
	actual := inspected(t, buildInfo())
	baseline := record(assess(t, actual, access, profile()).Actual)
	pass := assess(t, actual, access, profile(), baseline)
	if pass.Decisions[0].Status != compatibility.Tested || pass.Require(compatibility.Policy{}) != nil {
		t.Fatal("matching executed evidence not recognized")
	}
	for _, test := range []struct {
		name   string
		change func(*compatibility.Build, *compatibility.Profile, *compatibility.Record)
		want   compatibility.Status
	}{
		{"go", func(build *compatibility.Build, _ *compatibility.Profile, _ *compatibility.Record) {
			build.Go = observed("go1.26.5")
		}, compatibility.Untested},
		{"framework", func(build *compatibility.Build, _ *compatibility.Profile, _ *compatibility.Record) {
			build.Framework.Version = observed("v0.2.0")
		}, compatibility.Untested},
		{"sdk", func(build *compatibility.Build, _ *compatibility.Profile, _ *compatibility.Record) {
			build.SDKs[0].Version = observed("v1.1.0")
		}, compatibility.Untested},
		{"options", func(_ *compatibility.Build, mode *compatibility.Profile, _ *compatibility.Record) {
			mode.Options[0].Value = "1"
		}, compatibility.Untested},
		{"mode", func(_ *compatibility.Build, mode *compatibility.Profile, _ *compatibility.Record) {
			mode.SDKMode = "automatic-ack"
		}, compatibility.Untested},
		{"service", func(_ *compatibility.Build, mode *compatibility.Profile, _ *compatibility.Record) {
			mode.ServiceVersion = observed("1.2.4")
		}, compatibility.Untested},
		{"protocol", func(_ *compatibility.Build, mode *compatibility.Profile, _ *compatibility.Record) {
			mode.Protocol = observed("protocol-3")
		}, compatibility.Untested},
		{"format", func(_ *compatibility.Build, _ *compatibility.Profile, base *compatibility.Record) {
			base.Baseline.Format++
		}, compatibility.Untested},
		{"limits", func(_ *compatibility.Build, _ *compatibility.Profile, base *compatibility.Record) {
			base.Baseline.Limits.Active++
		}, compatibility.Untested},
		{"missing-sdk", func(build *compatibility.Build, _ *compatibility.Profile, _ *compatibility.Record) {
			build.SDKs[0] = compatibility.Module{Path: observed(sdkPath)}
		}, compatibility.Unknown},
		{"vendor-sum", func(build *compatibility.Build, _ *compatibility.Profile, _ *compatibility.Record) {
			build.SDKs[0].Sum = compatibility.Fact{}
		}, compatibility.Unknown},
		{"unknown-service", func(_ *compatibility.Build, mode *compatibility.Profile, _ *compatibility.Record) {
			mode.ServiceVersion = compatibility.Fact{}
		}, compatibility.Unknown},
		{"skipped", func(_ *compatibility.Build, _ *compatibility.Profile, base *compatibility.Record) {
			base.Evidence[3].Status = compatibility.Skipped
		}, compatibility.Untested},
		{"source-only", func(_ *compatibility.Build, _ *compatibility.Profile, base *compatibility.Record) {
			base.Evidence[2].Method = compatibility.SourceReview
		}, compatibility.Untested},
		{"not-run", func(_ *compatibility.Build, _ *compatibility.Profile, base *compatibility.Record) {
			base.Evidence[2].Status = compatibility.NotRun
		}, compatibility.Untested},
		{"compile-only", func(_ *compatibility.Build, _ *compatibility.Profile, base *compatibility.Record) {
			base.Evidence[2].Covers = nil
		}, compatibility.Untested},
		{"incompatible", func(_ *compatibility.Build, _ *compatibility.Profile, base *compatibility.Record) {
			base.Evidence[2].Status = compatibility.Failed
		}, compatibility.Incompatible},
	} {
		t.Run(test.name, func(t *testing.T) {
			build, mode := actual.Clone(), profile()
			base := record(assess(t, actual, access, profile()).Actual)
			test.change(&build, &mode, &base)
			report := assess(t, build, access, mode, base)
			if report.Decisions[0].Status != test.want {
				t.Fatalf("got %s; want %s", report.Decisions[0].Status, test.want)
			}
			if report.Require(compatibility.Policy{}) == nil {
				t.Fatal("unverified guarantee silently allowed")
			}
			err := report.Require(compatibility.Policy{AllowUnknown: true, AllowUntested: true})
			if test.want == compatibility.Incompatible {
				if !errors.Is(err, compatibility.ErrUnsupported) {
					t.Fatal("known incompatibility waived")
				}
			} else if err != nil {
				t.Fatal("explicit policy not honored")
			}
		})
	}
	failed := record(pass.Actual)
	failed.ID = "contradiction"
	failed.Evidence[0].Status = compatibility.Failed
	for _, records := range [][]compatibility.Record{{baseline, failed}, {failed, baseline}} {
		report := assess(t, actual, access, profile(), records...)
		if report.Decisions[0].Status != compatibility.Incompatible {
			t.Fatal("passing evidence hid a contradiction")
		}
	}
}

func TestProfilesBuildsAndRecordsAreBoundedAndCopied(t *testing.T) {
	owner, selected, access := sourceAccess(t)
	borrowed := resource.Borrow("alias", owner, selected)
	borrower, err := resource.Assemble(context.Background(), context.Background(), "borrower", borrowed)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := borrower.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	alias, err := resource.AccessFor(borrower, borrowed)
	if err != nil {
		t.Fatal(err)
	}
	build, mode := inspected(t, buildInfo()), profile()
	baseline := record(assess(t, build, access, mode).Actual)
	report := assess(t, build, alias, mode, baseline)
	if report.Source.Scope != "owner" || report.Source.Configuration.Identity.Name != "output" || report.Actual.SourceRevision != access.Info().Configuration.Revision {
		t.Fatal("alias relabeled actual source")
	}
	mode.Options[0].Value = "changed"
	baseline.Evidence[0].Covers[0] = compatibility.Resources
	baseline.Baseline.Build.SDKs[0].Version.Value = "changed"
	baseline.Limitations[0] = "changed"
	if report.Baselines[0].Evidence[0].Covers[0] != compatibility.Defaults ||
		report.Baselines[0].Baseline.Build.SDKs[0].Version.Value != "v1.0.0" || report.Baselines[0].Limitations[0] == "changed" {
		t.Fatal("report aliases input records")
	}
	conformance.Runtime(t, report, new(compatibility.Report), "private.example", "secret-canary")
	for _, change := range []func(*compatibility.Profile){
		func(p *compatibility.Profile) { p.Options = append(p.Options, p.Options[0]) },
		func(p *compatibility.Profile) { p.Options[0].Value = "/home/private-canary" },
		func(p *compatibility.Profile) {
			p.ServiceVersion = compatibility.Fact{Kind: compatibility.UnknownFact, Value: "private-canary"}
		},
		func(p *compatibility.Profile) { p.SDKMode = "" },
	} {
		p := profile()
		change(&p)
		report, err := compatibility.Assess(build, access, p, requirements(), nil)
		if !errors.Is(err, compatibility.ErrInvalid) || len(report.Decisions) != 0 {
			t.Fatal("malformed profile accepted")
		}
		conformance.Private(t, err, "private-canary")
	}
	bad := build.Clone()
	bad.SDKs[0].Replacement = compatibility.Replacement{Kind: compatibility.LocalReplacement, Path: observed("/home/private-canary")}
	if _, err := compatibility.Assess(bad, access, profile(), requirements(), nil); !errors.Is(err, compatibility.ErrInvalid) {
		t.Fatal("manually forged unsafe build accepted")
	}
	base := record(report.Actual)
	base.Evidence[3].Method = compatibility.TestDouble
	if _, err := compatibility.Assess(build, access, profile(), requirements(), []compatibility.Record{base}); !errors.Is(err, compatibility.ErrInvalid) {
		t.Fatal("fake service evidence accepted")
	}
	var group sync.WaitGroup
	for range 16 {
		group.Go(func() {
			for range 20 {
				local := assess(t, build, access, profile(), record(report.Actual))
				local.Baselines[0].Baseline.Profile.Options[0].Value = "local"
				if local.Decisions[0].Status != compatibility.Tested {
					t.Error("concurrent assessment changed")
				}
			}
		})
	}
	group.Wait()
	if (compatibility.Report{}).Require(compatibility.Policy{AllowUnknown: true, AllowUntested: true}) == nil {
		t.Fatal("empty report approved")
	}
}

func TestEveryUpgradeBehaviorNeedsExecutedCoverage(t *testing.T) {
	_, _, access := sourceAccess(t)
	build := inspected(t, buildInfo())
	for _, behavior := range allBehaviors() {
		base := record(assess(t, build, access, profile()).Actual)
		base.Evidence[2].Covers = slices.DeleteFunc(base.Evidence[2].Covers, func(value compatibility.Behavior) bool { return value == behavior })
		if assess(t, build, access, profile(), base).Decisions[0].Status != compatibility.Untested {
			t.Fatal("missing behavior test ignored")
		}
	}
	local := buildInfo()
	local.Deps[0].Replace = &debug.Module{Path: "/tmp/private-canary"}
	base := record(assess(t, inspected(t, local), access, profile()).Actual)
	if assess(t, inspected(t, local), access, profile(), base).Decisions[0].Status != compatibility.Unknown {
		t.Fatal("matching local path/development labels proved compatibility")
	}
	dirty := buildInfo()
	dirty.Main = *dirty.Deps[0]
	dirty.Main.Version += "+dirty"
	dirty.Deps = dirty.Deps[1:]
	base = record(assess(t, inspected(t, dirty), access, profile()).Actual)
	if assess(t, inspected(t, dirty), access, profile(), base).Decisions[0].Status != compatibility.Unknown {
		t.Fatal("dirty main framework matched tested content")
	}
	declared := profile()
	declared.ServiceVersion.Kind = compatibility.Declared
	base = record(assess(t, build, access, declared).Actual)
	if assess(t, build, access, declared, base).Decisions[0].Status != compatibility.Unknown {
		t.Fatal("declared server version became observed service evidence")
	}
}

func TestImplementationProvenanceAndOpaqueFlagsCannotBeHidden(t *testing.T) {
	_, _, access := sourceAccess(t)
	build := inspected(t, buildInfo())
	baseline := record(assess(t, build, access, profile()).Actual)
	for _, key := range []string{"-ldflags", "-gcflags", "-tags", "GOEXPERIMENT", "DefaultGODEBUG", "CGO_CFLAGS", "unknown-private-key"} {
		info := buildInfo()
		info.Settings = append(info.Settings, debug.BuildSetting{Key: key, Value: "/home/secret-canary"})
		current := inspected(t, info)
		report := assess(t, current, access, profile(), baseline)
		if report.Decisions[0].Status != compatibility.Unknown || report.Require(compatibility.Policy{}) == nil {
			t.Fatal("unreported behavior-affecting settings became tested equivalence")
		}
		conformance.Private(t, report, "secret-canary", "unknown-private-key")
	}
	info := buildInfo()
	info.Settings = append(info.Settings, debug.BuildSetting{Key: "-trimpath", Value: "true"})
	trimmed := inspected(t, info)
	if assess(t, trimmed, access, profile(), baseline).Decisions[0].Status != compatibility.Unknown {
		t.Fatal("Go's omitted linker/CGO flags were treated as known empty values")
	}
	mode := profile()
	mode.ImplementationModule = "example.org/unreported-provider"
	if assess(t, build, access, mode, baseline).Decisions[0].Status != compatibility.Unknown {
		t.Fatal("Provider implementation hidden behind SDK versions")
	}
	baseline.Baseline.Provider = "another.provider"
	if assess(t, build, access, profile(), baseline).Decisions[0].Status != compatibility.Untested {
		t.Fatal("another Provider's record was reused")
	}
	baseline = record(assess(t, build, access, profile()).Actual)
	baseline.Evidence[2].Method, baseline.Evidence[2].Status = compatibility.SourceReview, compatibility.Failed
	if assess(t, build, access, profile(), baseline).Decisions[0].Status != compatibility.Incompatible {
		t.Fatal("known source-reviewed incompatibility was suppressed")
	}
	info = buildInfo()
	info.Deps[1].Sum = moduleSum + strings.Repeat("\n", 4096)
	if inspected(t, info).SDKs[0].Sum.Kind != compatibility.Redacted {
		t.Fatal("noncanonical oversized checksum retained")
	}
}

func FuzzBuildMetadataPrivacy(f *testing.F) {
	for _, seed := range []string{"/tmp/secret-canary", "(devel)", "v1.0.0", "devel go1.27", "C:\\secret-canary"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		info := buildInfo()
		info.Deps[1].Replace = &debug.Module{Path: "secret-canary/" + value}
		build := inspected(t, info)
		if strings.Contains(fmt.Sprintf("%+v", build), "secret-canary") {
			t.Fatal("local path retained")
		}
		if _, err := json.Marshal(build); err == nil {
			t.Fatal("new wire format escaped guard")
		}
	})
}
