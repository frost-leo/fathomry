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

package compatibility

import (
	"cmp"
	"slices"
	"strings"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/resource"
)

// Option is a Provider-owned non-secret effective setting, including relevant
// defaults, retry policy and buffer limits. Values are bounded tokens, not raw
// configuration, credentials, endpoints or hashes of secrets. Syntax is not a
// secret detector: the Provider must explicitly select safe fields from the SAME
// prepared settings used to construct its resource. Lists have unique names.
type Option struct {
	localValue
	Name  string
	Value string
}

// Profile separates SDK execution mode, service mode/version, protocol and native
// library facts. It does not own Workflow/mode versions or deployment identity.
// ImplementationModule identifies the implementation's contributing Go module;
// include it in BuildRequest.SDKModules or disclose it when it is the main module.
// Unknown is explicit; Declared is not an observation. NotApplicable requires an
// integration-owned explanation in the record's linked limitations. This package
// does not probe services or infer the effective profile from a configuration ID.
type Profile struct {
	localValue
	ImplementationModule string
	SDKMode              string
	ServiceMode          Fact
	ServiceVersion       Fact
	Protocol             Fact
	Native               Fact
	Options              []Option
}

// Combination is an exact tested/observed combination, not a semver range.
// Format is the Provider configuration format. SourceRevision associates the
// frozen preparation; it is NOT compared for content equality. Limits remain a
// separate effective control, never hidden behind that revision or SDKMode.
type Combination struct {
	localValue
	Provider       string
	Build          Build
	Format         uint32
	SourceRevision string
	Limits         resource.Limits
	Profile        Profile
}

type Layer string

const (
	Mechanism  Layer = "mechanism"
	Capability Layer = "capability"
	SDK        Layer = "sdk"
	Service    Layer = "service"
)

type Method string

const (
	TestDouble      Method = "test-double"
	SourceReview    Method = "source-review"
	Executed        Method = "executed"
	IsolatedService Method = "isolated-service"
)

type TestStatus string

const (
	NotRun  TestStatus = ""
	Passed  TestStatus = "passed"
	Failed  TestStatus = "failed"
	Skipped TestStatus = "skipped"
)

// Behavior names the upgrade obligations. Compilation and connectivity are not
// substitutes. A relevant no-retry/default/empty-result guarantee still needs an
// executed assertion; "not applicable" prose cannot fill a missing behavior test.
type Behavior string

const (
	Defaults      Behavior = "defaults"
	Retries       Behavior = "retries"
	Cancellation  Behavior = "cancellation"
	ErrorIdentity Behavior = "error-identity"
	Results       Behavior = "results"
	Resources     Behavior = "resources"
)

// Evidence is a reviewed test/source reference, not a signed test attestation.
// SourceReview and skipped/not-run tests cannot satisfy successful executed support.
// A failed relevant review may establish a known incompatibility. References
// are non-secret IDs resolved by the integration document, not local file paths.
type Evidence struct {
	localValue
	Layer     Layer
	Method    Method
	Status    TestStatus
	Reference string
	Covers    []Behavior
}

// Record links one guarantee and exact baseline to its accepted standard revision,
// implementation revision/manifest, executable tests, and explicit limitations.
// Repeated baselines for a guarantee are allowed; a matching failed test takes
// precedence over passing records. No record is installed in a global catalog.
// IDs and limitation references are bounded non-secret code-owned tokens. Detailed
// provenance and explanations belong in the linked integration document.
type Record struct {
	localValue
	ID               string
	Guarantee        string
	StandardRevision string
	Implementation   string
	Baseline         Combination
	Evidence         []Evidence
	Limitations      []string
}

// Requirement explicitly selects the guarantee and evidence layers needed by its
// capability owner. A mechanism-only test is never promoted to service evidence.
type Requirement struct {
	localValue
	Guarantee string
	Layers    []Layer
}

type Status string

const (
	Unknown      Status = "unknown"
	Untested     Status = "untested"
	Tested       Status = "tested"
	Incompatible Status = "incompatible"
)

// Difference identifies an axis, never an unsafe rejected value. Untested means
// known facts differ; Unknown means comparison lacked sufficient evidence.
type Difference struct {
	localValue
	Baseline string
	Axis     string
	Status   Status
}

type Decision struct {
	localValue
	Guarantee   string
	Status      Status
	Matched     []string
	Differences []Difference
}

// Report owns independent metadata slices copied from all inputs. Values may be
// read concurrently when callers do not mutate them. It retains actual source
// identity across aliases, baselines and per-guarantee decisions separately.
// It is not a data protocol, deployment admission service or authenticity proof.
type Report struct {
	localValue
	Source    resource.Info
	Actual    Combination
	Baselines []Record
	Decisions []Decision
}

// Assess obtains source metadata and limits from the authoritative resource.
// It owns no resource and grants no admission. Integrations supply the effective
// profile, requirements and reviewed evidence; no SDK, user callback or I/O runs.
// Unknown/untested combinations are reported, not silently approved or rejected;
// use Report.Require with an explicit deployment policy before relying on them.
// Unknown actual facts determine Unknown decisions. Incomplete baselines retain
// Unknown differences but cannot relabel a known Untested combination.
func Assess(build Build, access *resource.Access, profile Profile, requirements []Requirement, records []Record) (Report, error) {
	info := access.Info()
	actual := Combination{Provider: info.Configuration.Identity.Provider, Build: build, Format: info.Configuration.Format,
		SourceRevision: info.Configuration.Revision, Limits: access.Limits(), Profile: profile}
	if info.Configuration.Revision == "" || len(requirements) == 0 || len(requirements) > 32 || len(records) > 128 || !validCombination(actual) {
		return Report{}, ErrInvalid.New(fault.Context{})
	}
	actual.Build, actual.Profile = build.Clone(), cloneProfile(profile)
	report := Report{Source: info, Actual: actual}
	ids := make(map[string]bool)
	for _, record := range records {
		if !validRecord(record) || ids[record.ID] {
			return Report{}, ErrInvalid.New(fault.Context{})
		}
		ids[record.ID] = true
		report.Baselines = append(report.Baselines, cloneRecord(record))
	}
	ids = make(map[string]bool)
	for _, requirement := range requirements {
		if !token(requirement.Guarantee) || ids[requirement.Guarantee] || !validLayers(requirement.Layers) {
			return Report{}, ErrInvalid.New(fault.Context{})
		}
		ids[requirement.Guarantee] = true
		decision := Decision{Guarantee: requirement.Guarantee, Status: Untested}
		if len(compare(actual, actual)) != 0 {
			decision.Status = Unknown
		}
		for _, record := range report.Baselines {
			if record.Guarantee != requirement.Guarantee {
				continue
			}
			differences := compare(actual, record.Baseline)
			for index := range differences {
				differences[index].Baseline = record.ID
			}
			decision.Differences = append(decision.Differences, differences...)
			if len(differences) != 0 {
				continue
			}
			decision.Matched = append(decision.Matched, record.ID)
			status := evidenceStatus(record, requirement.Layers, actual.Profile)
			if status == Incompatible || status == Tested && decision.Status != Incompatible {
				decision.Status = status
			}
		}
		report.Decisions = append(report.Decisions, decision)
	}
	return report, nil
}

const (
	ErrUnsupported = fault.Kind("fathomry.compatibility.unsupported")
	ErrUnverified  = fault.Kind("fathomry.compatibility.unverified")
)

// Policy is an explicit opt-in to unverified combinations, not a claim that tests
// passed. Zero requires tested evidence for every requested guarantee. A known
// incompatibility cannot be waived here or converted into a weaker guarantee.
type Policy struct {
	localValue
	AllowUnknown  bool
	AllowUntested bool
}

// Require applies policy without changing the report. Invalid/empty decisions
// never approve use. Call before granting the requested capability; it is not an
// authenticated deployment gate and assumes the caller has not falsified the report.
func (report Report) Require(policy Policy) error {
	if len(report.Decisions) == 0 {
		return ErrUnverified.New(fault.Context{})
	}
	unverified := false
	for _, decision := range report.Decisions {
		switch decision.Status {
		case Incompatible:
			return ErrUnsupported.New(fault.Context{})
		case Tested:
		case Unknown:
			unverified = unverified || !policy.AllowUnknown
		case Untested:
			unverified = unverified || !policy.AllowUntested
		default:
			unverified = true
		}
	}
	if unverified {
		return ErrUnverified.New(fault.Context{})
	}
	return nil
}

func compare(actual, baseline Combination) []Difference {
	var differences []Difference
	add := func(axis string, known, equal bool) {
		if !known {
			differences = append(differences, Difference{Axis: axis, Status: Unknown})
		} else if !equal {
			differences = append(differences, Difference{Axis: axis, Status: Untested})
		}
	}
	add("go", actual.Build.Go.Kind == Observed && baseline.Build.Go.Kind == Observed, actual.Build.Go == baseline.Build.Go)
	add("framework", pinned(actual.Build.Framework) && pinned(baseline.Build.Framework), actual.Build.Framework == baseline.Build.Framework)
	add("provider", true, actual.Provider == baseline.Provider)
	implementation, testedImplementation := implementationModule(actual), implementationModule(baseline)
	add("implementation-module", pinned(implementation) && pinned(testedImplementation), implementation == testedImplementation)
	add("platform", hasPlatform(actual.Build.Platform) && hasPlatform(baseline.Build.Platform), slices.Equal(actual.Build.Platform, baseline.Build.Platform))
	add("build-settings", len(actual.Build.OpaqueSettings) == 0 && len(baseline.Build.OpaqueSettings) == 0, true)
	actualSDKs, baselineSDKs := actual.Build.SDKs, baseline.Build.SDKs
	add("sdk-set", len(actualSDKs) == len(baselineSDKs), len(actualSDKs) == len(baselineSDKs))
	for index := 0; index < min(len(actualSDKs), len(baselineSDKs)); index++ {
		add("sdk-"+actualSDKs[index].Path.Value, pinned(actualSDKs[index]) && pinned(baselineSDKs[index]), actualSDKs[index] == baselineSDKs[index])
	}
	add("configuration-format", actual.Format != 0 && baseline.Format != 0, actual.Format == baseline.Format)
	add("source-limits", true, actual.Limits == baseline.Limits)
	add("sdk-mode", actual.Profile.SDKMode != "" && baseline.Profile.SDKMode != "", actual.Profile.SDKMode == baseline.Profile.SDKMode)
	add("effective-options", true, slices.Equal(actual.Profile.Options, baseline.Profile.Options))
	for _, pair := range []struct {
		axis             string
		actual, baseline Fact
	}{
		{"service-mode", actual.Profile.ServiceMode, baseline.Profile.ServiceMode},
		{"service-version", actual.Profile.ServiceVersion, baseline.Profile.ServiceVersion},
		{"protocol", actual.Profile.Protocol, baseline.Profile.Protocol},
		{"native", actual.Profile.Native, baseline.Profile.Native},
	} {
		add(pair.axis, comparableFact(pair.actual) && comparableFact(pair.baseline), pair.actual == pair.baseline)
	}
	return differences
}

func pinned(module Module) bool {
	if !module.Present || module.Path.Kind != Observed || module.Version.Kind != Observed || strings.Contains(module.Version.Value, "+dirty") || strings.HasSuffix(module.Version.Value, ".dirty") {
		return false
	}
	switch module.Replacement.Kind {
	case NoReplacement:
		if module.Main {
			return module.VCS.Revision.Kind == Observed && module.VCS.Modified == (Fact{Kind: Observed, Value: "false"})
		}
		return module.Sum.Kind == Observed
	case ModuleReplacement:
		return module.Replacement.Path.Kind == Observed && module.Replacement.Version.Kind == Observed && module.Replacement.Sum.Kind == Observed && !strings.Contains(module.Replacement.Version.Value, "+dirty") && !strings.HasSuffix(module.Replacement.Version.Value, ".dirty")
	default:
		return false
	}
}

func comparableFact(fact Fact) bool {
	return fact.Kind == Observed || fact.Kind == NotApplicable
}

func implementationModule(combination Combination) Module {
	path := combination.Profile.ImplementationModule
	if combination.Build.Framework.Path.Value == path {
		return combination.Build.Framework
	}
	if combination.Build.Main.Path.Value == path {
		return combination.Build.Main
	}
	for _, module := range combination.Build.SDKs {
		if module.Path.Value == path {
			return module
		}
	}
	return Module{}
}

func hasPlatform(options []Option) bool {
	var os, arch bool
	for _, option := range options {
		os = os || option.Name == "GOOS"
		arch = arch || option.Name == "GOARCH"
	}
	return os && arch
}

func evidenceStatus(record Record, layers []Layer, profile Profile) Status {
	complete := true
	for _, layer := range layers {
		covered := make(map[Behavior]bool)
		for _, evidence := range record.Evidence {
			if evidence.Layer != layer {
				continue
			}
			if evidence.Status == Failed {
				return Incompatible
			}
			if evidence.Status == Passed && evidence.Method != SourceReview {
				for _, behavior := range evidence.Covers {
					covered[behavior] = true
				}
			}
		}
		complete = complete && len(covered) == 6
		if layer == Service {
			complete = complete && profile.ServiceMode.Kind == Observed && profile.ServiceVersion.Kind == Observed && profile.Protocol.Kind == Observed
		}
	}
	if complete {
		return Tested
	}
	return Untested
}

func validLayers(layers []Layer) bool {
	if len(layers) == 0 || len(layers) > 4 {
		return false
	}
	seen := make(map[Layer]bool)
	for _, layer := range layers {
		if seen[layer] || layer != Mechanism && layer != Capability && layer != SDK && layer != Service {
			return false
		}
		seen[layer] = true
	}
	return true
}

func validFact(fact Fact) bool {
	switch fact.Kind {
	case UnknownFact, Redacted, Development, NotApplicable:
		return fact.Value == ""
	case Observed, Declared:
		return token(fact.Value)
	default:
		return false
	}
}

func validOptions(options []Option) bool {
	if len(options) > 64 {
		return false
	}
	seen := make(map[string]bool)
	for _, option := range options {
		if seen[option.Name] || !token(option.Name) || !token(option.Value) {
			return false
		}
		seen[option.Name] = true
	}
	return true
}

func validCombination(combination Combination) bool {
	profile, limits := combination.Profile, combination.Limits
	if !token(combination.Provider) || combination.Format == 0 || !token(combination.SourceRevision) || !validBuild(combination.Build) ||
		!modulePath(profile.ImplementationModule) ||
		!token(profile.SDKMode) || !validOptions(profile.Options) ||
		limits.Active <= 0 || limits.Bytes <= 0 || limits.MaxLeases <= 0 || limits.Queued < 0 ||
		!(limits.Queued == 0 && limits.QueuedBytes == 0 || limits.Queued > 0 && limits.QueuedBytes > 0) {
		return false
	}
	for _, fact := range []Fact{profile.ServiceMode, profile.ServiceVersion, profile.Protocol, profile.Native} {
		if !validFact(fact) {
			return false
		}
	}
	return true
}

func validRecord(record Record) bool {
	for _, value := range []string{record.ID, record.Guarantee, record.StandardRevision, record.Implementation} {
		if !token(value) {
			return false
		}
	}
	if !validCombination(record.Baseline) || len(record.Evidence) > 32 || len(record.Limitations) == 0 || len(record.Limitations) > 32 {
		return false
	}
	for _, limitation := range record.Limitations {
		if !token(limitation) {
			return false
		}
	}
	for _, evidence := range record.Evidence {
		if !validLayers([]Layer{evidence.Layer}) || !token(evidence.Reference) || len(evidence.Covers) > 6 ||
			evidence.Status != NotRun && evidence.Status != Passed && evidence.Status != Failed && evidence.Status != Skipped {
			return false
		}
		switch evidence.Layer {
		case Mechanism, Capability:
			if evidence.Method != TestDouble && evidence.Method != Executed && evidence.Method != SourceReview {
				return false
			}
		case SDK:
			if evidence.Method != Executed && evidence.Method != SourceReview {
				return false
			}
		case Service:
			if evidence.Method != IsolatedService && evidence.Method != SourceReview {
				return false
			}
		}
		seen := make(map[Behavior]bool)
		for _, behavior := range evidence.Covers {
			if seen[behavior] || behavior != Defaults && behavior != Retries && behavior != Cancellation && behavior != ErrorIdentity && behavior != Results && behavior != Resources {
				return false
			}
			seen[behavior] = true
		}
	}
	return true
}

func sortOptions(options []Option) {
	slices.SortFunc(options, func(left, right Option) int { return cmp.Compare(left.Name, right.Name) })
}

func cloneProfile(profile Profile) Profile {
	profile.Options = slices.Clone(profile.Options)
	sortOptions(profile.Options)
	return profile
}

func cloneRecord(record Record) Record {
	record.Baseline.Build = record.Baseline.Build.Clone()
	record.Baseline.Profile = cloneProfile(record.Baseline.Profile)
	record.Evidence = slices.Clone(record.Evidence)
	for index := range record.Evidence {
		record.Evidence[index].Covers = slices.Clone(record.Evidence[index].Covers)
	}
	record.Limitations = slices.Clone(record.Limitations)
	return record
}
