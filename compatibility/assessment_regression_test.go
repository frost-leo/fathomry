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
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/frost-leo/fathomry/compatibility"
)

func TestKnownUntestedCannotBeWaivedByUnrelatedUnknownBaseline(t *testing.T) {
	_, _, access := sourceAccess(t)
	build := inspected(t, buildInfo())
	incomplete := record(assess(t, build, access, profile()).Actual)
	incomplete.Evidence[2].Covers = nil
	policy := compatibility.Policy{AllowUnknown: true, AllowUntested: false}
	baseline := assess(t, build, access, profile(), incomplete)
	if baseline.Decisions[0].Status != compatibility.Untested ||
		!errors.Is(baseline.Require(policy), compatibility.ErrUnverified) {
		t.Fatal("control case did not establish a known, untested, rejected combination")
	}
	t.Logf("control: status=%s, policy allowed=%t", baseline.Decisions[0].Status, baseline.Require(policy) == nil)

	unrelated := record(incomplete.Baseline)
	unrelated.ID = "unrelated-provider-record"
	unrelated.Baseline.Provider = "different.provider"
	unrelated.Baseline.Build = unrelated.Baseline.Build.Clone()
	unrelated.Baseline.Build.SDKs[0].Sum = compatibility.Fact{}

	for _, test := range []struct {
		name    string
		records []compatibility.Record
	}{
		{"incomplete-first", []compatibility.Record{incomplete, unrelated}},
		{"unrelated-first", []compatibility.Record{unrelated, incomplete}},
	} {
		t.Run(test.name, func(t *testing.T) {
			report := assess(t, build, access, profile(), test.records...)
			t.Logf("with unrelated record: status=%s, matched=%v, policy allowed=%t",
				report.Decisions[0].Status, report.Decisions[0].Matched, report.Require(policy) == nil)
			if report.Decisions[0].Status != compatibility.Untested {
				t.Errorf("unrelated missing metadata changed a known untested combination to %s", report.Decisions[0].Status)
			}
			if !errors.Is(report.Require(policy), compatibility.ErrUnverified) {
				t.Error("adding unrelated evidence waived the explicitly disallowed untested combination")
			}
			if !errors.Is(report.Require(compatibility.Policy{}), compatibility.ErrUnverified) {
				t.Fatal("strict default policy was also weakened")
			}
			t.Log("strict default policy still rejects this combination")
		})
	}
	failed := record(incomplete.Baseline)
	failed.Evidence[2].Status = compatibility.Failed
	report := assess(t, build, access, profile(), failed, unrelated)
	if report.Decisions[0].Status != compatibility.Incompatible ||
		!errors.Is(report.Require(compatibility.Policy{AllowUnknown: true, AllowUntested: true}), compatibility.ErrUnsupported) {
		t.Fatal("known incompatibility was also waived")
	}
	t.Log("matching known incompatibility still cannot be waived")
}

func assertDecisionPolicies(t *testing.T, report compatibility.Report, want compatibility.Status) {
	t.Helper()
	if len(report.Decisions) != 1 || report.Decisions[0].Status != want {
		t.Fatalf("decisions = %v; want one %s decision", report.Decisions, want)
	}
	var expected [4]error
	switch want {
	case compatibility.Tested:
	case compatibility.Untested:
		expected = [4]error{compatibility.ErrUnverified, compatibility.ErrUnverified, nil, nil}
	case compatibility.Unknown:
		expected = [4]error{compatibility.ErrUnverified, nil, compatibility.ErrUnverified, nil}
	case compatibility.Incompatible:
		expected = [4]error{compatibility.ErrUnsupported, compatibility.ErrUnsupported, compatibility.ErrUnsupported, compatibility.ErrUnsupported}
	default:
		t.Fatal("missing independent policy expectation")
	}
	for index, policy := range []compatibility.Policy{{}, {AllowUnknown: true}, {AllowUntested: true}, {AllowUnknown: true, AllowUntested: true}} {
		if err := report.Require(policy); !errors.Is(err, expected[index]) {
			t.Errorf("policy %+v returned %v; want %v", policy, err, expected[index])
		}
	}
}

func TestAssessmentBaselineAggregation(t *testing.T) {
	_, _, access := sourceAccess(t)
	build, mode := inspected(t, buildInfo()), profile()
	actual := assess(t, build, access, mode).Actual
	makeRecord := func(id string) compatibility.Record {
		base := record(actual)
		base.ID = id
		base.Baseline.Build = actual.Build.Clone()
		base.Baseline.Profile.Options = slices.Clone(actual.Profile.Options)
		base.Baseline.SourceRevision = "historical-" + id
		return base
	}
	passed, incomplete := makeRecord("passed"), makeRecord("incomplete")
	incomplete.Evidence[2].Covers = nil
	partial, partialProfile := makeRecord("partial-build"), makeRecord("partial-profile")
	partial.Baseline.Build.SDKs[0].Sum = compatibility.Fact{}
	partialProfile.Baseline.Profile.ServiceVersion = compatibility.Fact{}
	unrelated := makeRecord("unrelated")
	unrelated.Baseline.Provider = "different.provider"
	unrelated.Baseline.Build.SDKs[0].Sum = compatibility.Fact{}
	unrelated.Evidence[2].Status = compatibility.Failed
	unrelatedMode := makeRecord("unrelated-mode")
	unrelatedMode.Baseline.Profile.SDKMode = "automatic-ack"
	unrelatedMode.Baseline.Profile.ServiceVersion = compatibility.Fact{}
	failed := makeRecord("failed")
	failed.Evidence[2].Status = compatibility.Failed
	reviewFailed := makeRecord("review-failed")
	reviewFailed.Evidence[2].Method, reviewFailed.Evidence[2].Status = compatibility.SourceReview, compatibility.Failed
	skipped, notRun, review := makeRecord("skipped"), makeRecord("not-run"), makeRecord("source-review")
	skipped.Evidence[3].Status = compatibility.Skipped
	notRun.Evidence[2].Status = compatibility.NotRun
	review.Evidence[2].Method = compatibility.SourceReview
	other := makeRecord("another-guarantee")
	other.Guarantee = "another-guarantee"
	other.Evidence[2].Status = compatibility.Failed
	for _, test := range []struct {
		name    string
		records []compatibility.Record
		want    compatibility.Status
		matched []string
	}{
		{"no-records", nil, compatibility.Untested, nil},
		{"incomplete", []compatibility.Record{incomplete}, compatibility.Untested, []string{"incomplete"}},
		{"partial-build-only", []compatibility.Record{partial}, compatibility.Untested, nil},
		{"partial-profile-only", []compatibility.Record{partialProfile}, compatibility.Untested, nil},
		{"unrelated-only", []compatibility.Record{unrelated}, compatibility.Untested, nil},
		{"inadequate-baselines", []compatibility.Record{incomplete, partial, partialProfile, unrelated}, compatibility.Untested, []string{"incomplete"}},
		{"multiple-unrelated", []compatibility.Record{incomplete, unrelated, unrelatedMode, partialProfile}, compatibility.Untested, []string{"incomplete"}},
		{"unexecuted-coverage", []compatibility.Record{skipped, notRun, review, partial}, compatibility.Untested, []string{"not-run", "skipped", "source-review"}},
		{"passing-support", []compatibility.Record{passed, incomplete, partial, unrelated}, compatibility.Tested, []string{"incomplete", "passed"}},
		{"passing-with-partial-profile", []compatibility.Record{passed, partialProfile}, compatibility.Tested, []string{"passed"}},
		{"exact-failure", []compatibility.Record{failed, passed, partial, unrelated}, compatibility.Incompatible, []string{"failed", "passed"}},
		{"exact-review-failure", []compatibility.Record{reviewFailed, passed, partialProfile}, compatibility.Incompatible, []string{"passed", "review-failed"}},
		{"other-guarantee-only", []compatibility.Record{other}, compatibility.Untested, nil},
		{"other-guarantee-with-support", []compatibility.Record{other, passed}, compatibility.Tested, []string{"passed"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			order := 0
			var permute func([]compatibility.Record, int)
			permute = func(records []compatibility.Record, start int) {
				if start < len(records) {
					for index := start; index < len(records); index++ {
						records[start], records[index] = records[index], records[start]
						permute(records, start+1)
						records[start], records[index] = records[index], records[start]
					}
					return
				}
				t.Run(fmt.Sprintf("order-%d", order), func(t *testing.T) {
					report := assess(t, build, access, mode, records...)
					assertDecisionPolicies(t, report, test.want)
					matched := slices.Clone(report.Decisions[0].Matched)
					slices.Sort(matched)
					if !slices.Equal(matched, test.matched) || !reflect.DeepEqual(report.Actual, actual) ||
						!reflect.DeepEqual(report.Source, access.Info()) || !slices.EqualFunc(report.Baselines, records, func(left, right compatibility.Record) bool {
						return reflect.DeepEqual(left, right)
					}) {
						t.Error("matching IDs or actual/baseline/source facts changed")
					}
					var differences []compatibility.Difference
					for _, base := range records {
						switch base.ID {
						case "unrelated":
							differences = append(differences, compatibility.Difference{Baseline: base.ID, Axis: "provider", Status: compatibility.Untested})
							fallthrough
						case "partial-build":
							differences = append(differences, compatibility.Difference{Baseline: base.ID, Axis: "sdk-" + sdkPath, Status: compatibility.Unknown})
						case "partial-profile":
							differences = append(differences, compatibility.Difference{Baseline: base.ID, Axis: "service-version", Status: compatibility.Unknown})
						case "unrelated-mode":
							differences = append(differences,
								compatibility.Difference{Baseline: base.ID, Axis: "sdk-mode", Status: compatibility.Untested},
								compatibility.Difference{Baseline: base.ID, Axis: "service-version", Status: compatibility.Unknown})
						}
					}
					if !slices.Equal(report.Decisions[0].Differences, differences) {
						t.Error("baseline uncertainty or mismatch diagnostics were lost")
					}
					if repeated := assess(t, build, access, mode, records...); !reflect.DeepEqual(report, repeated) {
						t.Error("repeated assessment changed its report")
					}
				})
				order++
			}
			permute(slices.Clone(test.records), 0)
		})
	}
}

func TestKnownBaselineMismatchesCannotDonateUncertainty(t *testing.T) {
	_, _, access := sourceAccess(t)
	build := inspected(t, buildInfo())
	actual := assess(t, build, access, profile()).Actual
	incomplete := record(actual)
	incomplete.Evidence[2].Covers = nil
	for _, test := range []struct {
		axis   string
		change func(*compatibility.Combination)
	}{
		{"provider", func(base *compatibility.Combination) { base.Provider = "different.provider" }},
		{"go", func(base *compatibility.Combination) { base.Build.Go = observed("go1.26.5") }},
		{"framework", func(base *compatibility.Combination) { base.Build.Framework.Version = observed("v0.2.0") }},
		{"sdk-" + sdkPath, func(base *compatibility.Combination) { base.Build.SDKs[0].Version = observed("v1.1.0") }},
		{"platform", func(base *compatibility.Combination) { base.Build.Platform[0].Value = "arm64" }},
		{"configuration-format", func(base *compatibility.Combination) { base.Format++ }},
		{"source-limits", func(base *compatibility.Combination) { base.Limits.Bytes++ }},
		{"sdk-mode", func(base *compatibility.Combination) { base.Profile.SDKMode = "automatic-ack" }},
		{"effective-options", func(base *compatibility.Combination) { base.Profile.Options[0].Value = "256" }},
		{"service-mode", func(base *compatibility.Combination) { base.Profile.ServiceMode = observed("cluster") }},
		{"service-version", func(base *compatibility.Combination) { base.Profile.ServiceVersion = observed("1.2.4") }},
		{"protocol", func(base *compatibility.Combination) { base.Profile.Protocol = observed("protocol-3") }},
	} {
		t.Run(test.axis, func(t *testing.T) {
			unrelated := record(actual)
			unrelated.ID = "mismatched-baseline"
			unrelated.Baseline.Build = actual.Build.Clone()
			unrelated.Baseline.Profile.Options = slices.Clone(actual.Profile.Options)
			unrelated.Baseline.Profile.Native = compatibility.Fact{}
			test.change(&unrelated.Baseline)
			for _, records := range [][]compatibility.Record{{incomplete, unrelated}, {unrelated, incomplete}} {
				report := assess(t, build, access, profile(), records...)
				assertDecisionPolicies(t, report, compatibility.Untested)
				for _, difference := range []compatibility.Difference{
					{Baseline: unrelated.ID, Axis: test.axis, Status: compatibility.Untested},
					{Baseline: unrelated.ID, Axis: "native", Status: compatibility.Unknown},
				} {
					if !slices.Contains(report.Decisions[0].Differences, difference) {
						t.Error("known mismatch or missing metadata diagnostic lost")
					}
				}
			}
		})
	}
}

func TestUnknownActualFactsRemainUnknownWithMultipleBaselines(t *testing.T) {
	_, _, access := sourceAccess(t)
	build := inspected(t, buildInfo())
	actual := assess(t, build, access, profile()).Actual
	for _, axis := range []string{"build", "profile"} {
		t.Run(axis, func(t *testing.T) {
			current, mode := build.Clone(), profile()
			if axis == "build" {
				current.SDKs[0].Sum = compatibility.Fact{}
			} else {
				mode.ServiceVersion = compatibility.Fact{}
			}
			passed, failed := record(actual), record(actual)
			failed.ID, failed.Evidence[2].Status = "known-baseline-failure", compatibility.Failed
			unrelated := record(actual)
			unrelated.ID, unrelated.Baseline.Provider = "unrelated", "different.provider"
			for _, records := range [][]compatibility.Record{nil, {passed, failed, unrelated}, {unrelated, failed, passed}} {
				report := assess(t, current, access, mode, records...)
				assertDecisionPolicies(t, report, compatibility.Unknown)
				if len(report.Decisions[0].Matched) != 0 {
					t.Error("unknown actual facts established an exact match")
				}
			}
		})
	}
}

func TestAssessmentInputsAndReportsStayIndependent(t *testing.T) {
	_, _, access := sourceAccess(t)
	inputs := func() (compatibility.Build, compatibility.Profile, []compatibility.Record) {
		build, mode := inspected(t, buildInfo()), profile()
		matched := record(assess(t, build, access, mode).Actual)
		unrelated := record(matched.Baseline)
		unrelated.ID, unrelated.Baseline.Provider = "unrelated", "different.provider"
		unrelated.Baseline.Build = matched.Baseline.Build.Clone()
		unrelated.Baseline.Build.SDKs[0].Sum = compatibility.Fact{}
		return build, mode, []compatibility.Record{matched, unrelated}
	}
	build, mode, records := inputs()
	controlBuild, controlMode, controlRecords := inputs()
	report := assess(t, build, access, mode, records...)
	control := assess(t, controlBuild, access, controlMode, controlRecords...)
	assertDecisionPolicies(t, report, compatibility.Tested)
	if !reflect.DeepEqual(build, controlBuild) || !reflect.DeepEqual(mode, controlMode) || !reflect.DeepEqual(records, controlRecords) {
		t.Fatal("assessment mutated caller-owned inputs")
	}
	build.SDKs[0].Version.Value = "changed"
	build.Platform[0].Value = "changed"
	mode.Options[0].Value = "changed"
	for index := range records {
		records[index].Baseline.Build.SDKs[0].Version.Value = "changed"
		records[index].Baseline.Profile.Options[0].Value = "changed"
		records[index].Evidence[0].Covers[0] = compatibility.Resources
		records[index].Limitations[0] = "changed"
	}
	if !reflect.DeepEqual(report, control) {
		t.Fatal("report aliases mutable input metadata")
	}
	report.Baselines[0].Baseline.Build.SDKs[0].Version.Value = "changed"
	report.Baselines[0].Baseline.Profile.Options[0].Value = "changed"
	if !reflect.DeepEqual(report.Actual, control.Actual) || !reflect.DeepEqual(report.Baselines[1], control.Baselines[1]) {
		t.Fatal("actual facts or distinct baselines share mutable slices")
	}
	report.Actual.Profile.Options[0].Value = "changed"
	report.Decisions[0].Matched[0] = "changed"
	report.Decisions[0].Differences[0].Axis = "changed"
	report.Source.Configuration.Provenance[0].Fields = []string{"changed"}
	if repeated := assess(t, controlBuild, access, controlMode, controlRecords...); !reflect.DeepEqual(repeated, control) {
		t.Fatal("report mutation escaped into another evaluation or source metadata")
	}
}
