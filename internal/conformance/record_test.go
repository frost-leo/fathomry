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

package conformance_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/compatibility"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/operation"
)

func TestTraceableFixtureRecordNeverClaimsServiceSupport(t *testing.T) {
	status := compatibility.Passed
	for _, test := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"pull", TestPullIntegration},
		{"async", TestAsyncIntegration},
		{"background", TestBackgroundSessionRequiresStopAndJoin},
		{"bounds", TestBoundedPayloadConcurrencyQueueAndOutstandingEvidence},
	} {
		if !t.Run(test.name, test.run) {
			status = compatibility.Failed
		}
	}
	f := newFixture(t, 1, 4096, 2)
	if !t.Run("recorded-finite-combination", func(t *testing.T) {
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		if call, err := operation.Begin(canceled, f.access, request("canceled", operation.Finite, 4096), f.inbox, nil); call != nil || !errors.Is(err, context.Canceled) {
			t.Error("canceled work crossed the recorded entry boundary")
		}
		input := request("recorded", operation.Finite, 4096)
		call := f.begin(t, input)
		payload := bytes.Repeat([]byte("x"), 4096)
		value := transfer{Bytes: len(payload), Unknown: 1, Digest: sha256.Sum256(payload)}
		if err := call.Execute(context.Background(), operation.Budget{Limit: time.Second}, func(context.Context, operation.Scope) operation.Outcome[transfer] {
			done := f.counts.enter(int64(len(payload)))
			defer done()
			if _, err := call.Attempt(); err != nil {
				t.Error(err)
			}
			return operation.Outcome[transfer]{Present: true, Value: value, Primary: io.ErrUnexpectedEOF}
		}); err != nil {
			t.Fatal(err)
		}
		expected := f.expected(input, value)
		expected.Primary = io.ErrUnexpectedEOF
		conformance.Accounting(t, f.owner.Snapshot().Sources[0].Usage, f.limits, f.inbox.Usage(), 2, 256)
		receive(t, f, expected)
	}) {
		status = compatibility.Failed
	}
	build, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{"go.yaml.in/yaml/v3"}})
	if err != nil {
		t.Fatal(err)
	}
	notApplicable := compatibility.Fact{Kind: compatibility.NotApplicable}
	profile := compatibility.Profile{ImplementationModule: compatibility.FrameworkModule,
		SDKMode: "finite-fixture", ServiceMode: notApplicable, ServiceVersion: notApplicable,
		Protocol: notApplicable, Native: notApplicable,
		Options: []compatibility.Option{{Name: "attempts", Value: "1"}, {Name: "payload-bytes", Value: "4096"},
			{Name: "evidence-count", Value: "2"}, {Name: "evidence-bytes", Value: "256"}, {Name: "execute-budget-ns", Value: "1000000000"}}}
	requirement := []compatibility.Requirement{{Guarantee: "finite-local-handoff", Layers: []compatibility.Layer{compatibility.Mechanism, compatibility.Capability}}}
	captured, err := compatibility.Assess(build, f.access, profile, requirement, nil)
	if err != nil {
		t.Fatal(err)
	}
	behaviors := []compatibility.Behavior{compatibility.Defaults, compatibility.Retries, compatibility.Cancellation,
		compatibility.ErrorIdentity, compatibility.Results, compatibility.Resources}
	record := compatibility.Record{
		ID: "gh-6-transfer-fixtures", Guarantee: "finite-local-handoff",
		StandardRevision: "0c3f9d82947229a861a30fbf9d1730fa02ca44ea",
		Implementation:   "gh-6-reviewed-worktree", Baseline: captured.Actual,
		Evidence: []compatibility.Evidence{
			{Layer: compatibility.Mechanism, Method: compatibility.TestDouble, Status: status, Reference: "TestTraceableFixtureRecordNeverClaimsServiceSupport", Covers: behaviors},
			{Layer: compatibility.Capability, Method: compatibility.TestDouble, Status: status, Reference: "TestTraceableFixtureRecordNeverClaimsServiceSupport", Covers: behaviors},
			{Layer: compatibility.SDK, Method: compatibility.SourceReview, Status: compatibility.NotRun, Reference: "no-transfer-sdk-selected"},
			{Layer: compatibility.Service, Method: compatibility.IsolatedService, Status: compatibility.Skipped, Reference: "service-not-authorized"},
		},
		Limitations: []string{"test-double-not-service", "payload-accounting-not-rss", "no-durable-recording", "test-build-provenance-incomplete"},
	}
	report, err := compatibility.Assess(build, f.access, profile, requirement, []compatibility.Record{record})
	if err != nil {
		t.Fatal(err)
	}
	if report.Baselines[0].Evidence[0].Status != compatibility.Passed || report.Baselines[0].Baseline.Build.Go.Value == "" {
		t.Fatal("record lost executed test status or actual available build facts")
	}
	requirement[0].Layers = append(requirement[0].Layers, compatibility.Service)
	service, err := compatibility.Assess(build, f.access, profile, requirement, []compatibility.Record{record})
	if err != nil || service.Decisions[0].Status == compatibility.Tested || service.Require(compatibility.Policy{}) == nil {
		t.Fatal("skipped service evidence became a support declaration")
	}
}
