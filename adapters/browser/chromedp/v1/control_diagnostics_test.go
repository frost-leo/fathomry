/*
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
	"bytes"
	"errors"
	"testing"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/performance"
	"github.com/chromedp/cdproto/profiler"
	sdk "github.com/chromedp/chromedp"
)

func TestLiveManagedProfilerCoverageAndPerformance(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<title>diagnostic fixture</title>", 32)
	options := performance.EnableParams{TimeDomain: performance.EnableTimeDomainTimeTicks}
	control, _, err := retained.Page().Control(retained.Context(), ControlOptions{Profiler: true, Performance: &options})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunDiagnosticCommand(bounded(t), control, profiler.SetSamplingInterval, profiler.SetSamplingIntervalParams{Interval: 100}); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	coverage := profiler.StartPreciseCoverageParams{CallCount: new(true), Detailed: new(true), AllowTriggeredUpdates: new(false)}
	if _, _, err := RunDiagnosticCommand(bounded(t), control, profiler.StartPreciseCoverage, coverage); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunDiagnosticCommand(bounded(t), control, profiler.Start, cdp.Empty{}); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if value, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int64](`function diagnosticWork(){let total=0;for(let i=0;i<100000;i++)total+=i;return total}diagnosticWork();diagnosticWork();diagnosticWork()`)); err != nil || value != 4999950000 {
		t.Fatal("synthetic profiling workload changed", err)
	}
	drain(t, dependencies, 1)
	profile, receipt, err := RunDiagnosticCommand(bounded(t), control, profiler.Stop, cdp.Empty{})
	if err != nil || profile.Profile == nil || len(profile.Profile.Nodes) == 0 || profile.Profile.EndTime < profile.Profile.StartTime {
		t.Fatal("native CPU profile missing", err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	evidence, _ := snapshot.ValueCopy()
	profile.Profile.Nodes[0].CallFrame.FunctionName = "mutated-profile-name"
	if bytes.Contains(evidence.PayloadCopy(), []byte("mutated-profile-name")) {
		t.Fatal("CPU profile aliases its independent evidence")
	}
	drain(t, dependencies, 1)
	result, _, err := RunDiagnosticCommand(bounded(t), control, profiler.TakePreciseCoverage, cdp.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, script := range result.Result {
		for _, function := range script.Functions {
			if function.FunctionName == "diagnosticWork" && len(function.Ranges) > 0 && function.Ranges[0].Count == 3 {
				found = true
			}
		}
	}
	if !found || !*coverage.CallCount || !*coverage.Detailed || *coverage.AllowTriggeredUpdates {
		t.Fatal("native precise coverage options/counts differ")
	}
	drain(t, dependencies, 1)
	metrics, _, err := RunDiagnosticCommand(bounded(t), control, performance.GetMetrics, cdp.Empty{})
	if err != nil || len(metrics.Metrics) == 0 || options.TimeDomain != performance.EnableTimeDomainTimeTicks {
		t.Fatal("explicit native metrics mode did not run", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunDiagnosticCommand(bounded(t), control, profiler.Enable, cdp.Empty{}); !errors.Is(err, ErrUnsupported) {
		t.Fatal("raw enable escaped the managed domain lifetime", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunDiagnosticCommand(bounded(t), control, profiler.Start, cdp.Empty{}); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	fillControlInbox(t, retained, dependencies)
	if err := control.Close(bounded(t)); err != nil || !control.Closed() {
		t.Fatal("ignored CPU/coverage work could not stop at evidence saturation", err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("diagnostic work prevented source cleanup")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveBorrowedProfilerClosePreservesPeerProfiler(t *testing.T) {
	owner, retained, peer, dependencies := borrowedControlFixture(t)
	target := sdk.FromContext(peer).Target
	if _, err := cdp.Call(bounded(t), target, profiler.Enable, cdp.Empty{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cdp.Call(bounded(t), target, profiler.Start, cdp.Empty{}); err != nil {
		t.Fatal(err)
	}
	control, _, err := retained.Page().Control(retained.Context(), ControlOptions{Profiler: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunDiagnosticCommand(bounded(t), control, profiler.Start, cdp.Empty{}); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunDiagnosticCommand(bounded(t), control, performance.GetMetrics, cdp.Empty{}); !errors.Is(err, ErrState) {
		t.Fatal("profiler option granted another diagnostic domain", err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() || !control.Closed() {
		t.Fatal("borrowed profiler did not release")
	}
	profile, err := cdp.Call(bounded(t), target, profiler.Stop, cdp.Empty{})
	if err != nil || profile.Profile == nil {
		t.Fatal("managed cleanup stopped another CDP session's profile", err)
	}
	if _, err := cdp.Call(bounded(t), target, profiler.Disable, cdp.Empty{}); err != nil {
		t.Fatal(err)
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[int]("37")); err != nil || value != 37 {
		t.Fatal("borrowed browser did not survive diagnostic cleanup", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
