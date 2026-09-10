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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

type config struct{}

func TestFrameworkCompositionKeepsIndependentEvidence(t *testing.T) {
	identity := fault.Kind("consumer.example.failed")
	if !errors.Is(identity.New(fault.Context{}, context.Canceled), context.Canceled) {
		t.Fatal("cause inspection")
	}
	settings, err := resource.Prepare(resource.Schema[config]{Format: 1}, resource.Input{
		Identity: resource.Identity{Provider: "example.local", Name: "one"}, Format: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	selected := resource.WithLimits(resource.Select(settings, func(context.Context, config) (resource.Resource[func() string], error) {
		return resource.Resource[func() string]{Acquired: true, Capability: func() string { return "one" },
			Release: func(context.Context) resource.ReleaseResult {
				return resource.ReleaseResult{Released: true, Quiescent: true}
			},
		}, nil
	}), resource.Limits{Active: 1, Bytes: 128, MaxLeases: 2})
	assembly, err := resource.Assemble(context.Background(), context.Background(), "consumer", selected)
	if err != nil {
		t.Fatal(err)
	}
	value, _, err := resource.Bind(assembly, selected)
	if err != nil || value() != "one" {
		t.Fatal("public binding")
	}
	access, err := resource.AccessFor(assembly, selected)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[string](1, 128)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	call, err := invocation.Begin(ctx, access, invocation.Request{Name: "read", Shape: invocation.Finite,
		Correlation: fault.Correlation{Call: "external-call"}, Bytes: 128, EvidenceBytes: 128,
		Admission: invocation.Budget{Limit: 1000000000}}, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := call.Execute(ctx, invocation.Budget{Limit: 1000000000}, func(context.Context, invocation.Scope) invocation.Outcome[string] {
		return invocation.Outcome[string]{Value: value(), Present: true}
	}); err != nil {
		t.Fatal(err)
	}
	result, err := call.Receipt().WaitReleased(ctx)
	if err != nil || result.Err() != nil || !result.Final || result.Outcome.Value != "one" {
		t.Fatal("public result")
	}
	wait, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	delivery, err := inbox.Next(wait)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := delivery.Receipt().WaitReleased(wait)
	if err != nil || !evidence.Final || !evidence.Released || evidence.Outcome.Value != "one" || evidence.Context.Correlation.Call != "external-call" {
		t.Fatal("wrong public evidence")
	}
	if err := delivery.Release(); err != nil {
		t.Fatal(err)
	}
	build, err := compatibility.Inspect(compatibility.BuildRequest{
		SDKModules: []string{"go.yaml.in/yaml/v3"}, DisclosePaths: []string{"example.org/consumer"},
	})
	if err != nil {
		t.Fatal(err)
	}
	absent := compatibility.Fact{Kind: compatibility.NotApplicable}
	profile := compatibility.Profile{ImplementationModule: "example.org/consumer", SDKMode: "local",
		ServiceMode: absent, ServiceVersion: absent, Protocol: absent, Native: absent}
	diagnostic, err := compatibility.Assess(build, access, profile,
		[]compatibility.Requirement{{Guarantee: "public-call-provenance", Layers: []compatibility.Layer{compatibility.Mechanism}}}, nil)
	if err != nil || diagnostic.Source.Scope != "consumer" || diagnostic.Actual.SourceRevision != access.Info().Configuration.Revision ||
		!errors.Is(diagnostic.Require(compatibility.Policy{}), compatibility.ErrUnverified) {
		t.Fatal("framework composition diagnostics lost source facts or accepted missing evidence")
	}
	if err := assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
