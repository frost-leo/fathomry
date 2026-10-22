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

package health

import wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"

// Declarations returns a synthetic health workflow and two immutable mode revisions.
func Declarations() wf.Declaration {
	defaultTimeout := wf.Decimal("1000")
	spec := wf.Specification{
		Contracts: map[string]wf.DataSchema{
			"target":  {Type: "string", MinLength: integer(1), MaxLength: integer(128)},
			"metric":  {Type: "integer", Minimum: integer(0), Maximum: integer(10000)},
			"summary": {Type: "string", MinLength: integer(1), MaxLength: integer(256)},
		},
		Bindings: map[string]wf.Binding{
			"read_cpu": {
				Type: "health/read-cpu", Revision: "1", RetrySafety: "read_only",
				Inputs:  map[string]wf.Port{"target": port("target")},
				Outputs: map[string]wf.Port{"value": port("metric")},
			},
			"read_memory": {
				Type: "health/read-memory", Revision: "1", RetrySafety: "read_only",
				Inputs:  map[string]wf.Port{"target": port("target")},
				Outputs: map[string]wf.Port{"value": port("metric")},
			},
			"summarize": {
				Type: "health/summarize", Revision: "1", RetrySafety: "read_only",
				Inputs:  map[string]wf.Port{"cpu": port("metric"), "memory": port("metric")},
				Outputs: map[string]wf.Port{"text": port("summary")},
			},
		},
		Inputs:  map[string]wf.Port{"target": port("target")},
		Outputs: map[string]wf.Export{"summary": {Node: "summary", Port: "text", Required: true}},
		Nodes: map[string]wf.Node{
			"sample": {
				Inputs: map[string]wf.Input{
					"target": input("target", wf.Source{Kind: "root", Port: "target"}),
				},
				Outputs: map[string]wf.Source{
					"cpu":    {Kind: "activity", Activity: "cpu", Port: "value"},
					"memory": {Kind: "activity", Activity: "memory", Port: "value"},
				},
				Activities: map[string]wf.Activity{
					"cpu": {Binding: "read_cpu", Policy: readPolicy(),
						Inputs: map[string]wf.Source{"target": {Kind: "input", Port: "target"}}},
					"memory": {Binding: "read_memory", Policy: readPolicy(),
						Inputs: map[string]wf.Source{"target": {Kind: "input", Port: "target"}}},
				},
				Phases: [][]string{{"cpu", "memory"}}, Join: "all", Routes: []string{"continue"},
				Route: wf.Route{Kind: "fixed", Value: "continue"},
			},
			"summary": {
				Inputs: map[string]wf.Input{
					"cpu":    input("metric", wf.Source{Kind: "edge", Edge: "sampled", Port: "cpu"}),
					"memory": input("metric", wf.Source{Kind: "edge", Edge: "sampled", Port: "memory"}),
				},
				Outputs: map[string]wf.Source{"text": {Kind: "activity", Activity: "format", Port: "text"}},
				Activities: map[string]wf.Activity{
					"format": {Binding: "summarize", Policy: readPolicy(), Inputs: map[string]wf.Source{
						"cpu": {Kind: "input", Port: "cpu"}, "memory": {Kind: "input", Port: "memory"},
					}},
				},
				Phases: [][]string{{"format"}}, Join: "all", Routes: []string{"continue"},
				Route: wf.Route{Kind: "fixed", Value: "continue"},
			},
		},
		Edges: map[string]wf.Edge{"sampled": {From: "sample", To: "summary", On: []string{"continue"}}},
		Controls: map[string]wf.Control{
			"attempt_timeout": {Kind: "duration_ms", Minimum: "100", Maximum: "1500", Default: &defaultTimeout},
		},
		Limits: wf.Limits{
			ActiveChunks: wf.Fixed("1"), ItemsPerChunk: wf.Fixed("1"), ChunkItemConcurrency: wf.Fixed("1"),
			RunTimeoutMS: wf.Fixed("10000"), CleanupReserveMS: wf.Fixed("1000"),
		},
		Completion: wf.Completion{
			Item: "selected_paths_and_required_effects", Run: "all_succeeded", MaximumFailedItems: "0",
		},
	}
	return wf.Declaration{Workflows: []wf.WorkflowDeclaration{{
		Namespace: "example", Code: "server-health", Version: "1", Spec: spec,
		Modes: []wf.ModeDeclaration{
			{Code: "standard", Revision: "1"},
			{Code: "patient", Revision: "1", Values: map[string]wf.Decimal{"attempt_timeout": "1500"}},
		},
	}}}
}

func readPolicy() wf.Policy {
	return wf.Policy{
		StartToCloseMS: wf.Tuned("attempt_timeout"), ScheduleToCloseMS: wf.Fixed("2000"),
		MaximumAttempts: wf.Fixed("1"), InitialIntervalMS: wf.Fixed("10"),
		MaximumIntervalMS: wf.Fixed("10"), BackoffMilli: wf.Fixed("1000"),
	}
}

func integer(value int64) *int64 { return &value }

func port(contract string) wf.Port {
	return wf.Port{Contract: contract, Delivery: "value", Required: true}
}

func input(contract string, source wf.Source) wf.Input {
	return wf.Input{Contract: contract, Delivery: "value", Required: true, Source: source}
}
