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

package declaration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

type controlledCancel struct {
	context.Context
	stop      context.CancelFunc
	remaining int
}

func (ctx *controlledCancel) Err() error {
	ctx.remaining--
	if ctx.remaining <= 0 {
		ctx.stop()
	}
	return ctx.Context.Err()
}
func cancelAfter(calls int) (context.Context, context.CancelFunc) {
	base, stop := context.WithCancel(context.Background())
	return &controlledCancel{Context: base, stop: stop, remaining: calls}, stop
}

func TestCancellationDuringProcessing(t *testing.T) {
	input := fixtureDeclaration(t)
	bundle := mustCompile(t, input)
	raw, _ := bundle.BytesCopy()
	for _, operation := range []struct {
		name string
		run  func(context.Context) error
	}{
		{"compile", func(ctx context.Context) error {
			result, err := Compile(ctx, input)
			if result != nil {
				t.Fatal("partial compile")
			}
			return err
		}},
		{"parse", func(ctx context.Context) error {
			result, err := Parse(ctx, raw)
			if result != nil {
				t.Fatal("partial parse")
			}
			return err
		}},
	} {
		t.Run(operation.name, func(t *testing.T) {
			ctx, stop := cancelAfter(25)
			defer stop()
			if err := operation.run(ctx); !errors.Is(err, ErrCanceled) || !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
	cause := errors.New("private-cancel-cause")
	ctx, stop := context.WithCancelCause(context.Background())
	stop(cause)
	_, err := Parse(ctx, raw)
	if !errors.Is(err, cause) || !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "private-") {
		t.Fatal("cause lost or leaked", err)
	}
}

func TestConflictReportBound(t *testing.T) {
	makeInput := func(value Decimal) Declaration {
		base := fixtureDeclaration(t).Workflows[0]
		result := Declaration{}
		for index := range 4 {
			workflow := base
			workflow.Code = fmt.Sprintf("health_%d", index)
			workflow.Modes = nil
			for mode := range MaxModesPerWorkflow {
				workflow.Modes = append(workflow.Modes, ModeDeclaration{Code: fmt.Sprintf("mode_%d", mode), Revision: "1", Values: map[string]Decimal{"attempt_timeout": value}})
			}
			result.Workflows = append(result.Workflows, workflow)
		}
		return result
	}
	left, right := mustCompile(t, makeInput("1000")), mustCompile(t, makeInput("1200"))
	report, err := Check(context.Background(), left, right)
	if err != nil || report.Passed || !report.Truncated || len(report.Conflicts) != MaxDiagnostics || report.Workflows != 4 || report.Modes != MaxModes {
		t.Fatal("untruthful bounded report", report, err)
	}
	ctx, stop := cancelAfter(20)
	defer stop()
	report, err = Check(ctx, left, right)
	if !errors.Is(err, ErrCanceled) || report.Inputs != 0 {
		t.Fatal("partial report after cancellation", report, err)
	}
}

func TestMaterializedBundleValueBound(t *testing.T) {
	for _, count := range []int{1536, 2048} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			spec := Specification{
				Contracts: map[string]DataSchema{"result": {Type: "boolean"}},
				Bindings: map[string]Binding{"read": {Type: "example/read", Revision: "1", RetrySafety: "read_only",
					Outputs: map[string]Port{"result": {Contract: "result", Delivery: "value", Required: true}}}},
				Nodes:    map[string]Node{},
				Controls: map[string]Control{},
				Limits: Limits{ActiveChunks: Fixed("1"), ItemsPerChunk: Fixed("1"), ChunkItemConcurrency: Fixed("1"),
					RunTimeoutMS: Fixed("10"), CleanupReserveMS: Fixed("1")},
				Completion: Completion{Item: "selected_paths_and_required_effects", Run: "all_succeeded", MaximumFailedItems: "0"},
			}
			one := Decimal("1")
			next := 0
			setting := func() Setting {
				name := fmt.Sprintf("c%04d", next)
				next++
				spec.Controls[name] = Control{Kind: "duration_ms", Minimum: one, Maximum: one, Default: &one}
				return Tuned(name)
			}
			for index := range count / 4 {
				name := fmt.Sprintf("n%d", index/256)
				node, ok := spec.Nodes[name]
				if !ok {
					node = Node{Activities: map[string]Activity{}, Phases: [][]string{{}}, Join: "all",
						Routes: []string{"continue"}, Route: Route{Kind: "fixed", Value: "continue"}}
				}
				call := fmt.Sprintf("a%d", index)
				node.Activities[call] = Activity{Binding: "read", Policy: Policy{
					StartToCloseMS: setting(), ScheduleToCloseMS: setting(), InitialIntervalMS: setting(), MaximumIntervalMS: setting(),
					MaximumAttempts: Fixed("1"), BackoffMilli: Fixed("1000"),
				}}
				node.Phases[0] = append(node.Phases[0], call)
				spec.Nodes[name] = node
			}
			input := Declaration{}
			for index := range 4 {
				workflow := WorkflowDeclaration{Namespace: "example", Code: fmt.Sprintf("w%d", index), Version: "1", Spec: spec}
				for mode := range MaxModesPerWorkflow {
					workflow.Modes = append(workflow.Modes, ModeDeclaration{Code: fmt.Sprintf("m%d", mode), Revision: "1"})
				}
				input.Workflows = append(input.Workflows, workflow)
			}
			bundle, err := Compile(context.Background(), input)
			if count == 2048 {
				if bundle != nil || !errors.Is(err, ErrLimit) {
					var parsed error
					if bundle != nil {
						raw, _ := bundle.BytesCopy()
						_, parsed = Parse(context.Background(), raw)
					}
					detail, _ := Diagnose(parsed)
					t.Fatalf("expanded value budget escaped: compile=%v, parse=%+v", err, detail)
				}
				if detail, ok := Diagnose(err); !ok || detail.Reason != "values" {
					t.Fatalf("wrong expansion limit: %+v / %v", detail, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := bundle.BytesCopy()
			if _, err := Parse(context.Background(), raw); err != nil {
				t.Fatal("admitted expansion did not round trip", err)
			}
		})
	}
}
