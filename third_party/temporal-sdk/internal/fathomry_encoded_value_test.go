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

package internal

import (
	"context"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
)

type fathomryPollValueInterceptor struct {
	ClientOutboundInterceptorBase
	value converter.EncodedValue
	polls int
}

func (poller *fathomryPollValueInterceptor) PollWorkflowUpdate(context.Context, *ClientPollWorkflowUpdateInput) (*ClientPollWorkflowUpdateOutput, error) {
	poller.polls++
	return &ClientPollWorkflowUpdateOutput{Result: poller.value}, nil
}
func (poller *fathomryPollValueInterceptor) PollActivityResult(context.Context, *ClientPollActivityResultInput) (*ClientPollActivityResultOutput, error) {
	poller.polls++
	return &ClientPollActivityResultOutput{Result: poller.value}, nil
}
func (poller *fathomryPollValueInterceptor) PollNexusOperationResult(context.Context, *ClientPollNexusOperationResultInput) (*ClientPollNexusOperationResultOutput, error) {
	poller.polls++
	return &ClientPollNexusOperationResultOutput{Result: poller.value}, nil
}

type fathomryEncodedContextKey struct{}

func TestFathomryNativeEncodedConsumptionCapturesOriginAcrossCachedGets(t *testing.T) {
	for _, hooked := range []bool{false, true} {
		for _, kind := range []string{"completed-update", "fallback-completed-update", "lazy-update", "activity", "nexus"} {
			name := kind + "/native-default"
			if hooked {
				name = kind + "/scoped"
			}
			t.Run(name, func(t *testing.T) {
				payloads, err := converter.GetDefaultDataConverter().ToPayloads("value")
				if err != nil {
					t.Fatal(err)
				}
				poller := &fathomryPollValueInterceptor{value: newEncodedValue(payloads, converter.GetDefaultDataConverter())}
				client := &WorkflowClient{namespace: "fixture", interceptor: poller, capabilities: &workflowservice.GetSystemInfoResponse_Capabilities{},
					dataConverter: converter.GetDefaultDataConverter(), failureConverter: GetDefaultFailureConverter()}
				firstCalls, laterCalls := 0, 0
				var contexts []string
				first := context.WithValue(context.Background(), fathomryEncodedContextKey{}, "first")
				later := context.WithValue(context.Background(), fathomryEncodedContextKey{}, "later")
				if hooked {
					first = FathomryWithEncodedValueDecoderV1(first, func(ctx context.Context, value converter.EncodedValue, output any) error {
						firstCalls++
						contexts = append(contexts, ctx.Value(fathomryEncodedContextKey{}).(string))
						return value.Get(output)
					})
				}
				later = FathomryWithEncodedValueDecoderV1(later, func(ctx context.Context, value converter.EncodedValue, output any) error {
					laterCalls++
					contexts = append(contexts, ctx.Value(fathomryEncodedContextKey{}).(string))
					return value.Get(output)
				})
				ref := &updatepb.UpdateRef{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "workflow", RunId: "run"}, UpdateId: "update"}
				var get func(context.Context, any) error
				switch kind {
				case "completed-update", "fallback-completed-update":
					response := &workflowservice.UpdateWorkflowExecutionResponse{UpdateRef: ref, Stage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED}
					if kind == "completed-update" {
						response.Stage = enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
						response.Outcome = &updatepb.Outcome{Value: &updatepb.Outcome_Success{Success: payloads}}
					}
					handle, err := (&workflowClientInterceptor{client: client}).updateHandleFromResponse(first, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED, response)
					if err != nil || !IsUpdateWorkflowCompleted(handle) {
						t.Fatal("native completed Update identity changed", err)
					}
					get = handle.Get
				case "lazy-update":
					get = (&lazyUpdateHandle{client: client, baseUpdateHandle: baseUpdateHandle{ref: ref}}).Get
				case "activity":
					get = (&clientActivityHandleImpl{client: client, id: "activity", runID: "run"}).Get
				case "nexus":
					get = (&clientNexusOperationHandleImpl{client: client, id: "operation", runID: "run"}).Get
				}
				var output string
				if err := get(first, &output); err != nil || output != "value" {
					t.Fatal("first native decode changed", err)
				}
				output = ""
				if err := get(later, &output); err != nil || output != "value" {
					t.Fatal("later native decode changed", err)
				}
				wantFirst, wantLater := 0, 0
				if hooked {
					wantFirst = 2
				}
				if kind == "lazy-update" {
					wantLater = 1
					if hooked {
						wantFirst = 1
					}
				}
				if firstCalls != wantFirst || laterCalls != wantLater {
					t.Errorf("native result lost acquiring decoder hook: first=%d later=%d want=%d/%d", firstCalls, laterCalls, wantFirst, wantLater)
				}
				if hooked && (len(contexts) != 2 || contexts[0] != "first" || contexts[1] != "later") {
					t.Error("captured decoder did not receive each Get caller's context", contexts)
				}
				wantPolls := 1
				if kind == "completed-update" {
					wantPolls = 0
				} else if kind == "lazy-update" {
					wantPolls = 2
				}
				if poller.polls != wantPolls {
					t.Fatal("native poll/cache behavior changed", poller.polls)
				}
			})
		}
	}
}
