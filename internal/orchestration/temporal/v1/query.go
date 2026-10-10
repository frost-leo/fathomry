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

package temporal

import (
	"context"

	"github.com/frost-leo/fathomry/internal/fault"
	commonpb "go.temporal.io/api/common/v1"
	querypb "go.temporal.io/api/query/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

// QueryValue retains one native encoded response and its selected converter.
// Get and RawPayloads are independently admitted and serialized; neither sends
// another query. The originating retained use must remain open. Do not copy it.
type QueryValue struct {
	private
	client            *Executions
	native            converter.EncodedValue
	origin            *nativeCall
	present           bool
	workflowID, runID string
	identityOmitted   bool
	decoding          chan struct{}
}

func newQueryValue(work context.Context, client *Executions, value converter.EncodedValue, origin *nativeCall) (*QueryValue, error) {
	if value == nil {
		return nil, nil
	}
	if nilRuntime(value) {
		return nil, failure(ErrExecution, "query-value")
	}
	observed, release, err := transferEncodedValue(work, client.owner, origin, value)
	defer release()
	if err != nil {
		return nil, err
	}
	return &QueryValue{client: client, native: value, origin: origin, present: observed.HasValue(), decoding: make(chan struct{}, 1)}, nil
}

// HasValue is the native presence observation captured during the query. It
// remains available after use closure without entering a decoder or callback.
func (value *QueryValue) HasValue() bool { return value != nil && value.present }

// QueryWorkflowValue preserves the native encoded response, including absence,
// without decoding it or exposing an unguarded converter.
func (client *Executions) QueryWorkflowValue(ctx context.Context, correlation fault.Correlation, workflowID, runID, query string, args ...any) (*QueryValue, error) {
	var observed *Execution
	value, err := executeNative(ctx, client, correlation, Execution{Operation: "workflow.query", WorkflowID: workflowID, RunID: runID}, func(work context.Context, evidence *Execution) (*QueryValue, error) {
		observed = evidence
		encoded, err := client.owner.native.QueryWorkflow(work, workflowID, runID, query, args...)
		value, valueErr := newQueryValue(work, client, encoded, work.Value(nativeCallKey{}).(*nativeCall))
		if valueErr != nil {
			return nil, valueErr
		}
		evidence.ResultObtained = err == nil && value != nil
		return value, err
	})
	if value != nil {
		value.workflowID, value.runID = observed.WorkflowID, observed.RunID
		value.identityOmitted = observed.IdentityOmitted
	}
	return value, err
}

// QueryWorkflowValueWithOptions preserves native rejection separately from an
// encoded result. The returned rejection is caller-owned native data.
func (client *Executions) QueryWorkflowValueWithOptions(ctx context.Context, correlation fault.Correlation, request *sdk.QueryWorkflowWithOptionsRequest) (*QueryValue, *querypb.QueryRejected, error) {
	if request == nil {
		return nil, nil, failure(ErrInput, "workflow-query")
	}
	var observed *Execution
	var rejected *querypb.QueryRejected
	value, err := executeNative(ctx, client, correlation, Execution{Operation: "workflow.query", WorkflowID: request.WorkflowID, RunID: request.RunID}, func(work context.Context, evidence *Execution) (*QueryValue, error) {
		observed = evidence
		response, err := client.owner.native.QueryWorkflowWithOptions(work, request)
		if response == nil {
			return nil, err
		}
		if response.QueryRejected != nil {
			rejected = response.QueryRejected
			return nil, err
		}
		value, valueErr := newQueryValue(work, client, response.QueryResult, work.Value(nativeCallKey{}).(*nativeCall))
		if valueErr != nil {
			return nil, valueErr
		}
		evidence.ResultObtained = err == nil && value != nil
		return value, err
	})
	if value != nil {
		value.workflowID, value.runID = observed.WorkflowID, observed.RunID
		value.identityOmitted = observed.IdentityOmitted
	}
	return value, rejected, err
}

func readQueryValue[T any](ctx context.Context, value *QueryValue, correlation fault.Correlation, operation string, read func(context.Context, converter.EncodedValue) (T, error)) (T, error) {
	var zero T
	if value == nil || value.client == nil || value.native == nil || value.decoding == nil {
		return zero, failure(ErrInput, operation)
	}
	return executeNative(ctx, value.client, correlation, Execution{Operation: operation, WorkflowID: value.workflowID, RunID: value.runID, IdentityOmitted: value.identityOmitted}, func(work context.Context, evidence *Execution) (T, error) {
		evidence.NativeCalled = false
		select {
		case value.decoding <- struct{}{}:
			defer func() { <-value.decoding }()
		case <-work.Done():
			return zero, work.Err()
		}
		evidence.NativeCalled = true
		encoded, release, err := transferEncodedValue(work, value.client.owner, value.origin, value.native)
		defer release()
		if err != nil {
			return zero, err
		}
		result, err := read(work, encoded)
		evidence.ResultObtained = err == nil
		return result, err
	})
}

// Get decodes the retained response with the exact native converter. Use a fresh
// destination for each extraction; native decoders may merge into existing data.
func (value *QueryValue) Get(ctx context.Context, correlation fault.Correlation, output any) error {
	_, err := readQueryValue(ctx, value, correlation, "workflow.query-result", func(_ context.Context, encoded converter.EncodedValue) (struct{}, error) {
		return struct{}{}, encoded.Get(output)
	})
	return err
}

// RawPayloads invokes the optional native ValuesPayloads accessor under admission
// and returns a detached, source-size-bounded copy. Supported can be true with a
// nil payload; false means the native value does not implement that interface.
// Payload contents are intentionally sensitive caller-owned data.
func (value *QueryValue) RawPayloads(ctx context.Context, correlation fault.Correlation) (*commonpb.Payloads, bool, error) {
	var supported bool
	payloads, err := readQueryValue(ctx, value, correlation, "workflow.query-payloads", func(work context.Context, encoded converter.EncodedValue) (*commonpb.Payloads, error) {
		accessor, ok := encoded.(converter.ValuesPayloads)
		supported = ok
		if !ok {
			return nil, nil
		}
		payloads := accessor.Payloads()
		if payloads == nil {
			return nil, nil
		}
		if _, err := messageSize(work, payloads, value.client.owner.settings.MaxResponseBytes); err != nil {
			return nil, err
		}
		return proto.Clone(payloads).(*commonpb.Payloads), nil
	})
	return payloads, supported, err
}
