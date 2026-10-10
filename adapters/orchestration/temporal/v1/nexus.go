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
	context "context"
	native "github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	sdk "go.temporal.io/sdk/client"
)

// NexusRun retains its originating Client identity and native semantics.
type NexusRun struct {
	private
	native *native.NexusRun
	client *Client
}

func wrapNexusRun(value *native.NexusRun, client *Client) *NexusRun {
	if value == nil {
		return nil
	}
	return &NexusRun{native: value, client: client}
}

// NexusDescription retains its originating Client identity and native semantics.
type NexusDescription struct {
	private
	native   *native.NexusDescription
	client   *Client
	metadata NexusMetadata
}

func wrapNexusDescription(value *native.NexusDescription, client *Client) *NexusDescription {
	if value == nil {
		return nil
	}
	return &NexusDescription{native: value, client: client, metadata: captureNexusMetadata(value)}
}

// NexusCancellation retains its originating Client identity and native semantics.
type NexusCancellation struct {
	private
	native *native.NexusCancellation
	client *Client
}

func wrapNexusCancellation(value *native.NexusCancellation, client *Client) *NexusCancellation {
	if value == nil {
		return nil
	}
	return &NexusCancellation{native: value, client: client}
}

// GetID preserves the selected native contract within this retained use.
func (view *NexusRun) GetID() string {
	if view == nil || view.native == nil {
		var zero string
		return zero
	}
	return view.native.GetID()
}

// GetRunID preserves the selected native contract within this retained use.
func (view *NexusRun) GetRunID() string {
	if view == nil || view.native == nil {
		var zero string
		return zero
	}
	return view.native.GetRunID()
}

// ExecuteNexusOperation preserves native options and operation-reference typing.
// API availability is not namespace support; native capability errors remain
// errors. This operation enables no server setting or administrative authority.
func (view *Client) ExecuteNexusOperation(ctx context.Context, target sdk.NexusClientOptions, definition any, input any, options sdk.StartNexusOperationOptions) (*NexusRun, error) {
	value, err := view.executions().ExecuteNexusOperation(ctx, view.correlation(), target, definition, input, options)
	return wrapNexusRun(value, view), translate(err, "executenexusoperation")
}

// GetNexusOperationHandle defers native interceptor entry to the admitted call.
func (view *Client) GetNexusOperationHandle(options sdk.GetNexusOperationHandleOptions) (*NexusRun, error) {
	value, err := view.executions().GetNexusOperationHandle(options)
	return wrapNexusRun(value, view), translate(err, "getnexusoperationhandle")
}

// Get preserves the selected native contract within this retained use.
func (view *NexusRun) Get(ctx context.Context, result any) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "get")
	}
	return translate(view.native.Get(ctx, view.client.correlation(), result), "get")
}

// Cancel preserves the selected native contract within this retained use.
func (view *NexusRun) Cancel(ctx context.Context, options sdk.CancelNexusOperationOptions) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "cancel")
	}
	return translate(view.native.Cancel(ctx, view.client.correlation(), options), "cancel")
}

// Terminate preserves the selected native contract within this retained use.
func (view *NexusRun) Terminate(ctx context.Context, options sdk.TerminateNexusOperationOptions) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "terminate")
	}
	return translate(view.native.Terminate(ctx, view.client.correlation(), options), "terminate")
}

// Describe preserves the selected native contract within this retained use.
func (view *NexusRun) Describe(ctx context.Context, options sdk.DescribeNexusOperationOptions) (*NexusDescription, error) {
	if view == nil || view.native == nil {
		var zero *NexusDescription
		return zero, fail(ErrInput, "describe")
	}
	value, err := view.native.Describe(ctx, view.client.correlation(), options)
	return wrapNexusDescription(value, view.client), translate(err, "describe")
}

// GetSummary preserves the selected native contract within this retained use.
func (view *NexusDescription) GetSummary(ctx context.Context) (string, error) {
	if view == nil || view.native == nil {
		var zero string
		return zero, fail(ErrInput, "getsummary")
	}
	value, err := view.native.GetSummary(ctx, view.client.correlation())
	return value, translate(err, "getsummary")
}

// GetLastAttemptFailure preserves the selected native contract within this retained use.
func (view *NexusDescription) GetLastAttemptFailure(ctx context.Context) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "getlastattemptfailure")
	}
	return translate(view.native.GetLastAttemptFailure(ctx, view.client.correlation()), "getlastattemptfailure")
}

// GetLastAttemptFailure preserves the selected native contract within this retained use.
func (view *NexusCancellation) GetLastAttemptFailure(ctx context.Context) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "getlastattemptfailure")
	}
	return translate(view.native.GetLastAttemptFailure(ctx, view.client.correlation()), "getlastattemptfailure")
}

// WalkNexusOperations owns the native experimental lazy sequence.
func (view *Client) WalkNexusOperations(ctx context.Context, options sdk.ListNexusOperationsOptions, visit func(context.Context, *sdk.NexusOperationMetadata) error) error {
	return translate(view.executions().WalkNexusOperations(ctx, view.correlation(), options, visit), "walknexusoperations")
}

// CountNexusOperations preserves the selected native contract within this retained use.
func (view *Client) CountNexusOperations(ctx context.Context, options sdk.CountNexusOperationsOptions) (*sdk.CountNexusOperationsResult, error) {
	value, err := view.executions().CountNexusOperations(ctx, view.correlation(), options)
	return value, translate(err, "countnexusoperations")
}
