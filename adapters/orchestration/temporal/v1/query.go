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

	native "github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	commonpb "go.temporal.io/api/common/v1"
	querypb "go.temporal.io/api/query/v1"
	sdk "go.temporal.io/sdk/client"
)

// QueryValue retains one native query response and its original use/converter.
// Presence is local; Get and RawPayloads are admitted independently without
// re-querying. Closing the originating Client revokes later decoding even while
// another Client sharing the physical source remains open. Do not copy it.
type QueryValue struct {
	private
	native *native.QueryValue
	client *Client
}

func queryValue(value *native.QueryValue, client *Client) *QueryValue {
	if value == nil {
		return nil
	}
	return &QueryValue{native: value, client: client}
}

// QueryWorkflowValue preserves encoded-value presence and repeated native
// decoding of the same response. QueryWorkflow is the eager-decode convenience.
func (client *Client) QueryWorkflowValue(ctx context.Context, workflowID, runID, query string, args ...any) (*QueryValue, error) {
	value, err := client.executions().QueryWorkflowValue(ctx, client.correlation(), workflowID, runID, query, args...)
	return queryValue(value, client), translate(err, "queryworkflowvalue")
}

// QueryWorkflowValueWithOptions separates a native rejection from an encoded
// value. A non-nil rejection is caller-owned data, not a decoded query result.
func (client *Client) QueryWorkflowValueWithOptions(ctx context.Context, request *sdk.QueryWorkflowWithOptionsRequest) (*QueryValue, *querypb.QueryRejected, error) {
	value, rejected, err := client.executions().QueryWorkflowValueWithOptions(ctx, client.correlation(), request)
	return queryValue(value, client), rejected, translate(err, "queryworkflowvaluewithoptions")
}

func (value *QueryValue) HasValue() bool {
	return value != nil && value.native != nil && value.native.HasValue()
}

// Get extracts into a fresh destination under the originating use. Concurrent
// extractions are serialized; cancellation is cooperative and does not re-query.
func (value *QueryValue) Get(ctx context.Context, output any) error {
	if value == nil || value.native == nil {
		return fail(ErrInput, "query-result")
	}
	return translate(value.native.Get(ctx, value.client.correlation(), output), "query-result")
}

// RawPayloads returns a detached bounded copy through the optional native
// ValuesPayloads interface. The boolean reports interface support, independently
// of nil/absent data. Custom accessors run under admission and may be cooperative
// work; returned payloads are intentionally sensitive caller-owned data.
func (value *QueryValue) RawPayloads(ctx context.Context) (*commonpb.Payloads, bool, error) {
	if value == nil || value.native == nil {
		return nil, false, fail(ErrInput, "query-payloads")
	}
	payloads, supported, err := value.native.RawPayloads(ctx, value.client.correlation())
	return payloads, supported, translate(err, "query-payloads")
}
