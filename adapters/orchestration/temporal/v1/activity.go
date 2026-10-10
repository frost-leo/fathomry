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

// ActivityRun retains its originating Client identity and native semantics.
type ActivityRun struct {
	private
	native *native.ActivityRun
	client *Client
}

func wrapActivityRun(value *native.ActivityRun, client *Client) *ActivityRun {
	if value == nil {
		return nil
	}
	return &ActivityRun{native: value, client: client}
}

// ActivityDescription retains its originating Client identity and native semantics.
type ActivityDescription struct {
	private
	native   *native.ActivityDescription
	client   *Client
	metadata ActivityMetadata
	present  [5]bool
}

func wrapActivityDescription(value *native.ActivityDescription, client *Client) *ActivityDescription {
	if value == nil {
		return nil
	}
	return &ActivityDescription{native: value, client: client, metadata: captureActivityMetadata(value), present: [5]bool{value.HasInput(), value.HasHeartbeatDetails(), value.HasResult(), value.HasOutcomeFailure(), value.HasLastFailure()}}
}

// GetID preserves the selected native contract within this retained use.
func (view *ActivityRun) GetID() string {
	if view == nil || view.native == nil {
		var zero string
		return zero
	}
	return view.native.GetID()
}

// GetRunID preserves the selected native contract within this retained use.
func (view *ActivityRun) GetRunID() string {
	if view == nil || view.native == nil {
		var zero string
		return zero
	}
	return view.native.GetRunID()
}

// ExecuteActivity preserves the selected native contract within this retained use.
func (view *Client) ExecuteActivity(ctx context.Context, options sdk.StartActivityOptions, definition any, args ...any) (*ActivityRun, error) {
	value, err := view.executions().ExecuteActivity(ctx, view.correlation(), options, definition, args...)
	return wrapActivityRun(value, view), translate(err, "executeactivity")
}

// GetActivityHandle preserves the selected native contract within this retained use.
func (view *Client) GetActivityHandle(options sdk.GetActivityHandleOptions) (*ActivityRun, error) {
	value, err := view.executions().GetActivityHandle(options)
	return wrapActivityRun(value, view), translate(err, "getactivityhandle")
}

// Get preserves the selected native contract within this retained use.
func (view *ActivityRun) Get(ctx context.Context, result any) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "get")
	}
	return translate(view.native.Get(ctx, view.client.correlation(), result), "get")
}

// Cancel preserves the selected native contract within this retained use.
func (view *ActivityRun) Cancel(ctx context.Context, options sdk.CancelActivityOptions) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "cancel")
	}
	return translate(view.native.Cancel(ctx, view.client.correlation(), options), "cancel")
}

// Terminate preserves the selected native contract within this retained use.
func (view *ActivityRun) Terminate(ctx context.Context, options sdk.TerminateActivityOptions) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "terminate")
	}
	return translate(view.native.Terminate(ctx, view.client.correlation(), options), "terminate")
}

// CompleteActivity preserves native asynchronous completion and serialization
// context options. The task token is never copied into independent evidence.
func (view *Client) CompleteActivity(ctx context.Context, options sdk.CompleteActivityOptions) error {
	return translate(view.executions().CompleteActivity(ctx, view.correlation(), options), "completeactivity")
}

// CompleteActivityByID completes a Workflow Activity by native IDs. Namespace is
// required by the SDK and must match this source. Empty RunID targets the latest
// Workflow run. IDs do not fence Activity attempts; keep attempt-specific tokens
// when stale completion must be rejected by the Server.
func (view *Client) CompleteActivityByID(ctx context.Context, options sdk.CompleteActivityByIDOptions) error {
	return translate(view.executions().CompleteActivityByID(ctx, view.correlation(), options), "completeactivitybyid")
}

// CompleteActivityByActivityID is the native Standalone Activity completion path.
// ActivityRunID is not a Workflow run; empty targets the latest Activity execution.
func (view *Client) CompleteActivityByActivityID(ctx context.Context, options sdk.CompleteActivityByActivityIDOptions) error {
	return translate(view.executions().CompleteActivityByActivityID(ctx, view.correlation(), options), "completeactivitybyactivityid")
}

// RecordActivityHeartbeat preserves native cancellation/pause/reset errors.
// A successful heartbeat response is recorded separately from the SDK error.
// Tokens and heartbeat payloads are never included in Execution evidence.
func (view *Client) RecordActivityHeartbeat(ctx context.Context, options sdk.RecordActivityHeartbeatOptions) error {
	return translate(view.executions().RecordActivityHeartbeat(ctx, view.correlation(), options), "recordactivityheartbeat")
}

// RecordActivityHeartbeatByID retains native by-ID targeting and error semantics.
// In SDK v1.49.0 this path converts cancellation, but not pause/reset responses,
// into an error. Independently observed response flags remain in the evidence.
func (view *Client) RecordActivityHeartbeatByID(ctx context.Context, options sdk.RecordActivityHeartbeatByIDOptions) error {
	return translate(view.executions().RecordActivityHeartbeatByID(ctx, view.correlation(), options), "recordactivityheartbeatbyid")
}

// Pause is the experimental, namespace-gated native operational control. It
// requires an explicit PauseActivityExecution RPC grant; no grant is implied by
// binding a normal Activity handle. Server capability errors are preserved.
func (view *ActivityRun) Pause(ctx context.Context, options sdk.PauseActivityOptions) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "pause")
	}
	return translate(view.native.Pause(ctx, view.client.correlation(), options), "pause")
}

// Unpause preserves native jitter/time semantics and requires its exact RPC grant.
func (view *ActivityRun) Unpause(ctx context.Context, options sdk.UnpauseActivityOptions) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "unpause")
	}
	return translate(view.native.Unpause(ctx, view.client.correlation(), options), "unpause")
}

// UpdateOptions preserves native set/clear/no-change distinctions. This
// experimental operation requires an UpdateActivityExecutionOptions RPC grant.
func (view *ActivityRun) UpdateOptions(ctx context.Context, options sdk.ActivityOptionsUpdate) (*sdk.ActivityExecutionOptions, error) {
	if view == nil || view.native == nil {
		var zero *sdk.ActivityExecutionOptions
		return zero, fail(ErrInput, "updateoptions")
	}
	value, err := view.native.UpdateOptions(ctx, view.client.correlation(), options)
	return value, translate(err, "updateoptions")
}

// RestoreOriginalOptions is a distinct native request, not a locally synthesized
// default configuration. The same experimental update grant is required.
func (view *ActivityRun) RestoreOriginalOptions(ctx context.Context) (*sdk.ActivityExecutionOptions, error) {
	if view == nil || view.native == nil {
		var zero *sdk.ActivityExecutionOptions
		return zero, fail(ErrInput, "restoreoriginaloptions")
	}
	value, err := view.native.RestoreOriginalOptions(ctx, view.client.correlation())
	return value, translate(err, "restoreoriginaloptions")
}

// Describe requests the native opt-in payload fields without eagerly decoding
// them. The returned view has no connection or source-release authority.
func (view *ActivityRun) Describe(ctx context.Context, options sdk.DescribeActivityOptions) (*ActivityDescription, error) {
	if view == nil || view.native == nil {
		var zero *ActivityDescription
		return zero, fail(ErrInput, "describe")
	}
	value, err := view.native.Describe(ctx, view.client.correlation(), options)
	return wrapActivityDescription(value, view.client), translate(err, "describe")
}

// GetInput preserves the selected native contract within this retained use.
func (view *ActivityDescription) GetInput(ctx context.Context, values ...any) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "getinput")
	}
	return translate(view.native.GetInput(ctx, view.client.correlation(), values...), "getinput")
}

// GetHeartbeatDetails preserves the selected native contract within this retained use.
func (view *ActivityDescription) GetHeartbeatDetails(ctx context.Context, values ...any) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "getheartbeatdetails")
	}
	return translate(view.native.GetHeartbeatDetails(ctx, view.client.correlation(), values...), "getheartbeatdetails")
}

// GetResult preserves the selected native contract within this retained use.
func (view *ActivityDescription) GetResult(ctx context.Context, value any) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "getresult")
	}
	return translate(view.native.GetResult(ctx, view.client.correlation(), value), "getresult")
}

// GetOutcomeFailure preserves the native accessor's error result. HasOutcomeFailure
// distinguishes absent/unrequested failure data; conversion errors remain errors.
func (view *ActivityDescription) GetOutcomeFailure(ctx context.Context) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "getoutcomefailure")
	}
	return translate(view.native.GetOutcomeFailure(ctx, view.client.correlation()), "getoutcomefailure")
}

// GetLastFailure observes the most recent attempt failure, not necessarily the
// terminal execution outcome. It preserves the native error and presence semantics.
func (view *ActivityDescription) GetLastFailure(ctx context.Context) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "getlastfailure")
	}
	return translate(view.native.GetLastFailure(ctx, view.client.correlation()), "getlastfailure")
}

// GetSummary preserves the selected native contract within this retained use.
func (view *ActivityDescription) GetSummary(ctx context.Context) (string, error) {
	if view == nil || view.native == nil {
		var zero string
		return zero, fail(ErrInput, "getsummary")
	}
	value, err := view.native.GetSummary(ctx, view.client.correlation())
	return value, translate(err, "getsummary")
}

// GetStaticDetails preserves the selected native contract within this retained use.
func (view *ActivityDescription) GetStaticDetails(ctx context.Context) (string, error) {
	if view == nil || view.native == nil {
		var zero string
		return zero, fail(ErrInput, "getstaticdetails")
	}
	value, err := view.native.GetStaticDetails(ctx, view.client.correlation())
	return value, translate(err, "getstaticdetails")
}

// CountActivities preserves native visibility aggregation and approximate-group
// semantics. Returned data is caller-owned, not independent execution evidence.
func (view *Client) CountActivities(ctx context.Context, options sdk.CountActivitiesOptions) (*sdk.CountActivitiesResult, error) {
	value, err := view.executions().CountActivities(ctx, view.correlation(), options)
	return value, translate(err, "countactivities")
}
