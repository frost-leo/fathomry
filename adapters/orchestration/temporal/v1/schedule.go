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

// Schedule retains its originating Client identity and native semantics.
type Schedule struct {
	private
	native *native.Schedule
	client *Client
}

func wrapSchedule(value *native.Schedule, client *Client) *Schedule {
	if value == nil {
		return nil
	}
	return &Schedule{native: value, client: client}
}

// GetID preserves the selected native contract within this retained use.
func (view *Schedule) GetID() string {
	if view == nil || view.native == nil {
		var zero string
		return zero
	}
	return view.native.GetID()
}

// CreateSchedule preserves native calendar, timezone, overlap, catchup, initial
// patch and action options. It does not generate an ID or retry a new intention.
func (view *Client) CreateSchedule(ctx context.Context, options sdk.ScheduleOptions) (*Schedule, error) {
	value, err := view.executions().CreateSchedule(ctx, view.correlation(), options)
	return wrapSchedule(value, view), translate(err, "createschedule")
}

// GetSchedule constructs a local handle without network access. Missing schedules
// are reported by native operations, not inferred from an empty list/visibility.
func (view *Client) GetSchedule(id string) (*Schedule, error) {
	value, err := view.executions().GetSchedule(id)
	return wrapSchedule(value, view), translate(err, "getschedule")
}

// Describe returns caller-owned native metadata and payloads; native metadata
// decoding runs within admission. Observation does not lock subsequent updates.
func (view *Schedule) Describe(ctx context.Context) (*sdk.ScheduleDescription, error) {
	if view == nil || view.native == nil {
		var zero *sdk.ScheduleDescription
		return zero, fail(ErrInput, "describe")
	}
	value, err := view.native.Describe(ctx, view.client.correlation())
	return value, translate(err, "describe")
}

// Update owns the native read/DoUpdate/write sequence, including a slow callback.
// Cancellation does not forcibly stop DoUpdate. DoUpdate must not retain or use
// borrowed extensions asynchronously; its errors remain independently recorded.
// Native ErrSkipScheduleUpdate sends no update. SDK v1.49.0 does not send a conflict
// token: callers must not treat DoUpdate as transactional or automatically retry
// after an uncertain response. ACK does not prove application of the new state.
func (view *Schedule) Update(ctx context.Context, options sdk.ScheduleUpdateOptions) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "update")
	}
	return translate(view.native.Update(ctx, view.client.correlation(), options), "update")
}

// Delete preserves the selected native contract within this retained use.
func (view *Schedule) Delete(ctx context.Context) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "delete")
	}
	return translate(view.native.Delete(ctx, view.client.correlation()), "delete")
}

// Trigger preserves the selected native contract within this retained use.
func (view *Schedule) Trigger(ctx context.Context, options sdk.ScheduleTriggerOptions) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "trigger")
	}
	return translate(view.native.Trigger(ctx, view.client.correlation(), options), "trigger")
}

// Backfill preserves the selected native contract within this retained use.
func (view *Schedule) Backfill(ctx context.Context, options sdk.ScheduleBackfillOptions) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "backfill")
	}
	return translate(view.native.Backfill(ctx, view.client.correlation(), options), "backfill")
}

// Pause preserves the selected native contract within this retained use.
func (view *Schedule) Pause(ctx context.Context, options sdk.SchedulePauseOptions) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "pause")
	}
	return translate(view.native.Pause(ctx, view.client.correlation(), options), "pause")
}

// Unpause preserves the selected native contract within this retained use.
func (view *Schedule) Unpause(ctx context.Context, options sdk.ScheduleUnpauseOptions) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "unpause")
	}
	return translate(view.native.Unpause(ctx, view.client.correlation(), options), "unpause")
}

// WalkSchedules owns native lazy pagination and synchronous visitor callbacks in
// one admitted call. It fetches at most one native page at a time; each RPC retains
// the configured wire bound. The visitor owns each entry it retains. A visitor
// error stops iteration and is preserved; no detached iterator escapes with an
// expired context. Cancellation is cooperative and does not interrupt a blocked
// visitor. The list is eventually consistent, not proof of schedule absence.
func (view *Client) WalkSchedules(ctx context.Context, options sdk.ScheduleListOptions, visit func(context.Context, *sdk.ScheduleListEntry) error) error {
	return translate(view.executions().WalkSchedules(ctx, view.correlation(), options, visit), "walkschedules")
}
