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
	"log/slog"

	"github.com/frost-leo/fathomry/internal/fault"
	sdk "go.temporal.io/sdk/client"
)

// Schedule is a non-owning, namespace-bound native Schedule handle. Its ID is a
// local snapshot, not proof of existence. Concurrent updates retain native lost-
// update risk; neither a successful RPC nor this handle provides compare-and-swap.
type Schedule struct {
	private
	client *Executions
	native sdk.ScheduleHandle
	id     string
}

type schedulePageKey struct{}

type schedulePage struct{ more bool }

func (*Schedule) LogValue() slog.Value { return slog.StringValue("temporal[restricted]") }

func (schedule *Schedule) GetID() string {
	if schedule == nil {
		return ""
	}
	return schedule.id
}

// CreateSchedule preserves native calendar, timezone, overlap, catchup, initial
// patch and action options. It does not generate an ID or retry a new intention.
func (client *Executions) CreateSchedule(ctx context.Context, correlation fault.Correlation, options sdk.ScheduleOptions) (*Schedule, error) {
	return executeNative(ctx, client, correlation, Execution{Operation: "schedule.create", ScheduleID: options.ID}, func(work context.Context, evidence *Execution) (*Schedule, error) {
		native, err := client.owner.native.ScheduleClient().Create(work, options)
		if native == nil {
			return nil, err
		}
		evidence.ScheduleID = native.GetID()
		return &Schedule{client: client, native: native, id: native.GetID()}, err
	})
}

// GetSchedule constructs a local handle without network access. Missing schedules
// are reported by native operations, not inferred from an empty list/visibility.
func (client *Executions) GetSchedule(id string) (*Schedule, error) {
	if client == nil || client.owner == nil || !validText(id, 1024) {
		return nil, failure(ErrInput, "schedule-handle")
	}
	return &Schedule{client: client, native: client.owner.native.ScheduleClient().GetHandle(context.Background(), id), id: id}, nil
}

func scheduleCall[T any](ctx context.Context, schedule *Schedule, correlation fault.Correlation, operation string, invoke func(context.Context, *Execution) (T, error)) (T, error) {
	if schedule == nil || schedule.native == nil {
		var zero T
		return zero, failure(ErrInput, operation)
	}
	return executeNative(ctx, schedule.client, correlation, Execution{Operation: operation, ScheduleID: schedule.id}, invoke)
}

// Describe returns caller-owned native metadata and payloads; native metadata
// decoding runs within admission. Observation does not lock subsequent updates.
func (schedule *Schedule) Describe(ctx context.Context, correlation fault.Correlation) (*sdk.ScheduleDescription, error) {
	return scheduleCall(ctx, schedule, correlation, "schedule.describe", func(work context.Context, evidence *Execution) (*sdk.ScheduleDescription, error) {
		value, err := schedule.native.Describe(work)
		evidence.ResultObtained = err == nil
		return value, err
	})
}

// Update owns the native read/DoUpdate/write sequence, including a slow callback.
// Cancellation does not forcibly stop DoUpdate. DoUpdate must not retain or use
// borrowed extensions asynchronously; its errors remain independently recorded.
// Native ErrSkipScheduleUpdate sends no update. SDK v1.49.0 does not send a conflict
// token: callers must not treat DoUpdate as transactional or automatically retry
// after an uncertain response. ACK does not prove application of the new state.
func (schedule *Schedule) Update(ctx context.Context, correlation fault.Correlation, options sdk.ScheduleUpdateOptions) error {
	if options.DoUpdate == nil {
		return failure(ErrInput, "schedule-update-callback")
	}
	_, err := scheduleCall(ctx, schedule, correlation, "schedule.update", func(work context.Context, _ *Execution) (struct{}, error) {
		return struct{}{}, schedule.native.Update(work, options)
	})
	return err
}

func (schedule *Schedule) Delete(ctx context.Context, correlation fault.Correlation) error {
	_, err := scheduleCall(ctx, schedule, correlation, "schedule.delete", func(work context.Context, _ *Execution) (struct{}, error) {
		return struct{}{}, schedule.native.Delete(work)
	})
	return err
}

func (schedule *Schedule) Trigger(ctx context.Context, correlation fault.Correlation, options sdk.ScheduleTriggerOptions) error {
	_, err := scheduleCall(ctx, schedule, correlation, "schedule.trigger", func(work context.Context, _ *Execution) (struct{}, error) {
		return struct{}{}, schedule.native.Trigger(work, options)
	})
	return err
}

func (schedule *Schedule) Backfill(ctx context.Context, correlation fault.Correlation, options sdk.ScheduleBackfillOptions) error {
	_, err := scheduleCall(ctx, schedule, correlation, "schedule.backfill", func(work context.Context, _ *Execution) (struct{}, error) {
		return struct{}{}, schedule.native.Backfill(work, options)
	})
	return err
}

func (schedule *Schedule) Pause(ctx context.Context, correlation fault.Correlation, options sdk.SchedulePauseOptions) error {
	_, err := scheduleCall(ctx, schedule, correlation, "schedule.pause", func(work context.Context, _ *Execution) (struct{}, error) {
		return struct{}{}, schedule.native.Pause(work, options)
	})
	return err
}

func (schedule *Schedule) Unpause(ctx context.Context, correlation fault.Correlation, options sdk.ScheduleUnpauseOptions) error {
	_, err := scheduleCall(ctx, schedule, correlation, "schedule.unpause", func(work context.Context, _ *Execution) (struct{}, error) {
		return struct{}{}, schedule.native.Unpause(work, options)
	})
	return err
}

// WalkSchedules owns native lazy pagination and synchronous visitor callbacks in
// one admitted call. It fetches at most one native page at a time; each RPC retains
// the configured wire bound. The visitor owns each entry it retains. A visitor
// error stops iteration and is preserved; no detached iterator escapes with an
// expired context. Cancellation is cooperative and does not interrupt a blocked
// visitor. The list is eventually consistent, not proof of schedule absence.
func (client *Executions) WalkSchedules(ctx context.Context, correlation fault.Correlation, options sdk.ScheduleListOptions, visit func(context.Context, *sdk.ScheduleListEntry) error) error {
	if visit == nil {
		return failure(ErrInput, "schedule-visitor")
	}
	_, err := executeNative(ctx, client, correlation, Execution{Operation: "schedule.list"}, func(work context.Context, evidence *Execution) (struct{}, error) {
		page := &schedulePage{}
		work = context.WithValue(work, schedulePageKey{}, page)
		iterator, err := client.owner.native.ScheduleClient().List(work, options)
		if err != nil {
			return struct{}{}, err
		}
		for {
			if err := work.Err(); err != nil {
				return struct{}{}, err
			}
			if !iterator.HasNext() {
				// v1.49.0 reports false for an empty page even with a continuation.
				// Keep its iterator/conversion, but require protocol exhaustion.
				if page.more {
					continue
				}
				break
			}
			entry, err := iterator.Next()
			if err != nil {
				return struct{}{}, err
			}
			if err := visit(work, entry); err != nil {
				return struct{}{}, err
			}
		}
		evidence.ResultObtained = true
		return struct{}{}, nil
	})
	return err
}

type callbackScheduleClient struct{ borrower *callbackClient }

type scheduleHandleView struct {
	private
	native    sdk.ScheduleHandle
	id        string
	gate      func(context.Context, string, func(context.Context, *Execution) error) error
	factory   *clientFactory
	authority *nativeCall
}

func (client *callbackClient) ScheduleClient() sdk.ScheduleClient {
	return &callbackScheduleClient{borrower: client}
}

func (client *callbackScheduleClient) GetHandle(ctx context.Context, id string) sdk.ScheduleHandle {
	return client.borrower.scheduleHandle(client.borrower.nativeClient.ScheduleClient().GetHandle(ctx, id))
}

func (client *callbackClient) scheduleHandle(native sdk.ScheduleHandle) sdk.ScheduleHandle {
	if native == nil {
		return nil
	}
	id := native.GetID()
	return &scheduleHandleView{native: native, id: id, gate: func(ctx context.Context, operation string, invoke func(context.Context, *Execution) error) error {
		_, err := callbackNative(ctx, client, "", Execution{Operation: "callback." + operation, ScheduleID: id},
			func(work context.Context, evidence *Execution) (struct{}, error) {
				return struct{}{}, invoke(work, evidence)
			})
		return err
	}}
}

func (client *callbackScheduleClient) Create(ctx context.Context, options sdk.ScheduleOptions) (sdk.ScheduleHandle, error) {
	return callbackNative(ctx, client.borrower, "", Execution{Operation: "callback.schedule.create", ScheduleID: options.ID},
		func(work context.Context, evidence *Execution) (sdk.ScheduleHandle, error) {
			native, err := client.borrower.nativeClient.ScheduleClient().Create(work, options)
			if native != nil {
				evidence.ScheduleID = native.GetID()
			}
			return client.borrower.scheduleHandle(native), err
		})
}

func (client *callbackScheduleClient) List(ctx context.Context, options sdk.ScheduleListOptions) (sdk.ScheduleListIterator, error) {
	return callbackNative(ctx, client.borrower, "", Execution{Operation: "callback.schedule.list"},
		func(context.Context, *Execution) (sdk.ScheduleListIterator, error) {
			native, err := client.borrower.nativeClient.ScheduleClient().List(client.borrower.iteratorContext(ctx), options)
			if err != nil {
				return nil, err
			}
			return newCallbackIterator(client.borrower, ctx, "schedule.list", "", native), nil
		})
}

func (view *scheduleHandleView) GetID() string { return view.id }

func (view *scheduleHandleView) Describe(ctx context.Context) (value *sdk.ScheduleDescription, err error) {
	err = view.gate(ctx, "schedule.describe", func(work context.Context, evidence *Execution) error {
		var err error
		value, err = view.native.Describe(work)
		evidence.ResultObtained = err == nil
		return err
	})
	return
}

func (view *scheduleHandleView) Delete(ctx context.Context) error {
	return view.gate(ctx, "schedule.delete", func(work context.Context, _ *Execution) error { return view.native.Delete(work) })
}

func (view *scheduleHandleView) Update(ctx context.Context, options sdk.ScheduleUpdateOptions) error {
	return view.gate(ctx, "schedule.update", func(work context.Context, _ *Execution) error { return view.native.Update(work, options) })
}

func (view *scheduleHandleView) Trigger(ctx context.Context, options sdk.ScheduleTriggerOptions) error {
	return view.gate(ctx, "schedule.trigger", func(work context.Context, _ *Execution) error { return view.native.Trigger(work, options) })
}

func (view *scheduleHandleView) Backfill(ctx context.Context, options sdk.ScheduleBackfillOptions) error {
	return view.gate(ctx, "schedule.backfill", func(work context.Context, _ *Execution) error { return view.native.Backfill(work, options) })
}

func (view *scheduleHandleView) Pause(ctx context.Context, options sdk.SchedulePauseOptions) error {
	return view.gate(ctx, "schedule.pause", func(work context.Context, _ *Execution) error { return view.native.Pause(work, options) })
}

func (view *scheduleHandleView) Unpause(ctx context.Context, options sdk.ScheduleUnpauseOptions) error {
	return view.gate(ctx, "schedule.unpause", func(work context.Context, _ *Execution) error { return view.native.Unpause(work, options) })
}
