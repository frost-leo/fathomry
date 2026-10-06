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

package redis

import (
	"context"
	"errors"
	"strconv"
	"sync"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/cache/redis/v9"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

type family struct {
	gate  sync.Mutex
	inbox *invocation.Inbox[native.Result]
	id    uint64
}

func (op *operation) correlation() fault.Correlation {
	snapshot, _ := op.call.Receipt().Snapshot()
	return fault.Correlation{Call: "redis-" + strconv.FormatUint(op.family.id, 10) + "-" + strconv.FormatUint(snapshot.Info().Sequence, 10), Parent: op.parent}
}

// claimRoot runs before any callback child. Native Begin has already enqueued
// the root, so children cannot consume its lifecycle record by mistake.
func (op *operation) claimRoot() error {
	if op.record != nil {
		actual, _ := op.record.Receipt().Result()
		if actual.Context.Correlation != op.correlation() {
			return fail(ErrState, "evidence-correlation")
		}
		return nil
	}
	record, err := op.family.inbox.Next(context.Background())
	if err != nil {
		return err
	}
	op.record = record
	actual, _ := record.Receipt().Result()
	if actual.Context.Correlation != op.correlation() {
		return fail(ErrState, "evidence-correlation")
	}
	return nil
}
func (op *operation) quarantine(err error) bool {
	op.kept.Store(true)
	if !op.held {
		op.guard, _ = op.call.Hold()
		op.held = true
	}
	_ = op.call.Resolve(adapters.Outcome[Result]{Primary: translate(err, "evidence-custody", op.capability)})
	op.state.mu.Lock()
	defer op.state.mu.Unlock()
	if op.state.custody == nil {
		op.state.custody = make(map[*operation]error)
		close(op.state.custodyFailed)
	}
	op.state.custody[op] = err
	return false
}

func (op *operation) finish(receipt *invocation.Receipt[native.Result], setup error) bool {
	if receipt == nil {
		if setup == nil {
			setup = problem(op.capability, ErrState, "missing-receipt")
		}
		_ = op.call.Resolve(adapters.Outcome[Result]{Primary: translate(setup, "operation", op.capability)})
		return true
	}
	if err := op.claimRoot(); err != nil {
		return op.quarantine(err)
	}
	expected, _ := receipt.Result()
	if expected.Context.Correlation != op.correlation() {
		return op.quarantine(fail(ErrState, "evidence-correlation"))
	}
	value, err := receipt.WaitReleased(context.Background())
	if err != nil {
		return op.quarantine(err)
	}
	info, _ := op.call.Receipt().Snapshot()
	result := project(value, info.Info(), op.capability, op.kind, op.commands)
	result.primary = combine(translate(setup, "operation", op.capability), result.primary)
	if err := op.call.Resolve(adapters.Outcome[Result]{Present: true, Value: result, Primary: result.primary, Cleanup: result.cleanup}); err != nil {
		return op.quarantine(err)
	}
	if err := op.record.Release(); err != nil {
		return op.quarantine(err)
	}
	return true
}

// A callback's Goexit unwinds native cleanup without assigning its return values.
// The root claimed before entering that callback still owns the completed facts.
func (op *operation) finishCallback(receipt *invocation.Receipt[native.Result], setup error) {
	if receipt == nil && op.record != nil {
		receipt = op.record.Receipt()
	}
	op.finish(receipt, setup)
}

func (op *operation) child(ctx context.Context, name string, capability Capability, kind ResultKind, commands []Command, work func(context.Context, fault.Correlation) (*invocation.Receipt[native.Result], error)) (Result, error) {
	if ctx == nil || !capability.valid() || len(commands) > op.state.metadata.MaxCommands {
		return Result{}, problem(capability, ErrInput, name)
	}
	for _, command := range commands {
		if !command.valid {
			return Result{}, problem(capability, ErrInput, name)
		}
	}
	commands = append([]Command(nil), commands...)
	receipt, err := op.endpoint.Child(ctx, op.call.Scope(), request(capability, name, op.id, 0, op.state.policy.Budget.EvidenceBytes), func(call *adapters.Call[Result]) {
		live, stop := joinContext(call.Context(), op.lifetime)
		defer stop()
		child := &operation{family: op.family, state: op.state, call: call, capability: capability, kind: kind, commands: commands, parent: op.correlation().Call}
		nativeReceipt, err := work(live, child.correlation())
		child.finish(nativeReceipt, err)
	})
	return result(receipt, err)
}
func result(receipt *adapters.Receipt[Result], err error) (Result, error) {
	if receipt == nil {
		return Result{}, err
	}
	snapshot, _ := receipt.Snapshot()
	value, _ := snapshot.ValueCopy()
	return value, combine(err, snapshot.Primary(), snapshot.Cleanup())
}
func combine(causes ...error) error {
	var found []error
	for _, err := range causes {
		if err != nil {
			found = append(found, err)
		}
	}
	if len(found) == 1 {
		return found[0]
	}
	return errors.Join(found...)
}
