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

package kafka

import (
	"context"
	"errors"
	"strconv"
	"sync"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/broker/franz/v1"
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
	return fault.Correlation{Call: "kafka-" + strconv.FormatUint(op.family.id, 10) + "-" + strconv.FormatUint(snapshot.Info().Sequence, 10)}
}

func (family *family) take(receipt *invocation.Receipt[native.Result]) (*invocation.DeliveryRecord[native.Result], error) {
	record, err := family.inbox.Next(context.Background())
	if err != nil {
		return nil, err
	}
	expected, _ := receipt.Result()
	actual, _ := record.Receipt().Result()
	if expected.Context.Correlation != actual.Context.Correlation {
		return nil, fail(ErrState, "evidence-custody")
	}
	return record, nil
}

func (op *operation) transfer(record *invocation.DeliveryRecord[native.Result], value invocation.Result[native.Result], setup error) error {
	metadata, _ := op.call.Receipt().Snapshot()
	if err := op.call.Resolve(adapters.Outcome[Result]{Present: true, Value: project(value, metadata.Info()),
		Primary: translate(errors.Join(setup, value.Outcome.Primary), "operation"), Cleanup: translate(value.Outcome.Cleanup, "cleanup")}); err != nil {
		return err
	}
	return record.Release()
}

func (op *operation) finish(receipt *invocation.Receipt[native.Result], err error) {
	if receipt == nil {
		if err == nil {
			err = fail(ErrState, "missing-receipt")
		}
		_ = op.call.Resolve(adapters.Outcome[Result]{Primary: translate(err, "operation")})
		return
	}
	record, takeErr := op.family.take(receipt)
	if takeErr != nil {
		_ = op.call.Resolve(adapters.Outcome[Result]{Primary: takeErr})
		return
	}
	value, waitErr := receipt.WaitReleased(context.Background())
	if waitErr == nil {
		waitErr = op.transfer(record, value, err)
	}
	if waitErr != nil {
		_ = op.call.Resolve(adapters.Outcome[Result]{Primary: translate(waitErr, "evidence")})
	}
}

// keep claims the lifetime record before any children enter the private Inbox.
// The bounded join worker is not an SDK callback, exporter or forwarding queue.
func (op *operation) keep(receipt *invocation.Receipt[native.Result], setup error, cleanup func()) error {
	record, err := op.family.take(receipt)
	if err != nil {
		return err
	}
	op.kept = true
	go func() {
		value, err := receipt.WaitReleased(op.lifetime)
		if err != nil {
			if cleanup != nil {
				cleanup()
			}
			value, err = receipt.WaitReleased(context.Background())
		}
		if err == nil {
			err = op.transfer(record, value, setup)
		}
		if err != nil {
			_ = op.call.Resolve(adapters.Outcome[Result]{Primary: translate(err, "evidence")})
			return // An invariant failure cannot authorize discarding native custody.
		}
		op.stop()
		_ = op.guard.Release()
	}()
	return nil
}

func result(receipt *adapters.Receipt[Result], err error) (Result, error) {
	if receipt == nil {
		return Result{}, err
	}
	snapshot, _ := receipt.Snapshot()
	value, _ := snapshot.ValueCopy()
	return value, combineErrors(err, snapshot.Primary(), snapshot.Cleanup())
}

func waitResult(ctx context.Context, receipt *adapters.Receipt[Result]) (Result, error) {
	if ctx == nil || receipt == nil {
		return Result{}, fail(ErrInput, "wait")
	}
	snapshot, err := receipt.WaitReleased(ctx)
	value, _ := snapshot.ValueCopy()
	return value, combineErrors(err, snapshot.Primary(), snapshot.Cleanup())
}

func combineErrors(causes ...error) error {
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
