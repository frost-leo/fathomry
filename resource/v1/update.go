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

package resource

import (
	"context"
	"errors"
	"sync"
)

// Update is a historical receipt for one accepted Apply or Retry. It does not
// contain configuration values and does not promise that its targets remain current.
type Update struct {
	sequence uint64
	scope    string
	targets  []*target
}

type target struct {
	mu         sync.Mutex
	done       chan struct{}
	settled    bool
	name       string
	version    uint64
	generation uint64
	err        error
}

func newTarget(name string, version uint64) *target {
	return &target{done: make(chan struct{}), name: name, version: version}
}

func (target *target) settle(generation uint64, err error) {
	target.mu.Lock()
	defer target.mu.Unlock()
	if target.settled {
		return
	}
	target.generation, target.err, target.settled = generation, err, true
	close(target.done)
}

func (target *target) result() (bool, error) {
	target.mu.Lock()
	defer target.mu.Unlock()
	return target.settled, target.err
}

// Sequence is the scope-local accepted-input sequence, not a durable source revision.
func (update *Update) Sequence() uint64 {
	if update == nil {
		return 0
	}
	return update.sequence
}

// Wait waits for each captured target's construction/adoption outcome. Success is
// historical; later changes or scope closure do not rewrite an already successful
// receipt. Waiting cancellation never cancels construction or releases ownership.
// Selection is already complete; failed construction may leave earlier instances
// active and partially acquired candidates awaiting cleanup.
func (update *Update) Wait(ctx context.Context) error {
	if update == nil || update.sequence == 0 {
		return fail(ErrScope, "wait", "", Details{})
	}
	if ctx == nil {
		return fail(ErrOptions, "wait", "", Details{Scope: update.scope})
	}
	var causes []error
	for _, target := range update.targets {
		select {
		case <-target.done:
		default:
			select {
			case <-target.done:
			case <-ctx.Done():
				return fail(ErrWait, "wait", target.name, Details{Scope: update.scope, Target: target.version, Pending: true},
					errors.Join(causes...), ctx.Err(), context.Cause(ctx))
			}
		}
		_, err := target.result()
		if err != nil {
			causes = append(causes, err)
		}
	}
	if len(causes) == 1 {
		return causes[0]
	}
	if len(causes) != 0 {
		return fail(ErrBuild, "wait", "", Details{Scope: update.scope}, errors.Join(causes...))
	}
	return nil
}
