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

// Package orders demonstrates another independent semantic/detail owner.
// Its retry hint is an observation, never a universal safe-retry decision.
package orders

import (
	"time"

	"github.com/frost-leo/fathomry/failure"
)

const Throttled failure.Code = "example.orders.throttled"

type occurrence = failure.Error

// Limit owns an optional validated server delay. Zero is an invalid occurrence.
type Limit struct {
	occurrence
	delay   time.Duration
	present bool
	valid   bool
}

// NewLimit accepts a server hint in [0, 1 hour]. An absent hint must have zero delay.
// Invalid input preserves Throttled and reports unavailable detail separately.
// Valid present zero is distinct from absent. Effects and retry policy stay outside.
func NewLimit(delay time.Duration, present bool) Limit {
	result := Limit{occurrence: failure.New(Throttled, nil)}
	if delay < 0 || delay > time.Hour || !present && delay != 0 {
		return result
	}
	result.delay, result.present, result.valid = delay, present, true
	return result
}

// RetryAfter returns a delay, its presence, and whether the detail is valid.
// Invalid detail must not be interpreted as a known absent hint or permission to retry.
func (err Limit) RetryAfter() (delay time.Duration, present, valid bool) {
	return err.delay, err.present, err.valid
}
