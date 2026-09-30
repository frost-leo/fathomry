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

package adapters

import (
	"strings"
	"unicode/utf8"
)

// Options bounds one process-local operation runtime. Zero active/byte/tree/guard
// limits use defaults; MaxQueued zero deliberately disables queueing.
type Options struct {
	Name           string `json:"name"`
	MaxActive      int    `json:"max_active"`
	MaxQueued      int    `json:"max_queued"`
	MaxWorkBytes   int64  `json:"max_work_bytes"`
	MaxQueuedBytes int64  `json:"max_queued_bytes"`
	MaxTasks       int    `json:"max_tasks"`
	MaxDepth       int    `json:"max_depth"`
	MaxHolds       int    `json:"max_holds"`
}

// EvidenceOptions bounds reserved, queued and claimed records together. Zero
// selects 64 records and 16 MiB of declared evidence, not measured heap/RSS.
type EvidenceOptions struct {
	Capacity int   `json:"capacity"`
	MaxBytes int64 `json:"max_bytes"`
}

// Declaration binds a component's facts and evidence receiver once. Copy must
// isolate mutable data, never mutate its argument, and be bounded, concurrent-safe
// and non-panicking. It runs outside runtime locks. No reflected copy is inferred.
type Declaration[T any] struct {
	Copy     func(T) T
	Evidence *Inbox[T]
	Observer *Observer
}

// Request declares one operation. Operation is a non-secret dotted lowercase
// label; ID is optional opaque correlation, never diagnostic text. WorkBytes is
// the root family's declared envelope; children share it and specify zero.
// EvidenceBytes is a positive component-declared reservation for this result.
type Request struct {
	Operation     string
	ID            string
	WorkBytes     int64
	EvidenceBytes int64
}

// Outcome supplies a component-final technical result, not a business disposition.
// Present distinguishes a legitimate zero/empty value from absent data. Primary
// and Cleanup retain exact error objects; their graphs must remain immutable.
// Resolve once all outcome-relevant facts are known; it does not release work.
type Outcome[T any] struct {
	Value   T
	Present bool
	Primary error
	Cleanup error
}

func normalize(options Options) (Options, error) {
	if options.Name != "" && !label(options.Name, 64, false) {
		return Options{}, failureOf(ErrOptions, "new", "", Details{})
	}
	if options.MaxActive == 0 {
		options.MaxActive = 16
	}
	if options.MaxWorkBytes == 0 {
		options.MaxWorkBytes = 64 << 20
	}
	if options.MaxQueuedBytes == 0 && options.MaxQueued > 0 {
		options.MaxQueuedBytes = options.MaxWorkBytes
	}
	if options.MaxTasks == 0 {
		options.MaxTasks = 64
	}
	if options.MaxDepth == 0 {
		options.MaxDepth = 8
	}
	if options.MaxHolds == 0 {
		options.MaxHolds = 64
	}
	if options.MaxActive < 1 || options.MaxActive > 1024 || options.MaxQueued < 0 || options.MaxQueued > 4096 ||
		options.MaxWorkBytes < 1 || options.MaxWorkBytes > 1<<40 || options.MaxQueuedBytes < 0 || options.MaxQueuedBytes > 1<<40 ||
		options.MaxQueued == 0 && options.MaxQueuedBytes != 0 || options.MaxTasks < 1 || options.MaxTasks > 4096 ||
		options.MaxDepth < 1 || options.MaxDepth > 32 || options.MaxHolds < 1 || options.MaxHolds > 65536 {
		return Options{}, failureOf(ErrOptions, "new", "", Details{})
	}
	options.Name = strings.Clone(options.Name)
	return options, nil
}
func label(value string, maximum int, dotted bool) bool {
	if value == "" || len(value) > maximum {
		return false
	}
	if dotted {
		for _, part := range strings.Split(value, ".") {
			if part == "" || part[0] < 'a' || part[0] > 'z' {
				return false
			}
			for _, char := range part {
				if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '_' || char == '-') {
					return false
				}
			}
		}
		return true
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-' || char == '.') {
			return false
		}
	}
	return true
}
func validRequest(request Request, child bool) bool {
	if !label(request.Operation, 64, true) || len(request.ID) > 256 || !utf8.ValidString(request.ID) ||
		request.EvidenceBytes < 1 || request.EvidenceBytes > 1<<40 {
		return false
	}
	for _, char := range request.ID {
		if char < 0x20 || char == 0x7f {
			return false
		}
	}
	if child {
		return request.WorkBytes == 0
	}
	return request.WorkBytes > 0 && request.WorkBytes <= 1<<40
}
