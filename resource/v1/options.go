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
	"strings"
	"time"

	"github.com/frost-leo/fathomry/settings/v1"
)

// Policy chooses whether a binding adopts later selected configuration.
type Policy uint8

const (
	// Fixed captures the first accepted selection and ignores later configuration.
	Fixed Policy = 0
	// Follow reconstructs when the binding's selected configuration changes.
	Follow Policy = 1
)

// Options bounds one runtime scope. Zero limits select the documented defaults.
// Name is an optional non-secret label, not a process-global identity.
type Options struct {
	Name           string        `json:"name"`
	MaxBindings    int           `json:"max_bindings"`
	MaxPreparing   int           `json:"max_preparing"`
	CleanupTimeout time.Duration `json:"cleanup_timeout_ns"`
}

// Binding declares one typed component. Its functions are retained by the scope.
// Select, Clone and Equal must be bounded, synchronous, concurrency-safe and
// non-panicking. Select performs pure selection/validation from one captured view.
// Clone must isolate mutable configuration and never mutate its input. Equal
// borrows immutable configurations; it must not mutate them. Follow requires Equal.
//
// Build runs outside state locks with an owned generation lifetime context.
// Successful return does not cancel that context. Use a child context for a
// construction-phase budget; do not confuse a caller's waiting budget with the
// native instance lifetime. Build must not panic or hide acquired resources on
// failure. It receives a separate configuration copy, not the retained selection.
// Callbacks must not wait for lifecycle progress that requires their own return.
// Selection callbacks cannot re-enter Apply/Retry; Build cannot wait for another
// constructor needing its occupied slot or for the same scope's shutdown.
//
// MaxGenerations defaults to 2, accepts 2..16, and includes outstanding construction
// and incomplete cleanup. MaxBorrowers defaults to 1024, accepts 1..65536 per
// binding. Limits bound owned counts, not native memory or service-wide quotas.
type Binding[C, T any] struct {
	Name           string
	Policy         Policy
	Select         func(settings.View) (C, error)
	Clone          func(C) C
	Equal          func(C, C) bool
	Build          func(context.Context, C) (*Instance[T], error)
	MaxGenerations int
	MaxBorrowers   int
}

// Instance transfers ownership from Build. A non-nil result is owned even when
// Build also returns an error. Nil with no error is an invalid construction.
// Value can be any Go type, including a legitimate zero/nil value. Its concurrent
// use contract belongs to the component. Do not mutate the result record after
// returning it. Nil Release declares that no external cleanup is required.
type Instance[T any] struct {
	Value   T
	Release func(context.Context) ReleaseResult
}

// ReleaseResult separates observed cleanup completion from an error. Complete
// false retains ownership and permits a later serialized Release attempt. Complete
// true ends ownership, even with Err. Release must accurately report this boundary,
// be synchronous/non-panicking, and support continuation when returning false.
// Its fresh context has the scope's cleanup budget, not a canceled build context.
// Release must not wait for Scope.Close, which is joining this callback.
type ReleaseResult struct {
	Complete bool
	Err      error
}

func normalize(options Options) (Options, error) {
	if options.Name != "" && !validName(options.Name) {
		return Options{}, fail(ErrOptions, "new", "", Details{})
	}
	if options.MaxBindings == 0 {
		options.MaxBindings = 64
	}
	if options.MaxPreparing == 0 {
		options.MaxPreparing = 4
	}
	if options.CleanupTimeout == 0 {
		options.CleanupTimeout = 5 * time.Second
	}
	if options.MaxBindings < 1 || options.MaxBindings > 256 ||
		options.MaxPreparing < 1 || options.MaxPreparing > 64 ||
		options.CleanupTimeout < time.Millisecond || options.CleanupTimeout > time.Minute {
		return Options{}, fail(ErrOptions, "new", "", Details{})
	}
	options.Name = strings.Clone(options.Name)
	return options, nil
}

func validName(name string) bool {
	if name == "" || len(name) > 64 {
		return false
	}
	for _, char := range name {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' ||
			char >= '0' && char <= '9' || char == '.' || char == '-' || char == '_') {
			return false
		}
	}
	return true
}
