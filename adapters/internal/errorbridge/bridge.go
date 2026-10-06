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

package errorbridge

import (
	"errors"
	"slices"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/internal/fault"
)

// Classified reports only a valid direct public occurrence or a bridge-owned
// translated aggregate. Providers must check this before recursively unwrapping
// errors; retained original history is not another classification input.
func Classified(err error) bool {
	if value, ok := err.(*redactedCause); ok {
		return value != nil
	}
	_, ok := failure.Inspect(err)
	return ok
}

// Inspect visits at most limit nodes in cause order. It returns deduplicated
// provider classifications and, only for an entirely public graph behind neutral
// wrappers, a redacted forwarding representation. Recognized native boundaries
// retain their own meaning; public subtrees are already classified and opaque.
// classify must be pure and map a fixed finite set of kinds to stable codes.
// The original graph stays caller-owned and must remain immutable. Truncation
// never authorizes discarding it; callers retain it in their provider fallback.
func Inspect(original error, limit int, classify func(fault.Kind) failure.Code) ([]failure.Code, error) {
	var codes []failure.Code
	remaining := limit
	var visit func(error) (error, bool)
	visit = func(err error) (error, bool) {
		if err == nil {
			return nil, true
		}
		if remaining <= 0 {
			return nil, false
		}
		remaining--
		if Classified(err) {
			return err, true
		}
		var kind fault.Kind
		switch value := err.(type) {
		case *fault.Error:
			kind = value.Diagnostic().Kind
		case fault.Kind:
			kind = value
		}
		var code failure.Code
		if classify != nil {
			code = classify(kind)
		}
		if code != 0 && !slices.Contains(codes, code) {
			codes = append(codes, code)
		}

		switch value := err.(type) {
		case interface{ Unwrap() []error }:
			children := value.Unwrap()
			allPublic := code == 0 && len(children) > 0
			public := make([]error, 0, min(len(children), remaining))
			for _, child := range children {
				if remaining <= 0 {
					allPublic = false
					break
				}
				translated, ok := visit(child)
				allPublic = allPublic && ok
				if translated != nil {
					public = append(public, translated)
				}
			}
			if allPublic && len(public) > 0 {
				return Forward(err, join(public)), true
			}
		case interface{ Unwrap() error }:
			child := value.Unwrap()
			if child != nil {
				translated, ok := visit(child)
				if code == 0 && ok && translated != nil {
					return Forward(err, translated), true
				}
			}
		}
		return nil, false
	}
	forwarded, public := visit(original)
	if !public {
		forwarded = nil
	}
	return codes, forwarded
}

// Forward retains original wrapper/cause identity while exposing only translated
// public presentation. A single public occurrence retains its exact core; a heterogeneous
// aggregate never acquires one arbitrary owner. translated must already be public, safe,
// bounded and immutable; arbitrary original Error methods are never invoked.
func Forward(original, translated error) error {
	if translated == nil {
		return nil
	}
	value := &redactedCause{translated: translated, original: original}
	if core, ok := failure.Inspect(translated); ok {
		return &forwardedOccurrence{redactedCause: value, core: core}
	}
	return value
}
func join(values []error) error {
	if len(values) == 1 {
		return values[0]
	}
	return errors.Join(values...)
}

type redactedCause struct{ translated, original error }

func (value *redactedCause) Error() string   { return value.translated.Error() }
func (value *redactedCause) Unwrap() []error { return []error{value.translated, value.original} }

type forwardedOccurrence struct {
	*redactedCause
	core *failure.Error
}

func (value *forwardedOccurrence) Failure() *failure.Error { return value.core }
func (value *forwardedOccurrence) Error() string           { return value.core.Error() }
func (value *forwardedOccurrence) Is(target error) bool    { return value.core.Is(target) }
