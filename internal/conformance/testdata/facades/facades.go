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

// Package facades supplies layouts whose public selectors are exercised from a
// separate package using ordinary Go, not reflection or unsafe.
package facades

type callbacks struct{ Shutdown func() }
type otherCallbacks struct{ Shutdown func() }

type Safe struct {
	state callbacks
	close func()
}

func (Safe) Send() {}

type Direct struct{ Shutdown func() }

func (Direct) Send() {}

type State struct{ Count int }

func (State) Send() {}

type Promoted struct{ callbacks }

func (Promoted) Send() {}

func NewPromoted(close func()) Promoted { return Promoted{callbacks{close}} }

type Pointer struct{ *callbacks }

func (Pointer) Send() {}

func NewPointer(close func()) Pointer { return Pointer{&callbacks{close}} }

type middle struct{ *callbacks }
type Multi struct{ middle }

func (Multi) Send() {}

func NewMulti(close func()) Multi { return Multi{middle{&callbacks{close}}} }

type Ambiguous struct {
	callbacks
	otherCallbacks
}

func (Ambiguous) Send() {}

type left struct{ callbacks }
type right struct{ callbacks }
type Diamond struct {
	left
	right
}

func (Diamond) Send() {}

type ShadowedField struct {
	callbacks
	Shutdown int
}

func (ShadowedField) Send() {}

// Hiding an intermediate private field does not hide its promoted descendants.
type HiddenPath struct {
	middle
	callbacks int
}

func (HiddenPath) Send() {}

func NewHiddenPath(close func()) HiddenPath { return HiddenPath{middle: middle{&callbacks{close}}} }

type recursive struct{ *recursive }
type RecursiveSafe struct{ *recursive }

func (RecursiveSafe) Send() {}

type recursiveCallbacks struct {
	*recursiveCallbacks
	Shutdown func()
}

type Recursive struct{ *recursiveCallbacks }

func (Recursive) Send() {}

func NewRecursive(close func()) Recursive {
	inner := &recursiveCallbacks{Shutdown: close}
	inner.recursiveCallbacks = inner
	return Recursive{inner}
}

type mutualFirst struct{ *mutualSecond }
type mutualSecond struct{ *mutualFirst }
type MutualSafe struct{ *mutualFirst }

func (MutualSafe) Send() {}

type sender struct{}

func (sender) Send() {}

type PromotedMethod struct{ sender }

type pointerSender struct{}

func (*pointerSender) Send() {}

type PointerMethod struct{ pointerSender }
type NilMethod struct{ *pointerSender }

type owner struct{}

func (owner) Shutdown() {}

type OwningMethod struct{ owner }

func (OwningMethod) Send() {}

type PointerOwner struct{}

func (PointerOwner) Send()      {}
func (*PointerOwner) Shutdown() {}

type MethodShadow struct{ callbacks }

func (MethodShadow) Send()     {}
func (MethodShadow) Shutdown() {}

func NewMethodShadow(close func()) MethodShadow { return MethodShadow{callbacks{close}} }

type MethodFieldCollision struct {
	callbacks
	owner
}

func (MethodFieldCollision) Send() {}

type Map map[string]func()
type Slice []func()
type Array [1]func()
type Function func()
type Channel chan func()
type Scalar string

func (Map) Send()      {}
func (Slice) Send()    {}
func (Array) Send()    {}
func (Function) Send() {}
func (Channel) Send()  {}
func (Scalar) Send()   {}
