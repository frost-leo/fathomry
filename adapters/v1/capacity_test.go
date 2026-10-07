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
	"context"
	"errors"
	"testing"
)

func TestConfiguredCapacityObservation(t *testing.T) {
	var invalid Runtime
	if _, err := invalid.Options(); !errors.Is(err, ErrHandle) {
		t.Fatal("zero runtime accepted", err)
	}
	var empty Inbox[int]
	if _, err := empty.Options(); !errors.Is(err, ErrHandle) {
		t.Fatal("zero inbox accepted", err)
	}
	runtime, err := New(context.Background(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	options, err := runtime.Options()
	if err != nil || options.MaxActive != 16 || options.MaxTasks != 64 {
		t.Fatal("unnormalized runtime ceilings", err)
	}
	options.MaxActive = 1
	if next, _ := runtime.Options(); next.MaxActive != 16 {
		t.Fatal("mutable capacity alias")
	}
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if next, err := runtime.Options(); err != nil || next.MaxActive != 16 {
		t.Fatal("shutdown hid immutable limits", err)
	}
	inbox, err := NewInbox[int](EvidenceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	inbox.Seal()
	if actual, err := inbox.Options(); err != nil || actual.Capacity != 64 || actual.MaxBytes != 16<<20 {
		t.Fatal("sealed capacity changed", err)
	}
}
