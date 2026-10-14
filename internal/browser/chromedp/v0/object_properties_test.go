/*
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

package chromedp

import (
	"errors"
	"testing"

	"github.com/chromedp/cdproto/runtime"
	"github.com/frost-leo/fathomry/internal/fault"
)

func TestPropertyReferencesCountAllNativeKindsBeforeRetention(t *testing.T) {
	value := runtime.GetPropertiesResult{
		Result:             []*runtime.PropertyDescriptor{{Value: &runtime.RemoteObject{}, Get: &runtime.RemoteObject{}, Set: &runtime.RemoteObject{}, Symbol: &runtime.RemoteObject{}}},
		InternalProperties: []*runtime.InternalPropertyDescriptor{{Value: &runtime.RemoteObject{}}},
		PrivateProperties:  []*runtime.PrivatePropertyDescriptor{{Value: &runtime.RemoteObject{}, Get: &runtime.RemoteObject{}, Set: &runtime.RemoteObject{}}},
		ExceptionDetails:   &runtime.ExceptionDetails{Exception: &runtime.RemoteObject{}},
	}
	if references, err := propertyObjects(value, 9); err != nil || len(references) != 9 {
		t.Fatal("native reference kind omitted", err)
	}
	if _, err := propertyObjects(value, 8); !errors.Is(err, ErrLimit) {
		t.Fatal("reference capacity was exceeded", err)
	}
	for _, malformed := range []runtime.GetPropertiesResult{
		{Result: []*runtime.PropertyDescriptor{nil}},
		{InternalProperties: []*runtime.InternalPropertyDescriptor{nil}},
		{PrivateProperties: []*runtime.PrivatePropertyDescriptor{nil}},
	} {
		if _, err := propertyObjects(malformed, 9); !errors.Is(err, ErrNative) {
			t.Fatal("malformed native descriptor accepted", err)
		}
	}
}

func TestBrowserObjectPropertiesCapacityAndCleanup(t *testing.T) {
	options := browserOptions(t)
	options.MaxObjects = 3
	fixture := bindFixture(t, options, 1)
	id := fault.Correlation{Call: "property-capacity"}
	retained, err := fixture.client.Retain(deadline(t), id)
	if err != nil {
		t.Fatal(err)
	}
	group, err := retained.Page().NewObjectGroup(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	object, err := RunObjectAction(deadline(t), retained.Page(), group, Evaluate[*runtime.RemoteObject]("({first:{}, second:{}, third:{}})"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := object.Properties(deadline(t), runtime.GetPropertiesParams{OwnProperties: new(true)}); !errors.Is(err, ErrLimit) {
		t.Fatal("oversized property result was accepted", err)
	}
	if group.state.unknown || group.state.session.browserContext.objectCount != 1 {
		t.Fatal("terminal grouped result confused with unknown producer or leaked slots")
	}
	if _, err := object.Properties(deadline(t), runtime.GetPropertiesParams{}); !errors.Is(err, ErrState) {
		t.Fatal("overflowed group admitted another producer", err)
	}
	if err := group.Close(deadline(t)); err != nil || !group.Closed() {
		t.Fatal("grouped overflow custody did not clean up", err)
	}
	_ = retained.Close(deadline(t))
	settle(t, fixture, id, retained.Receipt(), ErrLimit, nil, func(testing.TB, Result) {})
}
