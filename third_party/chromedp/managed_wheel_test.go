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
	"context"
	"errors"
	"math"
	"testing"

	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/input"
)

func TestManagedWheelParametersKeepBothAxes(t *testing.T) {
	for _, deltas := range [][2]float64{{0, 350}, {220, 0}, {0, 0}, {-10, -20}} {
		mouse := input.DispatchMouseEventParams{Type: input.DispatchMouseEventTypeMouseWheel, X: 12, Y: 23, DeltaX: deltas[0], DeltaY: deltas[1]}
		touch := input.EmulateTouchFromMouseEventParams{Type: input.EmulateTouchFromMouseEventTypeMouseWheel, X: 12, Y: 23, Button: input.MouseButtonNone, DeltaX: deltas[0], DeltaY: deltas[1]}
		for _, command := range []struct {
			method string
			params any
		}{
			{input.DispatchMouseEvent.Method, mouse}, {input.DispatchMouseEvent.Method, &mouse},
			{input.EmulateTouchFromMouseEvent.Method, touch}, {input.EmulateTouchFromMouseEvent.Method, &touch},
		} {
			for _, limit := range []int64{0, 1024} {
				data, err := encodeCall(WithCallLimits(context.Background(), CallLimits{MaxInputBytes: limit}), command.method, command.params)
				if err != nil {
					t.Fatal(err)
				}
				var value struct {
					DeltaX *float64 `json:"deltaX"`
					DeltaY *float64 `json:"deltaY"`
					X      float64  `json:"x"`
					Y      float64  `json:"y"`
				}
				if err := jsonv2.Unmarshal(data, &value); err != nil {
					t.Fatal(err)
				}
				if value.DeltaX == nil || value.DeltaY == nil || *value.DeltaX != deltas[0] || *value.DeltaY != deltas[1] || value.X != 12 || value.Y != 23 {
					t.Fatalf("%s lost wheel axes: %s", command.method, data)
				}
			}
		}
		if mouse.DeltaX != deltas[0] || mouse.DeltaY != deltas[1] || touch.DeltaX != deltas[0] || touch.DeltaY != deltas[1] {
			t.Fatal("encoding changed caller parameters")
		}
	}
}

func TestManagedWheelEncodingKeepsAdmissionAndOtherEvents(t *testing.T) {
	params := input.DispatchMouseEventParams{Type: input.DispatchMouseEventTypeMouseWheel, DeltaY: math.NaN()}
	denied := errors.New("wheel admission refused")
	ctx := WithCallLimits(context.Background(), CallLimits{Before: func(string) error { return denied }})
	if _, err := encodeCall(ctx, input.DispatchMouseEvent.Method, params); !errors.Is(err, denied) {
		t.Fatal("encoding preceded admission", err)
	}
	params.DeltaY = 1
	if _, err := encodeCall(WithCallLimits(context.Background(), CallLimits{MaxInputBytes: 16}), input.DispatchMouseEvent.Method, params); !errors.Is(err, ErrCommandLimit) {
		t.Fatal("wheel encoding escaped its byte limit", err)
	}
	for _, command := range []struct {
		method string
		params any
	}{
		{input.DispatchMouseEvent.Method, input.DispatchMouseEventParams{Type: input.DispatchMouseEventTypeMouseMoved}},
		{input.EmulateTouchFromMouseEvent.Method, input.EmulateTouchFromMouseEventParams{Type: input.EmulateTouchFromMouseEventTypeMouseMoved, Button: input.MouseButtonNone}},
		{input.DispatchMouseEvent.Method, (*input.DispatchMouseEventParams)(nil)},
		{input.EmulateTouchFromMouseEvent.Method, (*input.EmulateTouchFromMouseEventParams)(nil)},
	} {
		want, err := jsonv2.Marshal(command.params, DefaultMarshalOptions)
		if err != nil {
			t.Fatal(err)
		}
		got, err := encodeCall(context.Background(), command.method, command.params)
		if err != nil || string(got) != string(want) {
			t.Fatal("non-wheel or nil parameters changed", string(got), err)
		}
	}
}
