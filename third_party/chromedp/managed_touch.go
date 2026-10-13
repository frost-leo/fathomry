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
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/input"
)

// TouchSwipeXY moves one touch contact between viewport coordinates in CSS pixels.
// It sends 20 paced moves during duration, then releases the contact. Duration
// must be positive. Native dispatch can extend the elapsed time.
// The page decides how the touch scrolls or handles pointer events. This does not
// retry a previous gesture or promise an exact scroll distance after inertia.
func TouchSwipeXY(fromX, fromY, toX, toY float64, duration time.Duration) Action[Void] {
	return Func(func(ctx context.Context, target *Target) (err error) {
		const steps = 20
		if duration <= 0 {
			return Error("touch swipe duration must be positive")
		}
		for _, coordinate := range [...]float64{fromX, fromY, toX, toY} {
			if math.IsNaN(coordinate) || math.IsInf(coordinate, 0) {
				return Error("touch swipe coordinates must be finite")
			}
		}
		point := &input.TouchPoint{X: fromX, Y: fromY, ID: ptr(0.0), RadiusX: ptr(1.0), RadiusY: ptr(1.0), Force: ptr(1.0)}
		params := input.DispatchTouchEventParams{Type: input.DispatchTouchEventTypeTouchStart, TouchPoints: []*input.TouchPoint{point}}
		pressed := false
		defer func() {
			if pressed {
				params.Type = input.DispatchTouchEventTypeTouchCancel
				params.TouchPoints = []*input.TouchPoint{}
				err = errors.Join(err, reportHelperCleanup(ctx, helperCleanup(ctx, target, input.DispatchTouchEvent, params)))
			}
		}()
		work := withQueued(ctx, func(string) { pressed = true })
		if _, err := cdp.Call(work, target, input.DispatchTouchEvent, params); err != nil {
			return err
		}
		params.Type = input.DispatchTouchEventTypeTouchMove
		for step := 1; step <= steps; step++ {
			timer := time.NewTimer(max(duration/steps, time.Nanosecond))
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			fraction := float64(step) / steps
			point.X = fromX + (toX-fromX)*fraction
			point.Y = fromY + (toY-fromY)*fraction
			if _, err := cdp.Call(ctx, target, input.DispatchTouchEvent, params); err != nil {
				return err
			}
		}
		params.Type = input.DispatchTouchEventTypeTouchEnd
		params.TouchPoints = []*input.TouchPoint{}
		err = helperReleaseInput(ctx, target, input.DispatchTouchEvent, params)
		pressed = false
		return err
	})
}
