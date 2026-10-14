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
	"slices"

	"github.com/chromedp/cdproto/network"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// RunResponse observes navigation before running one to 64 closed actions in
// order. It returns the first main-frame navigation's final HTTP response, even
// for non-2xx status. Same-document navigation and back/forward cache restoration
// return a nil response because no new HTTP response is observed.
// With no navigation it waits for cancellation; use a caller deadline.
//
// The returned receipt describes the response wait. Each attempted step also
// retains its normal independent receipt, including managed-value cleanup.
// Reserve evidence for both the outer operation and its steps. Cancellation or
// refusal of a later step does not undo earlier effects. Steps are borrowed and
// must remain immutable until return. No action is retried.
func RunResponse(ctx context.Context, page *Page, steps ...Action[sdk.Void]) (*network.Response, *adapters.Receipt[Result], error) {
	if len(steps) == 0 || len(steps) > 64 {
		return nil, nil, fail(ErrInput, "response-steps")
	}
	for _, step := range steps {
		if step.name == "" || !step.native.Valid() {
			return nil, nil, fail(ErrInput, "response-step")
		}
	}
	return runPageOperation(ctx, page, "run_response", func(work context.Context) (*network.Response, error) {
		steps := slices.Clone(steps)
		return native.RunResponse(work, page.native, func(trigger context.Context) error {
			for _, step := range steps {
				var err error
				step.native, err = native.ResponseStep(trigger, page.native, step.native)
				if err != nil {
					return err
				}
				if _, _, err := RunAction(trigger, page, step); err != nil {
					return err
				}
			}
			return nil
		})
	})
}
