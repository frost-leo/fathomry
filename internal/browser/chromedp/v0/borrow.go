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
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/target"
	sdk "github.com/chromedp/chromedp"
)

func (own *owner) targetAttempt(record *browserContext) {
	own.mu.Lock()
	defer own.mu.Unlock()
	if record.borrowed {
		record.unknownTargets++
	}
}
func (own *owner) targetCreated(record *browserContext, id target.ID) {
	own.mu.Lock()
	defer own.mu.Unlock()
	if !record.borrowed || id == "" {
		return
	}
	record.unknownTargets--
	if record.ownedTargets == nil {
		record.ownedTargets = make(map[target.ID]struct{})
	}
	record.ownedTargets[id] = struct{}{}
}
func (own *owner) disposeBorrowed(ctx context.Context, record *browserContext, native *sdk.Browser) (bool, error) {
	if record.unknownTargets != 0 {
		return false, failure(ErrCleanup, "unknown-target")
	}
	if record.detachUncertain {
		if own.connectionClosed {
			record.detachUncertain = false
		} else {
			return false, failure(ErrCleanup, "unconfirmed-detach")
		}
	}
	if len(record.ownedTargets) == 0 {
		own.forget(record)
		return true, nil
	}
	if err := own.closeBorrowedTargets(ctx, record, native); err != nil {
		return false, err
	}
	own.forget(record)
	return true, nil
}

func (own *owner) closeBorrowedTargets(ctx context.Context, record *browserContext, native *sdk.Browser) error {
	work := sdk.WithCallLimits(ctx, sdk.CallLimits{MaxInputBytes: own.settings.MaxCommandBytes, MaxOutputBytes: own.settings.MaxResultBytes})
	closing := make(map[target.ID]bool)
	for {
		targets, err := cdp.Call(work, native, target.GetTargets, target.GetTargetsParams{})
		if err != nil {
			return err
		}
		present := make(map[target.ID]bool)
		for _, info := range targets.TargetInfos {
			if _, owned := record.ownedTargets[info.TargetID]; !owned {
				continue
			}
			expected := record.browserID
			if record.defaultContext {
				if !record.defaultIdentityObserved {
					// This ID came from our explicit default-context creation,
					// never from a caller-supplied target string.
					record.observedID = info.BrowserContextID
					record.defaultIdentityObserved = true
				}
				expected = record.observedID
			}
			if info.BrowserContextID != expected {
				return failure(ErrCleanup, "target-origin-changed")
			}
			present[info.TargetID] = true
			if !closing[info.TargetID] {
				_, err := cdp.Call(work, native, target.CloseTarget, target.CloseTargetParams{TargetID: info.TargetID})
				if err != nil {
					return err
				}
				closing[info.TargetID] = true
			}
		}
		for id := range record.ownedTargets {
			if !present[id] {
				delete(record.ownedTargets, id)
			}
		}
		if len(record.ownedTargets) == 0 {
			return nil
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(ctx.Err(), context.Cause(ctx))
		case <-timer.C:
		}
	}
}

// The default context can have a nonempty TargetInfo identity. A temporary,
// explicitly default-scoped child supplies positive membership evidence. It is
// tracked before submission and closed before attachment to the borrowed page.
func (own *owner) identifyDefaultContext(ctx context.Context, record *browserContext, native *sdk.Browser) error {
	own.targetAttempt(record)
	created, err := cdp.Call(ctx, native, target.CreateTarget, target.CreateTargetParams{URL: "about:blank", NewWindow: new(true)})
	own.targetCreated(record, created.TargetID)
	if err != nil {
		return err
	}
	if created.TargetID == "" {
		return failure(ErrNative, "missing-default-probe-identity")
	}
	info, err := cdp.Call(ctx, native, target.GetTargetInfo, target.GetTargetInfoParams{TargetID: created.TargetID})
	if err != nil {
		return err
	}
	if info.TargetInfo == nil || info.TargetInfo.TargetID != created.TargetID {
		return failure(ErrNative, "default-probe-identity")
	}
	record.observedID = info.TargetInfo.BrowserContextID
	record.defaultIdentityObserved = true
	return own.closeBorrowedTargets(ctx, record, native)
}
