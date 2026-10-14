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

	"github.com/chromedp/cdproto/runtime"
	sdk "github.com/chromedp/chromedp"
)

// RunValueAction retains the same native helper and decoder as the ordinary
// action, while assigning exception/late-result custody to an admitted group.
func RunValueAction[T any](ctx context.Context, session *Session, group *ObjectGroup, action Action[T]) (value T, custody *Object, err error) {
	if action.managed == nil || group == nil || group.state == nil || session == nil || session.sessionState == nil || group.state.session.sessionState != session.sessionState {
		return value, nil, failure(ErrInput, "value-custody")
	}
	work, leave, err := session.enter(ctx, false)
	if err != nil {
		return value, nil, err
	}
	defer leave()
	return runValueAction(work, session, group, action)
}

func runValueAction[T any](ctx context.Context, session *Session, group *ObjectGroup, action Action[T]) (value T, custody *Object, err error) {
	if action.bytes > session.owner.settings.MaxCommandBytes {
		return value, nil, failure(ErrLimit, "action-input")
	}
	custody, err = group.executeQuery(ctx, action.query, func(name string, options []sdk.QueryOption) sdk.Action[*runtime.RemoteObject] {
		operation := action.managed(name, options)
		return func(ctx context.Context, target *sdk.Target) (*runtime.RemoteObject, error) {
			var err error
			value, err = operation(ctx, target)
			return nil, err
		}
	}, action.objectGrouped)
	return value, custody, err
}

func runManagedValue[T any](ctx context.Context, session *Session, action Action[T]) (value T, err error) {
	work, leave, err := session.enter(ctx, false)
	if err != nil {
		session.remember(err)
		return value, err
	}
	defer leave()
	return runManagedValueAdmitted(work, session, action)
}

func runManagedValueAdmitted[T any](ctx context.Context, session *Session, action Action[T]) (value T, err error) {
	group, err := session.newObjectGroup(session.work)
	if err != nil {
		session.remember(err)
		return value, err
	}
	defer func() {
		if group.Settled() {
			cleanup := group.Close(context.Background())
			session.rememberCleanup(cleanup)
			err = errors.Join(err, cleanup)
		}
		session.remember(err)
	}()
	value, _, err = runValueAction(ctx, session, group, action)
	return value, err
}

// Settled means no native producer/result custody remains unknown. It does not
// mean that cleanup or cooperative Go decoder work has completed.
func (group *ObjectGroup) Settled() bool {
	if group == nil || group.state == nil {
		return false
	}
	group.state.mu.Lock()
	defer group.state.mu.Unlock()
	return !group.state.unknown
}
