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
	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	sdk "github.com/chromedp/chromedp"
)

func (session *Session) startEvents() {
	if session.managedEvents {
		return
	}
	events, unsubscribe := sdk.FromContext(session.native).Target.SubscribeMany()
	session.eventsDone = make(chan struct{})
	stop := context.AfterFunc(session.work, unsubscribe)
	go func() {
		defer close(session.eventsDone)
		defer stop()
		defer unsubscribe()
		for {
			select {
			case <-session.work.Done():
				return
			case raw, ok := <-events:
				if !ok {
					return
				}
				event, err := decodeEvent(raw)
				if err != nil {
					session.remember(failure(ErrNative, "event-decode", err))
					session.cancel(err)
					return
				}
				session.observe(event)
			}
		}
	}()
}

func decodeEvent(raw jsonv2.Value) (any, error) {
	var message cdproto.Message
	if err := jsonv2.Unmarshal(raw, &message, sdk.DefaultUnmarshalOptions); err != nil {
		return nil, err
	}
	event, err := cdproto.UnmarshalMessage(&message, sdk.DefaultUnmarshalOptions)
	var unknown cdp.ErrUnknownCommandOrEvent
	if errors.As(err, &unknown) {
		return nil, nil
	}
	return event, err
}
