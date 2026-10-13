/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package chromedp

import (
	"context"
	"testing"
	"time"

	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/page"
	"github.com/frost-leo/fathomry/internal/fault"
)

func TestEventDecodePreservesUnknownProtocolCompatibility(t *testing.T) {
	value, err := decodeEvent(jsonv2.Value(`{"method":"Future.event","params":{}}`))
	if value != nil || err != nil {
		t.Fatal("unknown protocol event terminated compatibility", err)
	}
	for _, raw := range []string{`{"method":`, `{"method":"Page.screencastVisibilityChanged","params":[]}`} {
		if _, err := decodeEvent(jsonv2.Value(raw)); err == nil {
			t.Fatal("malformed event accepted")
		}
	}
	value, err = decodeEvent(jsonv2.Value(`{"method":"Page.screencastVisibilityChanged","params":{"visible":true}}`))
	if event, ok := value.(*page.EventScreencastVisibilityChanged); err != nil || !ok || !event.Visible {
		t.Fatal("known event lost its native type", err)
	}
}

func TestBrowserScreencastEventsRemainReachable(t *testing.T) {
	fixture := bindFixture(t, browserOptions(t), 1)
	id := fault.Correlation{Call: "screencast-events"}
	receipt, err := fixture.client.Run(deadline(t), deadline(t), id, func(session *Session) error {
		if err := session.Navigate(session.Context(), "data:text/html,<html><body>managed screencast</body></html>"); err != nil {
			return err
		}
		control, err := session.Control(session.Context(), ControlOptions{Screencast: &page.StartScreencastParams{}})
		if err != nil {
			return err
		}
		defer control.Close(context.Background())
		ctx, cancel := context.WithTimeout(session.Context(), 3*time.Second)
		defer cancel()
		for {
			event, err := control.Next(ctx)
			if err != nil {
				return err
			}
			if event.Method() != page.ScreencastFrame.Method {
				continue
			}
			var frame page.EventScreencastFrame
			if err := jsonv2.Unmarshal(event.DataCopy(), &frame); err != nil {
				return err
			}
			if len(frame.Data) == 0 {
				t.Error("empty screencast frame")
			}
			if err := event.Reply(session.Context(), ScreencastFrameAck()); err != nil {
				return err
			}
			return control.Close(session.Context())
		}
	})
	if err != nil {
		t.Fatal("allowed producer lost its event route", err)
	}
	settle(t, fixture, id, receipt, nil, nil, func(t testing.TB, result Result) {
		if !result.ContextReleased() {
			t.Fatal("screencast context cleanup was not confirmed")
		}
	})
}
