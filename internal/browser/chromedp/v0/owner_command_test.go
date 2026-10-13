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
	"testing"

	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
)

func TestOwnerCommandPreflightDoesNotStartUnownedOrInvalidWork(t *testing.T) {
	for method := range ownerObservationCommands {
		if schema, ok := protocolCommands[method]; !ok || schema.params == "" || schema.result == "" {
			t.Fatalf("missing pinned owner command schema: %s", method)
		}
	}
	own := &owner{settings: defaults(inertOptions())}
	source := Source{owner: own}
	if _, err := RunOwnerCommand(context.Background(), source, browser.Close, cdp.Empty{}); !errors.Is(err, ErrUnsupported) || own.started {
		t.Fatal("unmanaged lifecycle command acquired native owner work", err)
	}
	if _, err := RunOwnerCommand(context.Background(), source, cdp.Command[cdp.Empty, map[string]any]{Method: browser.CommandGetVersion}, cdp.Empty{}); !errors.Is(err, ErrInput) || own.started {
		t.Fatal("forged native result type started a browser", err)
	}
	own.settings.RemoteURL = "ws://not-connected.invalid/devtools/browser/borrowed"
	if _, err := RunOwnerCommand(context.Background(), source, browser.GetVersion, cdp.Empty{}); !errors.Is(err, ErrState) || own.started {
		t.Fatal("borrowed connection became process ownership", err)
	}
}
