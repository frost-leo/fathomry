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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/target"
)

func TestUnknownBorrowedChildNeverBecomesReleased(t *testing.T) {
	record := &browserContext{borrowed: true, unknownTargets: 1}
	own := &owner{sessions: map[*browserContext]struct{}{record: {}}, settings: defaults(inertOptions())}
	for range 2 {
		released, err := own.disposeWith(context.Background(), record, nil)
		if released || !errors.Is(err, ErrCleanup) || len(own.sessions) != 1 {
			t.Fatal("unknown target was guessed or refunded", released, err)
		}
	}
	own.targetCreated(record, target.ID("known"))
	if record.unknownTargets != 0 || len(record.ownedTargets) != 1 {
		t.Fatal("known target did not retain exact cleanup identity")
	}
}

func TestOwnedStartupRejectsForeignDebuggerEndpoint(t *testing.T) {
	host := bindFixture(t, browserOptions(t), 1)
	if err := host.client.owner.start(deadline(t)); err != nil {
		t.Fatal(err)
	}
	port, err := os.ReadFile(filepath.Join(host.client.owner.profileDir, "DevToolsActivePort"))
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "wrong-peer")
	source := "#!/bin/sh\nfor argument in \"$@\"; do\ncase \"$argument\" in --user-data-dir=*) directory=\"${argument#--user-data-dir=}\";; esac\ndone\nprintf '%s' '" + strings.ReplaceAll(string(port), "'", "'\\''") + "' > \"$directory/DevToolsActivePort\"\nexec /bin/sleep 30\n"
	if err := os.WriteFile(script, []byte(source), 0700); err != nil {
		t.Fatal(err)
	}
	options := inertOptions()
	options.ExecPath = script
	options.TempDir = t.TempDir()
	own := bindFixture(t, options, 1)
	if err := own.client.owner.start(deadline(t)); !errors.Is(err, ErrState) {
		t.Fatal("foreign browser accepted as owned", err)
	}
	if err := own.assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if result, err := cdp.Call(deadline(t), browserExecutor(t, host), browser.GetVersion, cdp.Empty{}); err != nil || result.Product == "" {
		t.Fatal("independent peer stopped", err)
	}
}
