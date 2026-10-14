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

package chromedp_test

import (
	"context"
	"errors"
	"testing"
	"time"

	browser "github.com/frost-leo/fathomry/adapters/browser/chromedp/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func publicDependencies(t *testing.T, prepared browser.Prepared) browser.Dependencies {
	t.Helper()
	runtime, err := adapters.New(context.Background(), prepared.Policy().Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[browser.Result](prepared.Policy().Evidence)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return browser.Dependencies{Runtime: runtime, Evidence: inbox}
}
func TestPolicyAccountsForSourceAndRejectsUndersizedMechanisms(t *testing.T) {
	settings := browser.Settings{Name: "policy", ExecPath: "/explicitly-not-started", MaxSessions: new(1), MaxPages: new(2), MaxStreams: new(1), MaxSubscriptions: new(3), MaxBindings: new(2), MaxBindingCalls: new(3), MaxObjectGroups: new(2), QueuedCalls: 2}
	prepared, err := browser.Prepare(settings, browser.NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	policy := prepared.Policy()
	recommended, err := browser.Recommend(settings)
	if err != nil || recommended != policy {
		t.Fatal("recommendation and prepared policy diverged", err)
	}
	if policy.Runtime.MaxActive != 2 || policy.Runtime.MaxTasks != 21 || policy.Evidence.Capacity != 20 ||
		policy.Runtime.MaxWorkBytes != policy.Budget.SourceWorkBytes+policy.Budget.WorkBytes ||
		policy.Evidence.MaxBytes != policy.Budget.SourceEvidenceBytes+19*policy.Budget.EvidenceBytes {
		t.Fatal("source or child envelope missing")
	}
	for _, field := range []string{"active", "work", "tasks", "depth", "holds", "queued", "queued-bytes", "evidence-count", "evidence-bytes"} {
		t.Run(field, func(t *testing.T) {
			limits, evidence := policy.Runtime, policy.Evidence
			switch field {
			case "active":
				limits.MaxActive--
			case "work":
				limits.MaxWorkBytes--
			case "tasks":
				limits.MaxTasks--
			case "depth":
				limits.MaxDepth--
			case "holds":
				limits.MaxHolds--
			case "queued":
				limits.MaxQueued--
			case "queued-bytes":
				limits.MaxQueuedBytes--
			case "evidence-count":
				evidence.Capacity--
			case "evidence-bytes":
				evidence.MaxBytes--
			}
			runtime, err := adapters.New(context.Background(), limits)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close(context.Background())
			inbox, err := adapters.NewInbox[browser.Result](evidence)
			if err != nil {
				t.Fatal(err)
			}
			owner, err := prepared.Open(context.Background(), browser.Dependencies{Runtime: runtime, Evidence: inbox})
			if owner != nil {
				defer owner.Close(context.Background())
			}
			if owner != nil || !errors.Is(err, browser.ErrLimit) {
				t.Fatal("undersized policy acquired source", err)
			}
			stats, _ := runtime.Inspect()
			custody, _ := inbox.Inspect()
			if stats.Accepted != 0 || custody.Outstanding != 0 {
				t.Fatal("rejection retained work or evidence")
			}
		})
	}
}

func TestPolicyRejectsUnconstructibleCombinedEvidenceCapacity(t *testing.T) {
	settings := browser.Settings{Name: "policy", ExecPath: "/not-started", MaxSessions: new(128), MaxPages: new(128), MaxStreams: new(128), MaxSubscriptions: new(128), MaxBindings: new(128), MaxBindingCalls: new(128)}
	if _, err := browser.Prepare(settings, browser.NativeOptions{}); !errors.Is(err, browser.ErrLimit) {
		t.Fatal("unconstructible policy was accepted", err)
	}
	settings.MaxSessions = new(64)
	prepared, err := browser.Prepare(settings, browser.NativeOptions{})
	if err != nil {
		t.Fatal("constructible control was refused", err)
	}
	publicDependencies(t, prepared)
}
