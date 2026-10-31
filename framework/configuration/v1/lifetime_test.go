/**
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

package configuration

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	viper "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestInitialDeadlineIncludesQueuedAdmission(t *testing.T) {
	runtime, err := adapters.New(context.Background(), adapters.Options{MaxActive: 1, MaxQueued: 1})
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := adapters.NewInbox[viper.Evidence](adapters.EvidenceOptions{Capacity: 4})
	if err != nil {
		t.Fatal(err)
	}
	blockerEvidence, err := adapters.NewInbox[int](adapters.EvidenceOptions{Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := adapters.Bind(runtime, adapters.Declaration[int]{Evidence: blockerEvidence, Copy: func(value int) int { return value }})
	if err != nil {
		t.Fatal(err)
	}
	var guard adapters.Guard
	_, err = endpoint.Run(context.Background(), adapters.Request{Operation: "fixture.blocker", WorkBytes: 1, EvidenceBytes: 1}, func(call *adapters.Call[int]) {
		var problem error
		guard, problem = call.Hold()
		if problem != nil {
			t.Error(problem)
		}
		if err := call.Resolve(adapters.Outcome[int]{Value: 1, Present: problem == nil, Primary: problem}); err != nil {
			t.Error(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = guard.Release()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
		delivery, err := blockerEvidence.NextReleased(ctx)
		if err != nil {
			t.Error(err)
		} else if err := delivery.Ack(); err != nil {
			t.Error(err)
		}
	})
	path := filepath.Join(t.TempDir(), "app.yaml")
	write(t, path, "workers: 1")
	selected := options()
	selected.InitialTimeout = 30 * time.Millisecond
	session, err := startSession(context.Background(), localPlan(path, false), schema(), selected, Dependencies{Local: viper.Dependencies{Runtime: runtime, Evidence: evidence}})
	if err != nil {
		t.Fatal(err)
	}
	defer closeSession(t, session, true)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := session.waitInitial(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("queued setup escaped initialization budget", err)
	}
	closeSession(t, session, true)
}
func TestBorrowedRuntimeTermination(t *testing.T) {
	dependencies, _ := localDependencies(t, 16)
	path := filepath.Join(t.TempDir(), "app.yaml")
	write(t, path, "workers: 2")
	session, err := startSession(context.Background(), localPlan(path, true), schema(), options(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSession(t, session, true)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := session.waitInitial(ctx); err != nil {
		t.Fatal(err)
	}
	if err := dependencies.Local.Runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for {
		update, err := session.Next(ctx)
		if err != nil {
			t.Fatal("missing terminal source report", err)
		}
		if errors.Is(update.Err, ErrSource) {
			break
		}
	}
	status, err := session.Status()
	if err != nil || status.State == Ready || status.State == Initializing {
		t.Fatal("terminal report preceded the stop fence", err)
	}
	closeSession(t, session, true)
	status, err = session.Status()
	if err != nil || !status.Complete || !status.Accepted || status.State != Closed {
		t.Fatal("terminal source state lost", err)
	}
	if !errors.Is(status.Latest.Err, ErrSource) {
		t.Fatal("terminal observation failure hidden", status.Latest.Err)
	}
	if read(t, session.Reader()).Workers != 2 {
		t.Fatal("terminal source removed accepted data")
	}
}
