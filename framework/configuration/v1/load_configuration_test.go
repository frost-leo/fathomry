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

package configuration

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/settings/v1"
)

func TestLoadConfigurationReturnsAcceptedSnapshot(t *testing.T) {
	for _, watch := range []bool{false, true} {
		name := "finite"
		if watch {
			name = "watch"
		}
		t.Run(name, func(t *testing.T) {
			dependencies, inbox := localDependencies(t, 8)
			path := startupFiles(t, watch)
			bootstrap, err := LoadBootstrap(context.Background(), BootstrapInput{Path: path}, dependencies.Local)
			if err != nil {
				t.Fatal(err)
			}
			session, err := LoadConfiguration(context.Background(), bootstrap, schema(),
				[]Variable{{Path: "/workers", Present: true, JSON: true, Value: "3"}}, dependencies)
			if session != nil {
				t.Cleanup(func() { closeSession(t, session, false) })
			}
			if err != nil {
				t.Fatal(err)
			}
			first, err := session.InitialView()
			if err != nil {
				t.Fatal("successful load has no initial snapshot", err)
			}
			state, err := session.Status()
			if err != nil || !state.Accepted || state.Watching != watch {
				t.Fatal("wrong load state", err)
			}
			if read(t, session.Reader()).Workers != 3 {
				t.Fatal("captured variable missing")
			}
			bootstrap.Configuration.Local.Paths[0] = "/not/the/selected/source"
			if watch {
				write(t, filepath.Join(filepath.Dir(path), "config/base.yaml"), "workers: 1\nlabels: [updated]")
				wait, stop := context.WithTimeout(context.Background(), 3*time.Second)
				defer stop()
				for {
					update, err := session.Next(wait)
					if err != nil {
						t.Fatal("load completion canceled Watch", err)
					}
					if update.Published && update.Publication > 1 {
						break
					}
				}
				if read(t, session.Reader()).Labels[0] != "updated" {
					t.Fatal("Watch used mutated caller selection")
				}
			}
			closeSession(t, session, false)
			original, err := settings.As[application](first)
			if err != nil {
				t.Fatal(err)
			}
			value, err := original.ValueCopy()
			if err != nil || value.Workers != 3 || len(value.Labels) != 0 {
				t.Fatal("initial view changed", err)
			}
			source, err := dependencies.Local.Runtime.Inspect()
			if err != nil || source.Closed {
				t.Fatal("borrowed runtime closed", err)
			}
			evidence, err := inbox.Inspect()
			if err != nil || evidence.Outstanding != 2 {
				t.Fatal("borrowed evidence discarded", err)
			}
			if settings.Configured() {
				t.Fatal("global settings installed")
			}
		})
	}
}

func TestLoadConfigurationFailureRetainsSession(t *testing.T) {
	dependencies, _ := localDependencies(t, 8)
	path := startupFiles(t, false)
	write(t, filepath.Join(filepath.Dir(path), "config/environment/development.yaml"), "workers: 0")
	bootstrap, err := LoadBootstrap(context.Background(), BootstrapInput{Path: path}, dependencies.Local)
	if err != nil {
		t.Fatal(err)
	}
	session, err := LoadConfiguration(context.Background(), bootstrap, schema(), nil, dependencies)
	if session == nil {
		t.Fatal("source/validation failure lost session", err)
	}
	t.Cleanup(func() { closeSession(t, session, true) })
	if !errors.Is(err, invalidWorkers) {
		t.Fatal("validator cause lost", err)
	}
	if _, err := session.InitialView(); !errors.Is(err, invalidWorkers) {
		t.Fatal("failed load has usable initial data", err)
	}
	if _, err := session.Reader().Capture(); !errors.Is(err, settings.ErrUnconfigured) {
		t.Fatal("invalid configuration published", err)
	}
}

func TestLoadConfigurationTimeoutAndCleanupBudget(t *testing.T) {
	dependencies, _ := localDependencies(t, 8)
	path := startupFiles(t, true)
	bootstrap, err := LoadBootstrap(context.Background(), BootstrapInput{Path: path}, dependencies.Local)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap.InitialTimeout = 100 * time.Millisecond
	bootstrap.ShutdownTimeout = 10 * time.Millisecond
	entered, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	declared := schema()
	declared.Validate = func(context.Context, application) error { close(entered); <-release; return nil }
	type result struct {
		session *Session[application]
		err     error
	}
	resultChannel := make(chan result, 1)
	go func() {
		session, err := LoadConfiguration(context.Background(), bootstrap, declared, nil, dependencies)
		resultChannel <- result{session, err}
	}()
	wait, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	select {
	case <-entered:
	case <-wait.Done():
		t.Fatal("validator not entered")
	}
	var loaded result
	select {
	case loaded = <-resultChannel:
	case <-wait.Done():
		t.Fatal("load did not honor initial budget")
	}
	if loaded.session == nil {
		t.Fatal("timed-out validator lost session", loaded.err)
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		closeSession(t, loaded.session, true)
	})
	if !errors.Is(loaded.err, context.DeadlineExceeded) {
		t.Fatal("load deadline lost", loaded.err)
	}
	if err := loaded.session.Close(wait); !errors.Is(err, context.DeadlineExceeded) || wait.Err() != nil {
		t.Fatal("bootstrap cleanup budget not applied", err)
	}
	if loaded.session.ShutdownComplete() {
		t.Fatal("blocked validator certified stopped")
	}
	close(release)
	closeSession(t, loaded.session, true)
	if _, err := loaded.session.Reader().Capture(); !errors.Is(err, settings.ErrUnconfigured) {
		t.Fatal("late validation published", err)
	}
}

func TestLoadConfigurationRejectsBeforeSourceEffects(t *testing.T) {
	dependencies, inbox := localDependencies(t, 8)
	path := startupFiles(t, false)
	bootstrap, err := LoadBootstrap(context.Background(), BootstrapInput{Path: path}, dependencies.Local)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Bootstrap){
		func(value *Bootstrap) { value.InitialTimeout = 0 },
		func(value *Bootstrap) { value.CandidateTimeout = 6 * time.Minute },
		func(value *Bootstrap) { value.ShutdownTimeout = 2 * time.Minute },
		func(value *Bootstrap) { value.Configuration.Provider = "nacos" },
	} {
		selected := bootstrap
		mutate(&selected)
		session, err := LoadConfiguration(context.Background(), selected, schema(), nil, dependencies)
		if err == nil || session != nil {
			t.Fatal("invalid declaration reached acquisition", err)
		}
	}
	status, err := inbox.Inspect()
	if err != nil || status.Outstanding != 1 {
		t.Fatal("invalid load caused source effects", err)
	}
}
