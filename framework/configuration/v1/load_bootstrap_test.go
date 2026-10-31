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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func TestDotenvProfile(t *testing.T) {
	dependencies, inbox := localDependencies(t, 32)
	path := startupFiles(t, false)
	selected, err := LoadBootstrap(context.Background(), BootstrapInput{Path: path}, dependencies.Local)
	if err != nil {
		t.Fatal(err)
	}
	if selected.Configuration.Environment != "development" || selected.InitialTimeout != 30*time.Second ||
		selected.CandidateTimeout != 5*time.Second || selected.ShutdownTimeout != 10*time.Second {
		t.Fatal("wrong profile defaults")
	}
	if selected.Configuration.Local.Paths[0] != filepath.Join(filepath.Dir(path), "config/base.yaml") {
		t.Fatal("wrong directory anchor")
	}
	env := "staging"
	selected, err = LoadBootstrap(context.Background(), BootstrapInput{Path: path, Environment: &env, Variables: map[string]string{
		"FATHOMRY_ENV": "production", "FATHOMRY_CONFIG_DIR": "other", "FATHOMRY_CONFIG_WATCH": "true",
		"FATHOMRY_STARTUP_TIMEOUT": "2s", "FATHOMRY_CONFIG_CANDIDATE_TIMEOUT": "1s", "FATHOMRY_SHUTDOWN_TIMEOUT": "20ms",
	}}, dependencies.Local)
	if err != nil {
		t.Fatal(err)
	}
	if selected.Configuration.Environment != "staging" || !selected.Configuration.Watch ||
		selected.Configuration.Local.Paths[1] != filepath.Join(filepath.Dir(path), "other/environment/staging.yaml") ||
		selected.InitialTimeout != 2*time.Second || selected.CandidateTimeout != time.Second || selected.ShutdownTimeout != 20*time.Millisecond {
		t.Fatal("override policy failed")
	}
	status, _ := inbox.Inspect()
	if status.Queued != 2 {
		t.Fatal("bootstrap silently acknowledged evidence")
	}
	keys := BootstrapKeys()
	keys[0] = "changed"
	if BootstrapKeys()[0] == "changed" {
		t.Fatal("mutable key inventory")
	}
}
func TestDotenvRejectingControls(t *testing.T) {
	cases := []map[string]string{
		{"UNKNOWN": "x"}, {"FATHOMRY_ENV": ""}, {"FATHOMRY_ENV": "../other"}, {"FATHOMRY_BOOTSTRAP_VERSION": "2"},
		{"FATHOMRY_CONFIG_MODE": "other"}, {"FATHOMRY_CONFIG_PROVIDER": "other"}, {"FATHOMRY_CONFIG_WATCH": "1"},
		{"FATHOMRY_CONFIG_DIR": ""}, {"FATHOMRY_CONFIG_DIR": "x\x00y"}, {"FATHOMRY_CONFIG_DIR": strings.Repeat("x", configsource.MaxDotenvBytes)},
		{"FATHOMRY_STARTUP_TIMEOUT": "0s"}, {"FATHOMRY_STARTUP_TIMEOUT": "6m"}, {"FATHOMRY_CONFIG_CANDIDATE_TIMEOUT": "-1s"},
		{"FATHOMRY_SHUTDOWN_TIMEOUT": "61s"}, {"FATHOMRY_CONFIG_DOCUMENTS_JSON": "[{\"unknown\":1}]"},
		{"FATHOMRY_NACOS_SETTINGS_JSON": "{\"unknown\":1}"},
	}
	for index, variables := range cases {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			dependencies, inbox := localDependencies(t, 4)
			_, err := LoadBootstrap(context.Background(), BootstrapInput{Path: filepath.Join(t.TempDir(), "missing"), Variables: variables}, dependencies.Local)
			if err == nil || errors.Is(err, ErrMissing) {
				t.Fatal("invalid variables reached acquisition", err)
			}
			status, _ := inbox.Inspect()
			if status.Queued != 0 {
				t.Fatal("invalid declaration caused source effects")
			}
		})
	}
}
func TestDotenvLayersAndPrivacy(t *testing.T) {
	t.Setenv("FATHOMRY_ENV", "not-selected")
	dependencies, _ := localDependencies(t, 32)
	path := startupFiles(t, false)
	if _, err := LoadBootstrap(context.Background(), BootstrapInput{Path: path}, dependencies.Local); err != nil {
		t.Fatal("ambient environment was read", err)
	}
	if os.Getenv("FATHOMRY_ENV") != "not-selected" {
		t.Fatal("process environment changed")
	}
	for _, bad := range []string{"", "# only a comment\n", "export FATHOMRY_ENV=development\n",
		"FATHOMRY_ENV=development\nFATHOMRY_ENV=production\n", "UNKNOWN=1\n", "FATHOMRY_CONFIG_WATCH=1\n"} {
		write(t, path, bad)
		if _, err := LoadBootstrap(context.Background(), BootstrapInput{Path: path, Variables: map[string]string{"FATHOMRY_CONFIG_WATCH": "false"}}, dependencies.Local); err == nil {
			t.Fatal("invalid original layer hidden")
		}
	}
	if _, err := LoadBootstrap(context.Background(), BootstrapInput{Path: filepath.Join(t.TempDir(), "missing")}, dependencies.Local); !errors.Is(err, ErrMissing) {
		t.Fatal("missing dotenv hidden", err)
	}
	for _, value := range []any{BootstrapInput{Path: "private-path", Variables: map[string]string{"x": "credential-canary"}},
		Bootstrap{Configuration: SourcePlan{Environment: "credential-canary"}}} {
		if text := fmt.Sprintf("%+v %#v", value, value); strings.Contains(text, "credential-canary") || strings.Contains(text, "private-path") {
			t.Fatal("unsafe formatting")
		}
	}
}
func TestStartupRemoteResource(t *testing.T) {
	for _, watch := range []bool{false, true} {
		t.Run(fmt.Sprint(watch), func(t *testing.T) {
			peer := newPeer(t)
			scope, ctx := startupScope(t)
			plan := peer.plan(watch)
			raw, err := json.Marshal(plan.Remote.Settings)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), ".env")
			write(t, path, fmt.Sprintf("FATHOMRY_ENV=fixture\nFATHOMRY_CONFIG_MODE=remote\nFATHOMRY_CONFIG_PROVIDER=nacos\nFATHOMRY_CONFIG_WATCH=%t\nFATHOMRY_CONFIG_DOCUMENTS_JSON='[{\"kind\":\"base\",\"encoding\":\"yaml\"}]'\nFATHOMRY_NACOS_SETTINGS_JSON='%s'\n", watch, raw))
			local, remote := 0, 0
			ref, _, err := BindConfiguration(scope, "configuration", fixedSchema(schema()), Policy{Receive: func(_ context.Context, evidence Evidence) error {
				if evidence.Viper != nil {
					local++
				}
				if evidence.Nacos != nil {
					remote++
					if !evidence.Nacos.Info().Released {
						t.Error("live source acknowledged")
					}
				}
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			input, err := PrepareInput(ctx, BootstrapInput{Path: path}, nil)
			if err != nil {
				t.Fatal(err)
			}
			update, err := scope.Apply(ctx, input.View())
			if err != nil {
				t.Fatal(err)
			}
			if err := update.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			lease, err := ref.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			loaded, err := lease.Value()
			if err != nil {
				t.Fatal(err)
			}
			if read(t, loaded.Reader()).Workers != 2 {
				t.Fatal("remote configuration mismatch")
			}
			if err := lease.Release(); err != nil {
				t.Fatal(err)
			}
			if err := scope.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if local != 1 || remote != 2 || peer.queries.Load() != 1 || peer.writes.Load() != 0 {
				t.Fatal("remote acquisition/custody contract", local, remote, peer.queries.Load(), peer.writes.Load())
			}
		})
	}
}
func TestFirstAcceptedViewIsStable(t *testing.T) {
	dependencies, _ := localDependencies(t, 8)
	path := filepath.Join(t.TempDir(), "application.yaml")
	write(t, path, "workers: 1")
	session, err := startSession(context.Background(), localPlan(path, true), schema(), options(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSession(t, session, false)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := session.waitInitial(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Next(ctx); err != nil {
		t.Fatal(err)
	}
	write(t, path, "workers: 2")
	for {
		value, err := session.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if value.Published && value.Publication >= 2 {
			break
		}
	}
	view, err := session.InitialView()
	if err != nil {
		t.Fatal(err)
	}
	first, err := settings.As[application](view)
	if err != nil {
		t.Fatal(err)
	}
	value, err := first.ValueCopy()
	if err != nil || value.Workers != 1 || read(t, session.Reader()).Workers != 2 {
		t.Fatal("first view followed Watch", err)
	}
}

func TestProjectedApplicationReplacement(t *testing.T) {
	dependencies, _ := localDependencies(t, 8)
	directory := t.TempDir()
	for _, version := range []struct{ name, data string }{{"first", "workers: 1"}, {"rejected", "workers: 0"}, {"recovered", "workers: 3"}} {
		path := filepath.Join(directory, version.name)
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
		write(t, filepath.Join(path, "application.yaml"), version.data)
	}
	if err := os.Symlink("first", filepath.Join(directory, "..data")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "application.yaml")
	if err := os.Symlink("..data/application.yaml", path); err != nil {
		t.Fatal(err)
	}
	session, err := startSession(context.Background(), localPlan(path, true), schema(), options(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSession(t, session, false)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := session.waitInitial(ctx); err != nil {
		t.Fatal(err)
	}
	first, err := session.InitialView()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Next(ctx); err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"rejected", "recovered"} {
		if err := os.Symlink(version, filepath.Join(directory, "..next")); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(filepath.Join(directory, "..next"), filepath.Join(directory, "..data")); err != nil {
			t.Fatal(err)
		}
		for {
			update, err := session.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if version == "rejected" && errors.Is(update.Err, invalidWorkers) {
				if update.Published || read(t, session.Reader()).Workers != 1 {
					t.Fatal("rejected projection replaced last-good data")
				}
				break
			}
			if version == "recovered" && update.Published && read(t, session.Reader()).Workers == 3 {
				break
			}
		}
	}
	snapshot, err := settings.As[application](first)
	if err != nil {
		t.Fatal(err)
	}
	value, err := snapshot.ValueCopy()
	if err != nil || value.Workers != 1 {
		t.Fatal("old view changed with projected files", err)
	}
}
func FuzzDotenvAdmission(f *testing.F) {
	for _, value := range []string{"true", "false", "1", "\x00", strings.Repeat("x", 65)} {
		f.Add(value)
	}
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > configsource.MaxDotenvBytes+1 {
			t.Skip()
		}
		err := admitDotenvValues(context.Background(), map[string]string{"FATHOMRY_CONFIG_WATCH": value})
		if (err == nil) != (value == "true" || value == "false") {
			t.Fatal("watch grammar mismatch")
		}
	})
}
