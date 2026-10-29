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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	viper "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

type application struct {
	Workers  int               `json:"workers"`
	Labels   []string          `json:"labels"`
	Defaults map[string]string `json:"defaults"`
	Optional *int              `json:"optional"`
}

var invalidWorkers = errors.New("private-validator-canary")

func schema() configsource.Schema[application] {
	return configsource.Schema[application]{Version: 1, Validate: func(ctx context.Context, value application) error {
		if value.Workers < 1 {
			return invalidWorkers
		}
		return ctx.Err()
	}}
}
func options() Options {
	return Options{InitialTimeout: 3 * time.Second, CandidateTimeout: time.Second}
}
func localDependencies(t *testing.T, capacity int) (Dependencies, *adapters.Inbox[viper.Evidence]) {
	t.Helper()
	runtime, err := adapters.New(context.Background(), adapters.Options{})
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[viper.Evidence](adapters.EvidenceOptions{Capacity: capacity})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
		status, _ := inbox.Inspect()
		for range status.Queued {
			delivery, err := inbox.NextReleased(ctx)
			if err != nil {
				t.Error(err)
				break
			}
			if err := delivery.Ack(); err != nil {
				t.Error(err)
			}
		}
	})
	return Dependencies{Local: viper.Dependencies{Runtime: runtime, Evidence: inbox}}, inbox
}
func localPlan(path string, watch bool) Bootstrap {
	return Bootstrap{Version: 1, Environment: "development", Mode: "local", Provider: "viper", Watch: watch,
		Documents: []Document{{Kind: "base", Encoding: configsource.YAML}},
		Local:     &viper.WatchSettings{Paths: []string{path}, Interval: 10 * time.Millisecond}}
}
func write(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path+".next", []byte(value), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".next", path); err != nil {
		t.Fatal(err)
	}
}
func read(t *testing.T, reader settings.Reader) application {
	t.Helper()
	view, err := reader.Capture()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := settings.As[application](view)
	if err != nil {
		t.Fatal(err)
	}
	value, err := snapshot.ValueCopy()
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func closeSession(t *testing.T, session *Session[application], allowError bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := session.Close(ctx); err != nil && !allowError {
		t.Error(err)
	}
	if !session.ShutdownComplete() {
		t.Error("cleanup incomplete")
	}
}
func TestBootstrap(t *testing.T) {
	dependencies, inbox := localDependencies(t, 16)
	dir := t.TempDir()
	app := filepath.Join(dir, "application.yaml")
	write(t, app, "workers: 2")
	first := filepath.Join(dir, "bootstrap.yaml")
	write(t, first, fmt.Sprintf("version: 1\nmode: local\nprovider: viper\ndocuments:\n - kind: base\n   encoding: yaml\nlocal:\n paths: [%q]\n", app))
	input := BootstrapInput{Environment: "development", Files: []File{{Path: first, Document: Document{Kind: "base", Encoding: configsource.YAML}}}}
	value, err := LoadBootstrap(context.Background(), input, dependencies.Local)
	if err != nil || value.Environment != "development" {
		t.Fatal(err)
	}
	prod := input
	prod.Environment = "production"
	other, err := LoadBootstrap(context.Background(), prod, dependencies.Local)
	if err != nil || other.Environment != "production" {
		t.Fatal("explicit environment selection failed", err)
	}
	status, _ := inbox.Inspect()
	if status.Queued != 2 {
		t.Fatal("bootstrap evidence was silently acknowledged")
	}
	input.Variables = []configsource.Variable{{Path: "/environment", Present: true, Value: "different"}}
	if _, err := LoadBootstrap(context.Background(), input, dependencies.Local); !errors.Is(err, ErrDeclaration) {
		t.Fatal("environment override accepted", err)
	}
	input.Variables = nil
	input.Files[0].Path = filepath.Join(dir, "missing")
	if _, err := LoadBootstrap(context.Background(), input, dependencies.Local); !errors.Is(err, ErrMissing) {
		t.Fatal("missing was not distinguished", err)
	}
}
func TestAdmissionAndInputIsolation(t *testing.T) {
	dependencies, inbox := localDependencies(t, 16)
	path := filepath.Join(t.TempDir(), "application.yaml")
	write(t, path, "workers: 2")
	plan := localPlan(path, true)
	defaultValue := 7
	declared := schema()
	declared.Defaults = application{Labels: []string{"original"}, Defaults: map[string]string{"key": "original"}, Optional: &defaultValue}
	selected := options()
	selected.Variables = []configsource.Variable{{Path: "/workers", Present: true, JSON: true, Value: "3"}}
	badSchema := declared
	badSchema.Validate = nil
	if value, err := Start(context.Background(), plan, badSchema, selected, dependencies); value != nil || !errors.Is(err, ErrDeclaration) {
		t.Fatal("nil validator admitted", err)
	}
	state, _ := inbox.Inspect()
	if state.Queued != 0 {
		t.Fatal("rejected declaration created evidence")
	}
	session, err := Start(context.Background(), plan, declared, selected, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSession(t, session, false)
	plan.Local.Paths[0] = "/not/the/selected/path"
	declared.Defaults.Labels[0] = "mutated"
	declared.Defaults.Defaults["key"] = "mutated"
	defaultValue = 99
	selected.Variables[0].Value = "9"
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := session.WaitInitial(ctx); err != nil {
		t.Fatal(err)
	}
	value := read(t, session.Reader())
	if value.Workers != 3 || value.Labels[0] != "original" || value.Defaults["key"] != "original" || *value.Optional != 7 {
		t.Fatal("retained caller aliases")
	}
	write(t, path, "workers: 4")
	update, err := session.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for update.Publication < 2 {
		update, err = session.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
	if read(t, session.Reader()).Workers != 3 {
		t.Fatal("captured variable changed")
	}
}
func TestBootstrapEvidencePressure(t *testing.T) {
	dependencies, _ := localDependencies(t, 1)
	path := filepath.Join(t.TempDir(), "application.yaml")
	write(t, path, "workers: 1")
	client, err := viper.New(dependencies.Local)
	if err != nil {
		t.Fatal(err)
	}
	source, err := client.Source(*localPlan(path, false).Local)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.Capture(context.Background()); err != nil {
		t.Fatal(err)
	}
	session, err := Start(context.Background(), localPlan(path, false), schema(), options(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSession(t, session, true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := session.WaitInitial(ctx); err == nil {
		t.Fatal("pending evidence was discarded")
	}
	if _, err := session.Reader().Capture(); !errors.Is(err, settings.ErrUnconfigured) {
		t.Fatal("failed initial load published")
	}
}
func TestRejectedPlans(t *testing.T) {
	dependencies, _ := localDependencies(t, 16)
	path := filepath.Join(t.TempDir(), "application.yaml")
	for _, mutate := range []func(*Bootstrap){
		func(value *Bootstrap) { value.Version = 2 },
		func(value *Bootstrap) { value.Environment = "../bad" },
		func(value *Bootstrap) { value.Provider = "nacos" },
		func(value *Bootstrap) { value.Documents = append(value.Documents, value.Documents[0]) },
		func(value *Bootstrap) { value.Local.QueueCapacity = 65 },
		func(value *Bootstrap) { value.Local.Interval = time.Nanosecond },
		func(value *Bootstrap) { value.Local.Paths = []string{"relative"} },
	} {
		plan := localPlan(path, false)
		mutate(&plan)
		if session, err := Start(context.Background(), plan, schema(), options(), dependencies); err == nil || session != nil {
			t.Fatal("invalid source plan admitted")
		}
	}
}
