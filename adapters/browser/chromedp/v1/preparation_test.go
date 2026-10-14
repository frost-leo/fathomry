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
	"crypto/tls"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	browser "github.com/frost-leo/fathomry/adapters/browser/chromedp/v1"
	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
)

func TestPreparationIsOfflineAndDistinguishesExplicitZero(t *testing.T) {
	var callbacks atomic.Int64
	settings := browser.Settings{Name: "prepared", ExecPath: "/explicitly-not-started"}
	prepared, err := browser.Prepare(settings, browser.NativeOptions{TLS: &tls.Config{GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
		callbacks.Add(1)
		return nil, errors.New("unexpected callback")
	}}})
	if err != nil || callbacks.Load() != 0 {
		t.Fatal("preparation performed I/O or invoked native code", err)
	}
	if prepared.Policy().Runtime.MaxActive != 5 {
		t.Fatal("source and default context admissions not reserved")
	}
	for _, change := range []func(*browser.Settings){
		func(s *browser.Settings) { s.MaxSessions = new(0) },
		func(s *browser.Settings) { s.MaxPages = new(0) },
		func(s *browser.Settings) { s.MaxStreams = new(0) },
		func(s *browser.Settings) { s.MaxObjectGroups = new(0) },
		func(s *browser.Settings) { s.MaxObjects = new(0) },
		func(s *browser.Settings) { s.MaxStreamBytes = new(int64(0)) },
		func(s *browser.Settings) { s.MaxCommands = new(0) },
		func(s *browser.Settings) { s.MaxCommandBytes = new(int64(0)) },
		func(s *browser.Settings) { s.MaxResultBytes = new(int64(0)) },
		func(s *browser.Settings) { s.MaxEventBytes = new(int64(0)) },
		func(s *browser.Settings) { s.MaxEvents = new(0) },
		func(s *browser.Settings) { s.SessionTimeout = new(time.Duration(0)) },
	} {
		value := settings
		change(&value)
		if err := browser.Validate(value); err == nil {
			t.Fatal("explicit zero silently defaulted")
		}
	}
	snapshot, err := configsource.Prepare(context.Background(), configsource.Schema[browser.Settings]{Version: 1, Defaults: settings, Validate: func(_ context.Context, s browser.Settings) error { return browser.Validate(s) }},
		[]configsource.Layer{{Kind: configsource.Local, Encoding: configsource.JSON, Content: []byte(`{"max_pages":2,"queued_calls":0}`)}})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := snapshot.ValueCopy()
	if err != nil || resolved.MaxPages == nil || *resolved.MaxPages != 2 || resolved.QueuedCalls != 0 {
		t.Fatal("loadable values changed", err)
	}
	if _, err := json.Marshal(prepared); !errors.Is(err, browser.ErrSerialization) {
		t.Fatal("prepared runtime state became durable", err)
	}
}
func TestPreparationRejectsMalformedAndConflictingInputs(t *testing.T) {
	for _, value := range []browser.Settings{
		{Name: "bad", ExecPath: "relative-chrome"},
		{Name: "bad", ExecPath: "/browser", RemoteURL: "ws://127.0.0.1:1/devtools/browser/fake"},
		{Name: "bad", ExecPath: "/browser", Flags: map[string]string{"no-sandbox": ""}},
		{Name: "bad", ExecPath: "/browser", Environment: []string{"missing-equals"}},
		{Name: "bad", ExecPath: "/browser", Headers: map[string][]string{"X-Value": {"invalid-\xff"}}},
		{Name: "bad", ExecPath: "/browser", MaxPages: new(-1)},
		{Name: "bad", ExecPath: "/browser", Version: 2},
	} {
		if err := browser.Validate(value); err == nil {
			t.Fatal("malformed input accepted")
		}
	}
}
