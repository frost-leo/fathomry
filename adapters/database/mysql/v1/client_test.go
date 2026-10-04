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

package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/adapters/v1"
)

func FuzzOpaqueCorrelation(f *testing.F) {
	for _, seed := range []struct {
		id     string
		family uint64
	}{
		{"", 1}, {"测试/correlation", 2}, {strings.Repeat("x", 256), ^uint64(0)},
		{strings.Repeat("x", 257), 3}, {"nul\x00value", 4}, {"del\x7f", 5}, {string([]byte{0xff}), 6},
	} {
		f.Add(seed.id, seed.family)
	}
	f.Fuzz(func(t *testing.T, id string, familyID uint64) {
		if len(id) > 4096 {
			return
		}
		valid := len(id) <= 256 && utf8.ValidString(id)
		for _, char := range id {
			if char < 0x20 || char == 0x7f {
				valid = false
			}
		}
		original := &Client{lifetime: context.Background(), id: "original"}
		copied, err := original.WithID(id)
		if valid != (err == nil) {
			t.Fatal("opaque correlation boundary changed")
		}
		if original.id != "original" {
			t.Fatal("WithID mutated its original facade")
		}
		if !valid {
			if copied != nil {
				t.Fatal("rejected correlation returned a facade")
			}
			return
		}
		if copied == original || copied.id != id {
			t.Fatal("WithID did not preserve an independent opaque ID")
		}
		runtime, err := adapters.New(context.Background(), adapters.Options{MaxActive: 1, MaxWorkBytes: 1})
		if err != nil {
			t.Fatal(err)
		}
		defer runtime.Close(context.Background())
		inbox, err := adapters.NewInbox[Result](adapters.EvidenceOptions{Capacity: 1, MaxBytes: 1})
		if err != nil {
			t.Fatal(err)
		}
		endpoint, err := bind(Dependencies{Runtime: runtime, Evidence: inbox})
		if err != nil {
			t.Fatal(err)
		}
		family := &evidenceFamily{id: familyID}
		_, err = endpoint.Run(context.Background(), request("fuzz", copied.id, 1, 1), func(call *adapters.Call[Result]) {
			native := family.correlation(call)
			snapshot, _ := call.Receipt().Snapshot()
			if snapshot.Info().ID != id || native.Call != "db-"+strconv.FormatUint(familyID, 10)+"-1" || native.Parent != "" {
				t.Fatal("public opaque ID altered code-owned native correlation")
			}
			if err := call.Resolve(adapters.Outcome[Result]{}); err != nil {
				t.Fatal(err)
			}
		})
		if err != nil {
			t.Fatal("accepted opaque ID rejected by public admission")
		}
		delivery, err := inbox.NextReleased(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Ack(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestClientLifetimeFencesAllWork(t *testing.T) {
	peer := newPeer(t, false, false)
	owner, inbox, runtime := testOwner(t, peer.options(), 0)
	lifetime, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	client := testUsingClient(t, lifetime, owner, inbox, runtime)
	cause := errors.New("explicit client lifetime ended")
	cancel(cause)
	before := peer.queries.Load()
	operations := []struct {
		name string
		call func() error
	}{
		{"ping", func() error { _, err := client.Ping(context.Background()); return err }},
		{"query", func() error { _, err := client.Query(context.Background(), "SELECT cells"); return err }},
		{"exec", func() error { _, err := client.Exec(context.Background(), "UPDATE cells"); return err }},
		{"prepare", func() error { _, err := client.Prepare(context.Background(), "SELECT cells"); return err }},
		{"begin", func() error { _, err := client.Begin(context.Background(), txOptions()); return err }},
		{"stats", func() error { _, err := client.Stats(context.Background()); return err }},
		{"profile", func() error { _, err := client.Profile(context.Background()); return err }},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			err := operation.call()
			if !errors.Is(err, cause) || !errors.Is(err, context.Canceled) {
				t.Fatalf("lifetime causes lost: %v", err)
			}
		})
	}
	if peer.queries.Load() != before {
		t.Fatal("expired client dispatched native work")
	}
	status, err := inbox.Inspect()
	if err != nil || status.Outstanding != 1 {
		t.Fatalf("expired work reserved evidence: %v", err)
	}
}

func TestZeroClient(t *testing.T) {
	for _, client := range []*Client{nil, {}} {
		operations := []struct {
			name string
			call func() error
		}{
			{"ping", func() error { _, err := client.Ping(context.Background()); return err }},
			{"query", func() error { _, err := client.Query(context.Background(), "SELECT cells"); return err }},
			{"exec", func() error { _, err := client.Exec(context.Background(), "UPDATE cells"); return err }},
			{"prepare", func() error { _, err := client.Prepare(context.Background(), "SELECT cells"); return err }},
			{"begin", func() error { _, err := client.Begin(context.Background(), txOptions()); return err }},
			{"stats", func() error { _, err := client.Stats(context.Background()); return err }},
			{"profile", func() error { _, err := client.Profile(context.Background()); return err }},
			{"id", func() error { _, err := client.WithID("valid"); return err }},
		}
		for _, operation := range operations {
			t.Run(operation.name, func(t *testing.T) {
				if err := operation.call(); err == nil {
					t.Fatal("accepted invalid client")
				}
			})
		}
	}
}

type rejectedValuer struct{ called *bool }

func (value rejectedValuer) Value() (driver.Value, error) {
	*value.called = true
	return "injected", nil
}

func TestArgumentsRefuseNativeEscapes(t *testing.T) {
	peer := newPeer(t, false, false)
	owner, inbox, _ := testOwner(t, peer.options(), 0)
	called := false
	for _, argument := range []any{rejectedValuer{&called}, map[string]any{}, func() { called = true }} {
		before := peer.queries.Load()
		if _, err := owner.Client().Query(context.Background(), "SELECT ?", argument); !errors.Is(err, ErrUnsupported) {
			t.Fatal("unsupported argument admitted", err)
		}
		if called || peer.queries.Load() != before {
			t.Fatal("rejected argument escaped into native work")
		}
		ack(t, inbox)
	}
	for _, query := range []string{"", strings.Repeat("x", MaxSQLBytes+1)} {
		if _, err := owner.Client().Query(context.Background(), query); !errors.Is(err, ErrInput) {
			t.Fatal("invalid SQL bound accepted", err)
		}
		ack(t, inbox)
	}
}
