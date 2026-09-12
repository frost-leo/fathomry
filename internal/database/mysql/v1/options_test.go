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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/go-sql-driver/mysql"
)

func TestOptionsFreezeAndPreflight(t *testing.T) {
	peer := newPeer(t, false, false)
	options := peer.options()
	layer := []byte("database: selected")
	selected, err := Select(options, resource.Layer{Kind: resource.Local, Content: layer})
	if err != nil {
		t.Fatal(err)
	}
	options.Password = "mutated"
	layer[0] = 'X'
	assembly, err := resource.Assemble(context.Background(), context.Background(), "options", selected)
	if err != nil {
		t.Fatal(err)
	}
	source, _, err := resource.Bind(assembly, selected)
	if err != nil || source.owner.settings.Password != "credential-canary" || source.owner.settings.Database != "selected" || source.owner.db.Stats().OpenConnections != 0 {
		t.Fatal("settings aliased or construction contacted a server")
	}
	if err = assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"unknown: secret", "max_packet_bytes: 0", "max_rows: 0", "timeout_ns: -1", "address: localhost", "password: null", "authentication: mysql_clear_password"} {
		if _, err := Select(peer.options(), resource.Layer{Kind: resource.Base, Content: []byte(raw)}); err == nil {
			t.Fatal("invalid overlay accepted")
		}
	}
	selected, err = Select(peer.options())
	if err != nil {
		t.Fatal(err)
	}
	if value, err := resource.Assemble(context.Background(), context.Background(), "duplicate", selected, selected); value != nil || !errors.Is(err, resource.ErrSelection) {
		t.Fatal("duplicate source reached construction")
	}
	conformance.Runtime(t, options, new(OptionsV1), "mutated")
}
func TestNativeConfigurationOwnsEveryMap(t *testing.T) {
	s := defaults(newPeer(t, false, false).options())
	first, second := nativeConfig(s), nativeConfig(s)
	first.Params["autocommit"] = "0"
	first.Params["injected"] = "private"
	if second.Params["autocommit"] != "1" || len(second.Params) != 2 {
		t.Fatal("native configuration maps alias")
	}
	// The upstream empty-map counterexample remains real; this integration never
	// borrows a caller SDK Config or its Params map in the first place.
	cfg := sdk.NewConfig()
	cfg.Params = map[string]string{}
	found := false
	sentinel := errors.New("before-network")
	_ = cfg.Apply(sdk.BeforeConnect(func(_ context.Context, v *sdk.Config) error { _, found = v.Params["later"]; return sentinel }))
	connector, err := sdk.NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Params["later"] = "1"
	if _, err = connector.Connect(context.Background()); !errors.Is(err, sentinel) || !found {
		t.Fatal("upstream empty-map ownership control changed")
	}
}
func FuzzOptionsV1(f *testing.F) {
	f.Add("127.0.0.1", uint16(3306), 1024, 4096)
	f.Fuzz(func(t *testing.T, address string, port uint16, rows, packet int) {
		if len(address) > 512 {
			return
		}
		options := OptionsV1{Name: "fuzz", Address: address, Port: port, Database: "fixture", User: "fixture", Password: "fixture", Plaintext: true, MaxRows: rows, MaxPacketBytes: packet}
		if _, err := Select(options); err != nil {
			conformance.Private(t, err, "never-an-option")
		}
	})
}
func FuzzStatement(f *testing.F) {
	f.Add("SELECT ?", "value")
	f.Add("LOAD DATA", "")
	f.Fuzz(func(t *testing.T, query, value string) {
		if len(query)+len(value) > 2*MaxSQLBytes {
			return
		}
		if err := validStatement(query, []any{value}); err == nil && (strings.ContainsRune(query, 0) || len(query) > MaxSQLBytes) {
			t.Fatal("malformed SQL accepted")
		}
	})
}

func TestArgumentDatesFailBeforeNativeParameterDispatch(t *testing.T) {
	for _, value := range []time.Time{time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.FixedZone("positive", 3600))} {
		if err := validStatement("SELECT ?, ?", []any{make([]byte, 800<<10), value}); !errors.Is(err, ErrInput) {
			t.Fatal("native-rejected UTC date reached partial parameter dispatch")
		}
	}
	for _, value := range []time.Time{{}, time.Date(1, 1, 1, 0, 0, 0, 1, time.UTC), time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)} {
		if err := validStatement("SELECT ?", []any{value}); err != nil {
			t.Fatal("supported native date rejected", err)
		}
	}
}
