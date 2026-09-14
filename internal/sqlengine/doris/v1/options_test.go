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

package doris

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestOptionsFreezeStrictLayersAndConstructionWithoutIO(t *testing.T) {
	options := OptionsV1{Name: "doris", HTTPOrigins: []string{"http://127.0.0.1:1"}, Database: "gh42", User: "test", Password: "credential-canary", Plaintext: true}
	layer := []byte("database: selected")
	selection, err := Select(options, resource.Layer{Kind: resource.Local, Content: layer})
	if err != nil {
		t.Fatal(err)
	}
	options.HTTPOrigins[0] = "http://changed.invalid:80"
	options.Password = "changed"
	layer[0] = 'X'
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	assembly, err := resource.Assemble(ctx, ctx, "options", selection)
	if err != nil {
		t.Fatal(err)
	}
	source, _, err := resource.Bind(assembly, selection)
	if err != nil || source.owner.settings.Password != "credential-canary" || source.owner.settings.Database != "selected" || source.owner.settings.HTTPOrigins[0] != "http://127.0.0.1:1" {
		t.Fatal("configuration aliases caller")
	}
	if err := assembly.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"unknown: x", "max_rows: 0", "max_packet_bytes: 0", "active: 0", "queued: -1", "password: null", "timeout_ns: -1", "http_origins: []"} {
		if _, err := Select(options, resource.Layer{Kind: resource.Local, Content: []byte(raw)}); err == nil {
			t.Fatal("unsupported overlay accepted", raw)
		}
	}
	conformance.Runtime(t, source, new(Source), "credential-canary")
}
func TestInvalidEndpointTrustAndBounds(t *testing.T) {
	base := OptionsV1{Name: "doris", SQLAddress: "127.0.0.1:9030", Database: "gh42", User: "test", Plaintext: true}
	for _, mutate := range []func(*OptionsV1){
		func(o *OptionsV1) { o.SQLAddress = "localhost:9030" },
		func(o *OptionsV1) { o.SQLAddress = "0.0.0.0:9030" },
		func(o *OptionsV1) { o.SQLAddress = "[::ffff:0.0.0.0]:9030" },
		func(o *OptionsV1) { o.SQLAddress = "127.0.0.1:0" },
		func(o *OptionsV1) { o.Database = "../other" },
		func(o *OptionsV1) { o.User = "user:password" },
		func(o *OptionsV1) { o.Plaintext = false },
		func(o *OptionsV1) { o.RootCAPEM = "invalid" },
		func(o *OptionsV1) { o.MaxRows = 65537 },
		func(o *OptionsV1) { o.Queued = 65 },
		func(o *OptionsV1) { o.Active = 33 },
		func(o *OptionsV1) { o.MaxBatchBytes = 9 << 20 },
		func(o *OptionsV1) { o.Timeout = 2 * time.Minute },
		func(o *OptionsV1) { o.HTTPOrigins = make([]string, 17) },
	} {
		options := base
		mutate(&options)
		if _, err := Select(options); err == nil {
			t.Fatal("invalid options accepted")
		}
	}
	for _, value := range []string{"http://host/path", "http://host", "http://user:secret@host:80", "http://host:80?query", "http://host:80#fragment", "https://host:443", "http://0.0.0.0:80",
		"http://host:0", "ftp://host:21", "http://host:80/", "http://[fe80::1%25eth0]:80"} {
		options := base
		options.HTTPOrigins = []string{value}
		if _, err := Select(options); err == nil {
			t.Fatal("invalid origin accepted")
		}
	}
	for _, method := range []func() error{
		func() error {
			_, err := (*Client)(nil).Query(context.Background(), correlation("nil"), "SELECT 1")
			return err
		},
		func() error {
			_, err := (*Client)(nil).StreamLoad(context.Background(), correlation("nil"), testBatch())
			return err
		},
		func() error {
			_, err := (*Client)(nil).InspectLabel(context.Background(), correlation("nil"), "label")
			return err
		},
	} {
		if !errors.Is(method(), ErrInput) {
			t.Fatal("zero facade not rejected")
		}
	}
}
func FuzzFramingValidation(f *testing.F) {
	f.Add(byte(wireGreeting), peerGreeting(false))
	f.Add(byte(wireHeader), []byte{1})
	f.Add(byte(wireRows), []byte{0xfe, 0, 0, 0, 0, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, phase byte, body []byte) {
		if len(body) > 64<<10 {
			return
		}
		w := &wire{settings: defaults(OptionsV1{Plaintext: true}), phase: phase % 7, columns: 1, remaining: 1}
		err := w.inspect(append([]byte(nil), body...))
		if err == nil && w.columns > MaxColumns {
			t.Fatal("unbounded metadata")
		}
	})
}
func TestCountOverflowNeverBecomesSuccess(t *testing.T) {
	for _, count := range []string{"9223372036854775808", "null", "-1", "1.0", "1e0"} {
		body := strings.Replace(goodLoad, `"NumberTotalRows":1`, `"NumberTotalRows":`+count, 1)
		if parseLoad([]byte(body), 1, &LoadEvidence{Label: "gh42-run-1"}) == nil {
			t.Fatal("invalid count became success")
		}
	}
}

func TestProfileDistinguishesEnabledTransports(t *testing.T) {
	peer := newSQLPeer(t, false)
	var previous []compatibility.Profile
	for _, mode := range []struct{ sql, load bool }{{true, false}, {false, true}, {true, true}} {
		options := peer.options()
		if !mode.sql {
			options.SQLAddress = ""
		}
		if mode.load {
			options.HTTPOrigins = []string{"http://127.0.0.1:1"}
		}
		fixture := bindFixture(t, options, 1)
		profile := fixture.client.Profile()
		flags := map[string]string{}
		for _, option := range profile.Options {
			flags[option.Name] = option.Value
		}
		if flags["sql-enabled"] != strconv.FormatBool(mode.sql) || flags["stream-load-enabled"] != strconv.FormatBool(mode.load) {
			t.Fatal("effective transport modes lost")
		}
		for _, other := range previous {
			if reflect.DeepEqual(other, profile) {
				t.Fatal("different capability sets share a profile")
			}
		}
		previous = append(previous, profile)
	}
}
