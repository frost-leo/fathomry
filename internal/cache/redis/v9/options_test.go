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

package redis

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/redis/go-redis/v9"
)

func TestAuthorityAndPreparedSettings(t *testing.T) {
	address := peer(t, func([]string) string { return "+PONG\r\n" })
	options := testOptions(address)
	cases := map[string]func(*OptionsV1){
		"version":             func(o *OptionsV1) { o.Version = 2 },
		"implicit-topology":   func(o *OptionsV1) { o.Mode = "" },
		"universal-ambiguous": func(o *OptionsV1) { o.Mode = "universal" },
		"csc-cluster":         func(o *OptionsV1) { o.Mode = "cluster"; o.ExperimentalCache = true },
		"csc-protocol":        func(o *OptionsV1) { o.ExperimentalCache = true; o.Protocol = 2 },
		"raw-auth":            func(o *OptionsV1) { o.AdminCommands = []string{"AUTH"} },
		"admin-unapproved":    func(o *OptionsV1) { o.Commands = []string{"FLUSHALL"} },
		"unbounded":           func(o *OptionsV1) { o.MaxReplyBytes = -1 },
		"plaintext-implicit":  func(o *OptionsV1) { o.Plaintext = false },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			copy := options
			change(&copy)
			if _, err := Select(copy); err == nil {
				t.Fatal("invalid accepted")
			}
		})
	}
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	options.Addrs[0] = "127.0.0.1:1"
	options.Commands[0] = "FLUSHALL"
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(context.Background(), context.Background(), "frozen", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(context.Background())
	inbox, _ := invocation.NewInbox[Result](1, defaults(options).evidenceReservation())
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := executeTest(t, client, "frozen", "PING")
	if got.Err() != nil {
		t.Fatal("prepared options aliased")
	}
	if _, err := client.Execute(context.Background(), fault.Correlation{Call: "denied"}, NewCommand("FLUSHALL")); !errors.Is(err, ErrAuthority) {
		t.Fatal("ungranted raw admin permitted")
	}
	drain(t, inbox)
}

func TestExplicitZeroIdleAndNamedCredentialBoundary(t *testing.T) {
	options := testOptions("127.0.0.1:1")
	selected, err := Select(options, resource.Layer{Kind: resource.Local, Content: []byte("max_idle_time_ns: 0\n")})
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(context.Background(), context.Background(), "zero-idle", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(context.Background())
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		t.Fatal(err)
	}
	if source.owner.settings.MaxIdleTime != 0 || source.owner.native.(*sdk.Client).Options().ConnMaxIdleTime != -1 {
		t.Fatal("explicit zero silently inherited the SDK idle default")
	}
	options.Username = "non-default-account"
	if _, err := Select(options); !errors.Is(err, ErrUnsupported) {
		t.Fatal("empty password can silently select the default account")
	}
	password, _ := NewPassword("")
	selected, err = SelectWithPassword(options, password)
	if err != nil {
		t.Fatal(err)
	}
	var entered atomic.Int32
	options.Addrs = []string{peer(t, func([]string) string { entered.Add(1); return "+OK\r\n" })}
	selected, err = SelectWithPassword(options, password)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	rotation, err := resource.Assemble(context.Background(), context.Background(), "empty-rotation", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer rotation.Close(context.Background())
	inbox, _ := invocation.NewInbox[Result](1, defaults(options).evidenceReservation())
	client, err := Bind(rotation, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := executeTest(t, client, "identity", "GET", "owned")
	if !errors.Is(result.Err(), ErrUnsupported) || entered.Load() != 0 {
		t.Fatal("rotating empty credential crossed the frozen account boundary")
	}
	drain(t, inbox)
}
