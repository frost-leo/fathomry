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
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
)

func TestNativeRoutingPositions(t *testing.T) {
	tests := []struct {
		args     []string
		position int8
	}{
		{[]string{"XGROUP", "CREATE", "{s}:stream", "group", "0"}, 2},
		{[]string{"XINFO", "GROUPS", "{s}:stream"}, 2},
		{[]string{"XREAD", "COUNT", "1", "BLOCK", "10", "STREAMS", "{s}:stream", "0"}, 6},
		{[]string{"XREADGROUP", "GROUP", "STREAMS", "consumer", "COUNT", "1", "STREAMS", "{s}:stream", ">"}, 7},
		{[]string{"XREADGROUP", "COUNT", "1", "GROUP", "STREAMS", "CLAIM", "CLAIM", "1000", "STREAMS", "{s}:stream", ">"}, 9},
		{[]string{"FCALL", "function", "1", "{s}:key", "argument"}, 3},
		{[]string{"ZUNION", "1", "{s}:sorted"}, 2},
		{[]string{"SINTERCARD", "1", "{s}:set"}, 2},
		{[]string{"BITOP", "NOT", "{s}:out", "{s}:in"}, 2},
		{[]string{"BLMPOP", "1", "1", "{s}:list", "LEFT"}, 3},
		{[]string{"MEMORY", "USAGE", "{s}:key"}, 2},
	}
	for _, test := range tests {
		command := NewCommand(test.args...)
		got, err := command.routingPosition()
		if err != nil || got != test.position {
			t.Errorf("routing %s: position=%d err=%v", test.args[0], got, err)
		}
		native := command.native(context.Background())
		if native.Args()[0] != native.Name() {
			t.Fatal("native command name is not canonical")
		}
	}
	for _, args := range [][]string{{"CLUSTER"}, {"CLUSTER", "COUNTKEYSINSLOT"}, {"CLUSTER", "GETKEYSINSLOT", "-1", "1"}, {"XREAD"}, {"XREADGROUP", "GROUP", "group", "consumer", "STREAMS"}, {"ZUNION", "999", "key"}} {
		if _, err := NewCommand(args...).routingPosition(); err == nil {
			t.Fatalf("malformed %s reached native routing", args[0])
		}
	}
}
func TestClusterSlotArgumentsNeverPanicOrStrand(t *testing.T) {
	options := testOptions("127.0.0.1:1")
	options.Mode = "cluster"
	options.AdminCommands = []string{"CLUSTER|COUNTKEYSINSLOT"}
	client, _, inbox, _ := bindTest(t, options, 2)
	command := NewCommand("cluster", "countkeysinslot", "1")
	args := command.native(context.Background()).Args()
	if slot, ok := args[2].(int); !ok || slot != 1 {
		t.Fatal("native slot type mismatch")
	}
	receipt, err := client.Execute(context.Background(), fault.Correlation{Call: "slot"}, command)
	got := resolved(t, receipt, err)
	if got.Err() == nil || !got.Released || got.Outcome.Value.Commands()[0].State() == Replied {
		t.Fatal("failed routing invented a reply or stranded work")
	}
	drain(t, inbox)
	if _, err := client.Execute(context.Background(), fault.Correlation{Call: "bad-slot"}, NewCommand("CLUSTER", "COUNTKEYSINSLOT", "bad")); !errors.Is(err, ErrInput) {
		t.Fatal("malformed slot accepted")
	}
}
