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
	"github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/redis/go-redis/v9"
	"testing"
)

func TestPreparedResolvedBudgetAndLifetime(t *testing.T) {
	options := testOptions("127.0.0.1:1")
	prepared, err := PrepareV1(options, nil, resource.Layer{Kind: resource.Local, Content: []byte("max_active: 2\nmax_commands: 3\nmax_reply_bytes: 2048\nmax_idle_time_ns: 0\nexperimental_cache: true\ncache_bytes: 4096\n")})
	if err != nil {
		t.Fatal(err)
	}
	selected := prepared.Selection()
	assembly, err := resource.Assemble(context.Background(), context.Background(), "resolved", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(context.Background())
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		t.Fatal(err)
	}
	meta := prepared.Metadata()
	value := source.owner.settings
	if meta != value.metadata() || meta.Limits.Active != 2 || meta.MaxCommands != 3 ||
		meta.WorkBytes != value.reservation() || meta.EvidenceBytes != value.evidenceReservation() ||
		meta.SourceBytes <= value.CacheBytes || source.owner.native.(*sdk.Client).Options().ConnMaxIdleTime != -1 {
		t.Fatal("resolved metadata and constructed source diverged")
	}
	options.Addrs[0] = "127.0.0.1:2"
	if source.owner.settings.Addrs[0] != "127.0.0.1:1" {
		t.Fatal("prepared configuration aliased")
	}
	if _, err := PrepareV1(options, &Password{}); err == nil {
		t.Fatal("invalid credential handle accepted")
	}
}

func TestReplyPresenceIsNotZeroValue(t *testing.T) {
	limits := defaults(testOptions("127.0.0.1:1"))
	limits.MaxReplyElements = 16
	for _, test := range []struct {
		name             string
		raw              any
		err              error
		entered, present bool
	}{
		{"null", nil, sdk.Nil, true, true},
		{"empty", "", nil, true, true},
		{"zero", int64(0), nil, true, true},
		{"empty-array", []any{}, nil, true, true},
		{"missing", unobservedReply, context.DeadlineExceeded, true, false},
		{"not-entered", nil, nil, false, false},
		{"discarded", make([]any, 17), nil, true, false},
		{"invalid", int32(9), nil, true, false},
		{"error", nil, errors.New("server or transport error"), true, false},
		{"partial-with-error", []any{"observed-prefix"}, context.DeadlineExceeded, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cmd := sdk.NewCmd(context.Background(), "GET", "owned")
			cmd.SetVal(test.raw)
			cmd.SetErr(test.err)
			result, _ := collect(context.Background(), []*sdk.Cmd{cmd}, test.entered, false, limits, test.err)
			if result.Commands()[0].HasValue() != test.present {
				t.Fatal("decoded value presence lost")
			}
		})
	}
}
