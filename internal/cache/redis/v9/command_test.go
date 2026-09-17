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
	"fmt"
	"math/big"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/redis/go-redis/v9"
)

func TestCommandsEvidenceAndAliasing(t *testing.T) {
	var writes atomic.Int32
	address := peer(t, func(args []string) string {
		switch strings.ToUpper(args[0]) {
		case "SET":
			writes.Add(1)
			return "+OK\r\n"
		case "GET":
			switch args[1] {
			case "missing":
				return "$-1\r\n"
			case "empty":
				return "$0\r\n\r\n"
			case "binary":
				return "$4\r\na\x00\xffb\r\n"
			default:
				return "-WRONGTYPE private-payload-canary\r\n"
			}
		default:
			return ":0\r\n"
		}
	})
	options := testOptions(address)
	client, assembly, inbox, selected := bindTest(t, options, 16)
	args := []string{"SET", "owned", "before"}
	command := NewCommand(args...)
	args[2] = "after"
	if command.args[2] != "before" {
		t.Fatal("input aliased")
	}
	receipt, err := client.Pipeline(context.Background(), fault.Correlation{Call: "mixed", Owner: "item"},
		command, NewCommand("GET", "missing"), NewCommand("GET", "empty"), NewCommand("GET", "binary"), NewCommand("GET", "wrong"))
	got := resolved(t, receipt, err)
	if !errors.Is(got.Err(), sdk.Nil) || writes.Load() != 1 {
		t.Fatal("native nil or effect missing")
	}
	replies := got.Outcome.Value.Commands()
	if len(replies) != 5 || replies[0].State() != Replied || replies[1].Value().Kind() != Null {
		t.Fatal("per-command evidence")
	}
	text, ok := replies[2].Value().Text()
	if !ok || text != "" {
		t.Fatal("empty conflated with missing")
	}
	data, ok := replies[3].Value().Bytes()
	if !ok || string(data) != "a\x00\xffb" {
		t.Fatal("binary changed")
	}
	data[0] = 'z'
	replies[3] = Reply{}
	original := got.Outcome.Value.Commands()
	again, _ := original[3].Value().Text()
	if again != "a\x00\xffb" {
		t.Fatal("result aliased")
	}
	var server sdk.Error
	if !errors.As(original[4].Err(), &server) {
		t.Fatal("native server cause missing")
	}
	_, source, err := resource.Bind(assembly, selected)
	if err != nil {
		t.Fatal(err)
	}
	conformance.Result(t, got, conformance.Expected[Result]{
		Context: fault.Context{Provider: ProviderID, Source: "cache", Scope: "redis-test", Operation: "pipeline", Correlation: fault.Correlation{Call: "mixed", Owner: "item"}},
		Source:  source, Limits: LimitsV1(options), Shape: invocation.Finite, Present: true, Final: true, Released: true,
		Primary: sdk.Nil, Attempts: invocation.Attempts{Observed: 1}, Value: func(t testing.TB, value Result) {
			if len(value.Commands()) != 5 {
				t.Error("oracle count")
			}
		},
	})
	for _, value := range []any{client, command, got.Outcome.Value, original[4].Err(), options} {
		for _, format := range []string{"%v", "%+v", "%#v"} {
			if strings.Contains(fmt.Sprintf(format, value), "private-payload-canary") {
				t.Fatal("privacy leak")
			}
		}
	}
	drain(t, inbox)
}

func TestUnknownMutationAndReplyBounds(t *testing.T) {
	var effects atomic.Int32
	address := peer(t, func(args []string) string {
		if strings.EqualFold(args[0], "incr") {
			effects.Add(1)
			return ""
		}
		return "$1073741824\r\n"
	})
	client, _, inbox, _ := bindTest(t, testOptions(address), 4)
	got := executeTest(t, client, "lost", "INCR", "owned")
	if got.Err() == nil || got.Outcome.Value.Commands()[0].State() != Unknown || effects.Load() != 1 {
		t.Fatal("lost acknowledgement invented certainty or retry")
	}
	bounded := executeTest(t, client, "oversized", "GET", "owned")
	if !errors.Is(bounded.Err(), ErrLimit) || bounded.Outcome.Value.Commands()[0].State() != Unknown {
		var inspect func(error)
		inspect = func(err error) {
			t.Logf("cause type=%T", err)
			if children, ok := err.(interface{ Unwrap() []error }); ok {
				for _, child := range children.Unwrap() {
					inspect(child)
				}
			}
			if child := errors.Unwrap(err); child != nil {
				inspect(child)
			}
		}
		inspect(bounded.Err())
		t.Fatalf("native bound evidence: error=%v limit=%t state=%d", bounded.Err(), errors.Is(bounded.Err(), ErrLimit), bounded.Outcome.Value.Commands()[0].State())
	}
	drain(t, inbox)
}

func TestIndependentEvidenceRejectsBeforeNativeEntry(t *testing.T) {
	var entries atomic.Int32
	address := peer(t, func([]string) string { entries.Add(1); return "+OK\r\n" })
	client, _, inbox, _ := bindTest(t, testOptions(address), 1)
	got := executeTest(t, client, "first", "SET", "owned", "value")
	if got.Err() != nil {
		t.Fatal(got.Err())
	}
	_, err := client.Execute(context.Background(), fault.Correlation{Call: "second"}, NewCommand("SET", "owned", "other"))
	if !errors.Is(err, invocation.ErrEvidence) || entries.Load() != 1 {
		t.Fatal("saturated evidence permitted native work")
	}
	drain(t, inbox)
}

func TestAggregateReplyBoundsAndCacheProvenance(t *testing.T) {
	settings := defaults(testOptions("127.0.0.1:1"))
	settings.MaxReplyElements = 16
	ctx := context.Background()
	if err := checkReply((*big.Int)(nil), 1024, 16); !errors.Is(err, ErrProtocol) {
		t.Fatal("typed nil decoder value was not refused")
	}
	aggregate := sdk.NewCmd(ctx, "KEYS", "*")
	aggregate.SetVal(make([]any, 17))
	result, err := collect(ctx, []*sdk.Cmd{aggregate}, true, false, settings, nil)
	if !errors.Is(err, ErrLimit) || result.Commands()[0].State() != Replied || !errors.Is(result.Commands()[0].Err(), ErrLimit) {
		t.Fatal("SDK aggregate bypassed retained-result bounds or erased reply evidence")
	}
	get := sdk.NewCmd(ctx, "GET", "missing")
	get.SetErr(sdk.Nil)
	set := sdk.NewCmd(ctx, "SET", "key", "value")
	set.SetVal("OK")
	result, err = collect(ctx, []*sdk.Cmd{get, set}, true, true, settings, nil)
	if err != nil || result.Commands()[0].State() != CacheOrReply || result.Commands()[1].State() != Replied {
		t.Fatal("cached nil or confirmed write provenance changed")
	}
}

func TestMalformedBigIntegerTerminatesWithoutLeakingEvidence(t *testing.T) {
	address := peer(t, func([]string) string { return "(-\r\n" })
	client, _, inbox, _ := bindTest(t, testOptions(address), 2)
	got := executeTest(t, client, "bad-bigint", "GET", "owned")
	if !errors.Is(got.Err(), ErrProtocol) || !got.Released {
		t.Fatal("malformed native bigint did not end as owned protocol failure")
	}
	drain(t, inbox)
}

func TestInitializationFailureIsNotACommandReply(t *testing.T) {
	for _, shape := range []string{"finite", "pipeline", "transaction", "automatic"} {
		t.Run(shape, func(t *testing.T) {
			var application atomic.Int32
			address := peer(t, func(args []string) string {
				if strings.EqualFold(args[0], "SELECT") {
					return "-ERR DB index out of range\r\n"
				}
				application.Add(1)
				return "+OK\r\n"
			})
			options := testOptions(address)
			options.DB = 65535
			options.ExperimentalAutoPipeline = true
			client, _, inbox, _ := bindTest(t, options, 4)
			check := func(got invocation.Result[Result]) {
				t.Helper()
				if got.Err() == nil || !got.Released || application.Load() != 0 {
					t.Fatal("initialization refusal not preserved")
				}
				for _, reply := range got.Outcome.Value.Commands() {
					if reply.State() == Replied || reply.Err() == nil {
						t.Fatal("unexecuted command presented as successful reply")
					}
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			switch shape {
			case "finite":
				receipt, err := client.Execute(ctx, fault.Correlation{Call: "finite"}, NewCommand("GET", "owned"))
				check(resolved(t, receipt, err))
			case "pipeline":
				receipt, err := client.Pipeline(ctx, fault.Correlation{Call: "pipeline"}, NewCommand("SET", "owned", "value"), NewCommand("GET", "owned"))
				check(resolved(t, receipt, err))
			case "automatic":
				receipt, err := client.Submit(ctx, fault.Correlation{Call: "automatic"}, NewCommand("GET", "owned"))
				check(resolved(t, receipt, err))
			case "transaction":
				receipt, err := client.Dedicated(ctx, context.Background(), fault.Correlation{Call: "parent"}, "owned", func(ctx context.Context, session *Session) error {
					child, err := session.Transaction(ctx, fault.Correlation{Call: "child", Parent: "parent"}, NewCommand("SET", "owned", "value"))
					result := resolved(t, child, err)
					check(result)
					return result.Err()
				})
				result := resolved(t, receipt, err)
				if result.Err() == nil {
					t.Fatal("root error lost")
				}
			}
			drain(t, inbox)
		})
	}
}
