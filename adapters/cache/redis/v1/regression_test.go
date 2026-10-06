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
	"net"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	sdk "github.com/redis/go-redis/v9"
)

func hasServerPrefix(err error, prefix string) bool {
	if err == nil {
		return false
	}
	if server, ok := err.(sdk.Error); ok && strings.HasPrefix(server.Error(), prefix) {
		return true
	}
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		for _, cause := range wrapped.Unwrap() {
			if hasServerPrefix(cause, prefix) {
				return true
			}
		}
	case interface{ Unwrap() error }:
		return hasServerPrefix(wrapped.Unwrap(), prefix)
	}
	return false
}

func TestNewStreamCommandsUseOrdinaryMessagingGrant(t *testing.T) {
	address := peer(t, func(_ net.Conn, args []string) string {
		if strings.EqualFold(args[0], "XNACK") {
			return ":1\r\n"
		}
		return "+OK\r\n"
	})
	settings := testSettings(address)
	settings.Commands = append(settings.Commands, "XNACK", "XCFGSET")
	owner, deps := openTest(t, settings)
	for _, args := range [][]string{{"XNACK", "owned", "group", "SILENT", "IDS", "1", "1-0"}, {"XCFGSET", "owned", "IDMP-DURATION", "60"}} {
		value, err := owner.Client().Messaging().Execute(testContext(t), command(t, Messaging, args...))
		if err != nil || value.Replies()[0].Capability() != Messaging || !value.Replies()[0].HasValue() {
			t.Fatal("ordinary messaging command path lost", err)
		}
		ack(t, deps.Evidence, 1)
	}
}

func TestCompleteStreamsClassification(t *testing.T) {
	for _, args := range [][]string{
		{"XNACK", "owned", "group", "SILENT", "IDS", "1", "1-0"},
		{"XCFGSET", "owned", "IDMP-DURATION", "60"},
	} {
		t.Run(args[0], func(t *testing.T) {
			if _, err := NewCommand(Cache, args...); !errors.Is(err, ErrInput) {
				t.Fatal("intrinsic Stream command accepted with Cache semantic owner")
			}
		})
	}
}

func TestCallbackWrappedPublicError(t *testing.T) {
	address := peer(t, func(_ net.Conn, _ []string) string { return "+OK\r\n" })
	owner, deps := openTest(t, testSettings(address))
	original, _ := failure.New(adapters.Definitions()[0], failure.Location{Operation: "synthetic"})
	_, err := owner.Client().Messaging().Dedicated(testContext(t), context.Background(), "owned", func(context.Context, *Session) error {
		return fmt.Errorf("callback context: %w", original)
	})
	ack(t, deps.Evidence, 1)
	var occurrence failure.Occurrence
	if !errors.As(err, &occurrence) {
		t.Fatal("no public occurrence")
	}
	if occurrence.Failure().Diagnostic().Definition.Code != original.Diagnostic().Definition.Code {
		t.Fatal("ordinary wrapped public failure was relabeled as Redis command failure")
	}
}

func TestPubSubArrayPayloadIsNotEmptyText(t *testing.T) {
	address := peer(t, func(_ net.Conn, _ []string) string {
		return "*3\r\n$9\r\nsubscribe\r\n$5\r\nowned\r\n:1\r\n" +
			"*3\r\n$7\r\nmessage\r\n$5\r\nowned\r\n*2\r\n$5\r\nfirst\r\n$6\r\nsecond\r\n"
	})
	owner, deps := openTest(t, testSettings(address))
	_, err := owner.Client().Messaging().Subscribe(testContext(t), context.Background(), SubscriptionOptions{Mode: "channel", Channels: []string{"owned"}}, func(ctx context.Context, subscription *Subscription) error {
		_, err := subscription.Receive(ctx)
		if err != nil {
			return err
		}
		ack(t, deps.Evidence, 1)
		message, err := subscription.Receive(ctx)
		ack(t, deps.Evidence, 1)
		if err != nil {
			return nil
		} // Explicit refusal is truthful for a text-only profile.
		payload := message.Replies()[0].Value().Elements()[3]
		if payload.Kind() != Array || len(payload.Elements()) != 2 {
			t.Error("SDK PayloadSlice silently converted to successful empty text")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ack(t, deps.Evidence, 1)
}

func TestMaximumBatchCausePreservation(t *testing.T) {
	const width = 256
	address := peer(t, func(_ net.Conn, args []string) string {
		switch strings.ToUpper(args[0]) {
		case "MULTI":
			return "+OK\r\n"
		case "EXEC":
			return fmt.Sprintf("*%d\r\n", width) + strings.Repeat("$-1\r\n", width)
		default:
			return "+QUEUED\r\n"
		}
	})
	settings := testSettings(address)
	settings.MaxCommands = width
	settings.MaxReplyElements = width + 16
	owner, deps := openTest(t, settings)
	commands := make([]Command, width)
	for index := range commands {
		commands[index] = command(t, Cache, "GET", "missing")
	}
	result, err := owner.Client().Cache().Transaction(testContext(t), context.Background(), "owned", commands...)
	ack(t, deps.Evidence, 2)
	if !IsNull(result.Execution().Err()) {
		t.Fatal("execution control lacks native null causes")
	}
	if !IsNull(err) || !IsNull(result.Lifecycle().Err()) {
		t.Fatal("valid bounded batch's native null causes were discarded by lifecycle error translation budget")
	}
}

func TestAggregateExecAbortSurvivesQueueErrors(t *testing.T) {
	address := peer(t, func(_ net.Conn, args []string) string {
		switch strings.ToUpper(args[0]) {
		case "MULTI":
			return "+OK\r\n"
		case "EXEC":
			return "-EXECABORT Transaction discarded because of previous errors.\r\n"
		default:
			return "-ERR wrong number of arguments\r\n"
		}
	})
	ctx := testContext(t)
	oracle := sdk.NewClient(&sdk.Options{Addr: address, Protocol: 3, MaxRetries: -1, DisableIdentity: true})
	defer oracle.Close()
	pipe := oracle.TxPipeline()
	pipe.Do(ctx, "GET", "owned", "invalid")
	pipe.Do(ctx, "XADD", "owned")
	_, nativeErr := pipe.Exec(ctx)
	if !hasServerPrefix(nativeErr, "EXECABORT") {
		t.Fatal("native control lacks EXECABORT")
	}
	owner, deps := openTest(t, testSettings(address))
	result, err := owner.Client().Cache().Transaction(ctx, context.Background(), "owned",
		command(t, Cache, "GET", "owned", "invalid"), command(t, Messaging, "XADD", "owned"))
	ack(t, deps.Evidence, 2)
	if len(result.Execution().Replies()) != 2 {
		t.Fatal("missing child replies")
	}
	if !hasServerPrefix(err, "EXECABORT") || !hasServerPrefix(result.Execution().Err(), "EXECABORT") || !hasServerPrefix(result.Execution().AggregateError(), "EXECABORT") {
		t.Fatal("public adapter lost distinct native EXECABORT error; only per-command queue errors survived")
	}
}
