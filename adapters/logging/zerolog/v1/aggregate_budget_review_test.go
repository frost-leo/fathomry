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

package zerolog_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	zerolog "github.com/frost-leo/fathomry/adapters/logging/zerolog/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
)

func measureAggregateAllocations(run func()) uint64 {
	runtime.GC()
	previous := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(previous)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	run()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

func TestAggregateClosedPreparationStaysInsideDeclaredWorkBudget(t *testing.T) {
	const limit = 1 << 20
	message := strings.Repeat("a", limit-1024)
	binary := bytes.Repeat([]byte{0x7f}, limit/2-1024)
	var occurrences []error
	for index := 0; index < 16; index++ {
		occurrence, err := failure.New(zerolog.Definitions()[0], failure.Location{Operation: "aggregate", Instance: fmt.Sprint(index)})
		if err != nil {
			t.Fatal(err)
		}
		occurrences = append(occurrences, occurrence)
	}
	joined := errors.Join(occurrences...)
	for _, test := range []struct {
		name      string
		attribute func(string) slog.Attr
	}{
		{"string", func(key string) slog.Attr { return zerolog.Attribute(key, logging.String(message)) }},
		{"binary", func(key string) slog.Attr { return zerolog.Attribute(key, logging.Binary(binary)) }},
		{"byte-string", func(key string) slog.Attr { return zerolog.Attribute(key, logging.ByteString(binary)) }},
		{"nested-array", func(key string) slog.Attr {
			return zerolog.Attribute(key, logging.Array(logging.Array(logging.Binary(binary))))
		}},
		{"nested-group", func(key string) slog.Attr {
			return zerolog.Attribute(key, logging.Group(logging.Field{Key: "child", Value: logging.String(message)}))
		}},
		{"safe-errors", func(key string) slog.Attr { return slog.Any(key, joined) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			writer := new(memoryWriter)
			settings := zerolog.Settings{Name: "aggregate", Version: 1, MaxRecordBytes: ptr(limit), Sinks: []zerolog.Sink{{Name: "out", Kind: "writer"}}}
			policy, err := zerolog.Recommend(settings)
			if err != nil {
				t.Fatal(err)
			}
			owner, deps := openPublic(t, settings, zerolog.Dependencies{Writers: map[string]io.Writer{"out": writer}})
			attrs := make([]slog.Attr, 64)
			for index := range attrs {
				attrs[index] = test.attribute(fmt.Sprintf("field-%02d", index))
			}
			var receipt *adapters.Receipt[zerolog.Result]
			var submission error
			allocated := measureAggregateAllocations(func() {
				receipt, submission = owner.Client().Log(context.Background(), zerolog.Info, "aggregate", attrs...)
			})
			if allocated > uint64(policy.Budget.WorkBytes) {
				t.Fatalf("rejected aggregate allocated %d bytes beyond declared %d-byte work envelope", allocated, policy.Budget.WorkBytes)
			}
			if receipt == nil {
				t.Fatal("missing admitted preparation evidence", submission)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			snapshot, err := receipt.WaitReleased(ctx)
			if err != nil {
				t.Fatal(err)
			}
			outcome := errors.Join(submission, snapshot.Err())
			if outcome == nil || !errors.Is(outcome, logging.ErrLimit) && !errors.Is(outcome, zerolog.ErrLimit) {
				t.Fatal("aggregate limit was not observable", outcome)
			}
			if len(writer.data()) != 0 {
				t.Fatal("rejected aggregate emitted partial data")
			}
			delivery, err := deps.Evidence.NextReleased(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := delivery.Ack(); err != nil {
				t.Fatal(err)
			}
			stats, err := deps.Runtime.Inspect()
			if err != nil || stats.Active != 1 {
				t.Fatal("preparation retained work allowance", err, stats.Active)
			}
			evidence, err := deps.Evidence.Inspect()
			if err != nil || evidence.Outstanding != 2 {
				t.Fatal("preparation retained independent evidence", err, evidence.Outstanding)
			}
			accepted, err := owner.Client().Log(context.Background(), zerolog.Info, "next", zerolog.Attribute("value", logging.Array(logging.Int64((1<<53)+1), logging.Null())))
			_, outcome = receiptResult(t, deps, accepted, err)
			if outcome != nil {
				t.Fatal("aggregate rejection damaged next independent event", outcome)
			}
		})
	}
}

func TestAggregatePreflightPreservesExactLegacyScalarInputCharge(t *testing.T) {
	limit := 64 * (32 + 3)
	writer := new(memoryWriter)
	owner, deps := openPublic(t, zerolog.Settings{Name: "legacy-charge", Version: 1, MaxRecordBytes: &limit, Sinks: []zerolog.Sink{{Name: "out", Kind: "writer"}}}, zerolog.Dependencies{Writers: map[string]io.Writer{"out": writer}})
	attrs := make([]slog.Attr, 64)
	for index := range attrs {
		attrs[index] = slog.Int(fmt.Sprintf("k%02d", index), index)
	}
	receipt, err := owner.Client().Log(context.Background(), zerolog.Info, "", attrs...)
	value, outcome := receiptResult(t, deps, receipt, err)
	if outcome != nil || !value.SinksCopy()[0].Accepted || len(writer.data()) == 0 {
		t.Fatal("necessary copy bounds narrowed legacy scalar domain", outcome)
	}
}

func TestAggregateOversizedInvalidClosedTextRefusesByLength(t *testing.T) {
	for _, value := range []logging.Value{
		logging.String(strings.Repeat("a", 4096) + "\xff"),
		logging.ByteString(append(bytes.Repeat([]byte{'a'}, 4096), 0xff)),
		logging.Array(logging.String(strings.Repeat("a", 4096) + "\xff")),
	} {
		writer := new(memoryWriter)
		owner, deps := openPublic(t, zerolog.Settings{Name: "length-first", Version: 1, MaxRecordBytes: ptr(1024), Sinks: []zerolog.Sink{{Name: "out", Kind: "writer"}}}, zerolog.Dependencies{Writers: map[string]io.Writer{"out": writer}})
		receipt, submission := owner.Client().Log(context.Background(), zerolog.Info, "", zerolog.Attribute("value", value))
		if receipt == nil {
			t.Fatal("missing bounded validation evidence", submission)
		}
		snapshot, err := receipt.WaitReleased(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		outcome := errors.Join(submission, snapshot.Err())
		if !errors.Is(outcome, logging.ErrLimit) && !errors.Is(outcome, zerolog.ErrLimit) {
			t.Fatal("oversized closed payload reached semantic scanning before byte refusal", outcome)
		}
		if len(writer.data()) != 0 {
			t.Fatal("invalid bounded text reached output")
		}
		delivery, err := deps.Evidence.NextReleased(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Ack(); err != nil {
			t.Fatal(err)
		}
	}
}
