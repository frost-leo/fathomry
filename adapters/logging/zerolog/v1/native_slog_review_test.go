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
	"encoding/json"
	"io"
	"log/slog"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	zerolog "github.com/frost-leo/fathomry/adapters/logging/zerolog/v1"
	sdk "github.com/rs/zerolog"
)

type slogReviewRecords struct{ records []zerolog.Record }

func (sink *slogReviewRecords) WriteRecord(_ context.Context, record zerolog.Record) error {
	sink.records = append(sink.records, record)
	return nil
}

func TestNativeSlogChronologyWitnessAndPublicGatewayControl(t *testing.T) {
	stamp := time.Date(2023, 4, 5, 6, 7, 8, 901, time.UTC)
	var pcs [1]uintptr
	runtime.Callers(1, pcs[:])
	record := slog.NewRecord(stamp, slog.LevelInfo, "chronology", pcs[0])
	record.AddAttrs(slog.Int("after", 3), slog.Group("", slog.String("inline", "value")))
	derive := func(handler slog.Handler) slog.Handler {
		return handler.WithAttrs([]slog.Attr{slog.Int("before", 1)}).
			WithGroup("outer").WithAttrs([]slog.Attr{slog.Int("middle", 2)}).WithGroup("inner")
	}
	decode := func(data []byte) map[string]any {
		t.Helper()
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		var value map[string]any
		if err := decoder.Decode(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	var nativeBytes, standardBytes bytes.Buffer
	if err := derive(sdk.NewSlogHandler(sdk.New(&nativeBytes))).Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if err := derive(slog.NewJSONHandler(&standardBytes, nil)).Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	native, standard := decode(nativeBytes.Bytes()), decode(standardBytes.Bytes())
	if _, exists := native["before"]; exists || native["outer.inner.before"] != json.Number("1") || native["outer.inner.middle"] != json.Number("2") {
		t.Fatal("selected native chronology limitation no longer reproduced; re-evaluate the controlled comparison")
	}
	delete(standard, "time")
	delete(standard, "level")
	delete(standard, "msg")
	if !reflect.DeepEqual(standard, map[string]any{
		"before": json.Number("1"),
		"outer":  map[string]any{"middle": json.Number("2"), "inner": map[string]any{"after": json.Number("3"), "inline": "value"}},
	}) {
		t.Fatal("standard control did not retain WithAttrs/WithGroup chronology", standard)
	}

	local := new(memoryWriter)
	records := new(slogReviewRecords)
	owner, _ := openPublic(t, zerolog.Settings{Name: "slog-review", Version: 1, Caller: ptr(true), Sinks: []zerolog.Sink{
		{Name: "local", Kind: "writer"}, {Name: "record", Kind: "record"},
	}}, zerolog.Dependencies{Writers: map[string]io.Writer{"local": local}, Records: map[string]zerolog.RecordWriter{"record": records}})
	gateway, err := owner.Client().Slog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := gateway.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := derive(gateway).Handle(canceled, record); err != nil {
		t.Fatal("safe ingress confused association cancellation with its owned lifetime", err)
	}
	actual := decode(local.data())
	if !reflect.DeepEqual(actual["attributes"], standard) {
		t.Fatal("public gateway adopted native chronology defect", actual["attributes"], standard)
	}
	if actual["time"] != stamp.Format(time.RFC3339Nano) {
		t.Fatal("wrapper replaced original record time")
	}
	if len(records.records) != 1 || records.records[0].PC() != pcs[0] || !records.records[0].Time().Equal(stamp) {
		t.Fatal("record ingress lost original time/return PC")
	}
	caller := records.records[0].Caller()
	if !caller.Defined || !strings.HasSuffix(caller.Function, "TestNativeSlogChronologyWitnessAndPublicGatewayControl") || !strings.HasSuffix(caller.File, "native_slog_review_test.go") {
		t.Fatal("caller refers to gateway wrapper instead of original business PC")
	}
	if err := gateway.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
