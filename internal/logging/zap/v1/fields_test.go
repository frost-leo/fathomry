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

package zap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/resource"
	sdk "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestTimeFieldsNormalizeAcrossTimezones(t *testing.T) {
	if zone := os.Getenv("FATHOMRY_ZAP_UTC_CHILD"); zone != "" {
		if time.Local.String() != zone {
			t.Fatal("child timezone was not applied")
		}
		checkTimeFieldOutputs(t)
		t.Log("UTC file and structured time-field oracle returned")
		return
	}
	for _, zone := range []string{"UTC", "Asia/Shanghai", "America/New_York"} {
		t.Run(zone, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0],
				"-test.run=^TestTimeFieldsNormalizeAcrossTimezones$", "-test.v", "-test.timeout=15s")
			command.Env = append(os.Environ(), "TZ="+zone, "FATHOMRY_ZAP_UTC_CHILD="+zone, "GORACE=atexit_sleep_ms=0")
			output, err := command.CombinedOutput()
			if err != nil || ctx.Err() != nil || !bytes.Contains(output, []byte("UTC file and structured time-field oracle returned")) {
				t.Fatalf("timezone contract failed: %v\n%s", err, output)
			}
		})
	}
}

func checkTimeFieldOutputs(t *testing.T) {
	t.Helper()
	inputZone := time.FixedZone("input-zone", -5*60*60)
	current := time.Date(2026, 9, 13, 1, 2, 3, 4, inputZone)
	past := time.Date(1960, 9, 13, 1, 2, 3, 4, inputZone)
	future := time.Date(9999, 9, 13, 1, 2, 3, 4, inputZone)
	cases := []struct {
		name    string
		field   zapcore.Field
		instant time.Time
	}{
		{"ordinary", sdk.Time("event_time", current), current},
		{"past", sdk.Time("event_time", past), past},
		{"full", sdk.Time("event_time", future), future},
		{"nil-location", zapcore.Field{Key: "event_time", Type: zapcore.TimeType, Integer: current.UnixNano()}, current},
		{"full-representable", zapcore.Field{Key: "event_time", Type: zapcore.TimeFullType, Interface: current}, current},
	}
	options := fileOptions(t, false)
	options.Outputs[0].MaxFileBytes = 64 << 10
	sink := &recordingSink{}
	fixture := bindFixture(t, options, sink, 2*len(cases))
	var expected []string
	for _, test := range cases {
		for _, bound := range []bool{false, true} {
			logger, fields := fixture.logger, []zapcore.Field{test.field}
			if bound {
				var err error
				logger, err = logger.With(test.field)
				if err != nil {
					t.Fatal(err)
				}
				logger, err = logger.With(sdk.Bool("derived_again", true))
				if err != nil {
					t.Fatal(err)
				}
				fields = nil
			}
			id := fmt.Sprintf("%s-%t", test.name, bound)
			if result := logResult(t, logger, id, fields...); result.Err() != nil {
				t.Fatal(result.Err())
			}
			want := test.instant.UTC().Format(time.RFC3339Nano)
			expected = append(expected, want)
			record := sink.records[len(sink.records)-1]
			buffer, err := zapcore.NewJSONEncoder(encoderConfig()).EncodeEntry(record.entry, record.fields)
			if err != nil {
				t.Fatal(err)
			}
			var structured map[string]any
			err = json.Unmarshal(buffer.Bytes(), &structured)
			buffer.Free()
			if err != nil {
				t.Fatal(err)
			}
			if structured["event_time"] != want {
				t.Errorf("%s structured UTC: got %q, want %q", id, structured["event_time"], want)
			}
		}
	}
	records := readJSON(t, filepath.Join(options.Outputs[0].Directory, "current.log"))
	if len(records) != len(expected) {
		t.Fatal("file time-field records missing")
	}
	for index, want := range expected {
		if records[index]["event_time"] != want {
			t.Errorf("file record %d UTC: got %q, want %q", index, records[index]["event_time"], want)
		}
	}
}

type hostileValue struct{ called *bool }

func TestNativeNoOpFieldsAreIgnoredWithoutRetainingValues(t *testing.T) {
	called := false
	fields := []zapcore.Field{sdk.Skip(), sdk.Error(nil), {
		Type: zapcore.SkipType, Key: "msg", Interface: hostileValue{&called},
	}}
	frozen, err := freezeFields(fields)
	if err != nil || len(frozen) != 0 || called {
		t.Fatal("native no-op fields executed or retained ignored values")
	}
	sink := &recordingSink{}
	fixture := bindFixture(t, OptionsV1{Name: "logs"}, sink, 1)
	if result := logResult(t, fixture.logger, "no-error", fields...); result.Err() != nil {
		t.Fatal(result.Err())
	}
	if len(sink.records[0].fields) != 6 || called {
		t.Fatal("no-op fields reached the structured sink")
	}
}

func (value hostileValue) String() string { *value.called = true; panic("must not format") }
func (value hostileValue) Error() string  { *value.called = true; panic("must not format") }
func (value hostileValue) MarshalLogObject(zapcore.ObjectEncoder) error {
	*value.called = true
	panic("must not marshal")
}
func TestUnsupportedNativeFieldsNeverExecuteCallbacks(t *testing.T) {
	fixture := bindFixture(t, OptionsV1{Name: "logs"}, &recordingSink{}, 2)
	called := false
	hostile := hostileValue{&called}
	for _, field := range []zapcore.Field{
		{}, sdk.Reflect("object", hostile), sdk.Object("object", hostile), sdk.Stringer("value", hostile), sdk.Error(hostile),
		sdk.Namespace("nested"), sdk.Any("map", map[string]int{"key": 1}), sdk.Ints("array", []int{1}),
		{Key: "time", Type: zapcore.TimeType, Interface: "wrong"}, {Key: "time", Type: zapcore.TimeFullType},
		{Key: "bytes", Type: zapcore.BinaryType, Interface: "wrong"}, {Key: "error", Type: zapcore.ErrorType, Interface: (*fault.Error)(nil)},
		sdk.String("msg", "reserved"), sdk.String("fathomry.call", "spoof"), sdk.Float64("number", math.NaN()),
		sdk.Float32("number", float32(math.Inf(1))), sdk.String("text", string([]byte{255})),
		sdk.ByteString("text", []byte{255}), sdk.String("large", strings.Repeat("x", MaxFieldBytes)),
	} {
		if receipt, err := fixture.logger.Log(context.Background(), fault.Correlation{Call: "rejected"}, zapcore.InfoLevel, "message", field); receipt != nil || err == nil {
			t.Fatal("unsafe native field accepted")
		}
		if _, err := fixture.logger.With(field); err == nil {
			t.Fatal("unsafe bound field accepted")
		}
	}
	if called || fixture.inbox.Usage().Outstanding != 0 {
		t.Fatal("validation invoked callbacks or admitted invalid data")
	}
	for _, severity := range []zapcore.Level{zapcore.DPanicLevel, zapcore.PanicLevel, zapcore.FatalLevel, zapcore.Level(-2), zapcore.Level(127)} {
		if receipt, err := fixture.logger.Log(context.Background(), fault.Correlation{Call: "terminal"}, severity, "message"); receipt != nil || !errors.Is(err, ErrInput) {
			t.Fatal("terminal action accepted")
		}
	}
}

func TestScalarFieldsAreCanonicalCopies(t *testing.T) {
	original := fault.Kind("fixture.error").New(fault.Context{}, errors.New("native-canary"))
	instant := time.Date(9999, 1, 1, 2, 3, 4, 5, time.FixedZone("private-zone", 3600))
	recent := time.Now()
	fields := []zapcore.Field{sdk.String("text", "value"), sdk.Bool("flag", true), sdk.Int("int", 7), sdk.Uint64("uint", ^uint64(0)),
		sdk.Float64("float", 1.25), sdk.Float32("small", 1.5), sdk.Duration("elapsed", 3*time.Millisecond),
		sdk.Time("time", instant), sdk.Time("shorttime", recent), sdk.Binary("blob", []byte{0, 1}), sdk.ByteString("bytes", []byte("value")), sdk.Error(original)}
	frozen, err := freezeFields(fields)
	if err != nil {
		t.Fatal(err)
	}
	fields[0].String = "changed"
	fields[9].Interface.([]byte)[0] = 99
	if frozen[0].String != "value" || frozen[9].Interface.([]byte)[0] != 0 || frozen[11].Interface != original {
		t.Fatal("field copy or native error identity changed")
	}
	buffer, err := zapcore.NewJSONEncoder(encoderConfig()).EncodeEntry(zapcore.Entry{}, frozen)
	if err != nil {
		t.Fatal(err)
	}
	var encoded map[string]any
	err = json.Unmarshal(buffer.Bytes(), &encoded)
	buffer.Free()
	if err != nil {
		t.Fatal(err)
	}
	if encoded["time"] != instant.UTC().Format(time.RFC3339Nano) || encoded["shorttime"] != recent.UTC().Format(time.RFC3339Nano) {
		t.Fatal("native time did not preserve the UTC instant")
	}
	for _, field := range frozen {
		switch field.Type {
		case zapcore.ErrorType, zapcore.BinaryType, zapcore.ByteStringType, zapcore.TimeType, zapcore.TimeFullType:
			continue
		}
		if field.Interface != nil {
			t.Fatal("unused native interface retained")
		}
	}
	fixture := bindFixture(t, OptionsV1{Name: "logs"}, &recordingSink{}, 2)
	derived, err := fixture.logger.With(sdk.String("bound", "value"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := derived.With(sdk.Int("bound", 1)); err == nil {
		t.Fatal("With permitted shadowing")
	}
	if _, err := derived.Log(context.Background(), fault.Correlation{Call: "duplicate"}, zapcore.InfoLevel, "message", sdk.Int("bound", 1)); err == nil {
		t.Fatal("call site permitted shadowing")
	}
	if _, err := derived.Named(strings.Repeat("a", 129)); err == nil {
		t.Fatal("unbounded name accepted")
	}
}

func TestRuntimePrivacyAndFacadeSurfaces(t *testing.T) {
	options := fileOptions(t, false)
	fixture := bindFixture(t, options, &recordingSink{}, 2)
	result := logResult(t, fixture.logger, "private", sdk.String("secret", "payload-canary"))
	source, _, err := resource.Bind(fixture.assembly, fixture.selection)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []struct{ value, target any }{
		{&options, new(OptionsV1)}, {&options.Outputs[0], new(OutputV1)}, {source, new(Source)},
		{fixture.logger, new(Logger)}, {result.Outcome.Value, new(Result)}, {result.Outcome.Value.SinksCopy()[0], new(SinkResult)},
	} {
		conformance.Runtime(t, value.value, value.target, "payload-canary", options.Outputs[0].Directory)
	}
	conformance.Facade(t, fixture.logger, "Log", "Sync", "With", "Named", "Profile", "String", "GoString", "Format", "LogValue", "MarshalJSON", "UnmarshalJSON")
	conformance.Facade(t, &source, "String", "GoString", "Format", "LogValue", "MarshalJSON", "UnmarshalJSON")
	for _, value := range []any{(*Logger)(nil), (*OptionsV1)(nil), (*Result)(nil)} {
		if fmt.Sprintf("%v", value) != "<nil>" {
			t.Fatal("nil fmt fallback changed")
		}
		var output bytes.Buffer
		slog.New(slog.NewJSONHandler(&output, nil)).Info("nil", "value", value)
		if !strings.Contains(output.String(), "zap[restricted]") {
			t.Fatal("nil slog projection changed")
		}
	}
}

func FuzzNativeFieldValidation(f *testing.F) {
	f.Add(uint8(zapcore.StringType), "key", []byte("value"))
	f.Add(uint8(255), "bad", []byte{255})
	f.Fuzz(func(t *testing.T, kind uint8, key string, data []byte) {
		if len(key) > 256 || len(data) > MaxFieldBytes+1 {
			return
		}
		field := zapcore.Field{Key: key, Type: zapcore.FieldType(kind), String: string(data), Interface: data, Integer: 42}
		frozen, err := freezeFields([]zapcore.Field{field})
		if err == nil {
			encoder := zapcore.NewJSONEncoder(encoderConfig())
			buffer, err := encoder.EncodeEntry(zapcore.Entry{}, frozen)
			if err != nil {
				t.Fatal("accepted field failed native encoding")
			}
			buffer.Free()
		}
	})
}
