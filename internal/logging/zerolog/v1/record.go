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

package zerolog

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/rs/zerolog"
)

// Record is an immutable process-local structured log snapshot, not a durable
// DTO. Copies share read-only storage. Inspection explicitly reveals selected
// log content, unlike ordinary formatting. Context is passed separately to sinks.
type Record struct {
	private
	data *recordData
}
type recordData struct {
	time        time.Time
	pc          uintptr
	caller      Caller
	level       Level
	message     string
	source      resource.Info
	correlation fault.Correlation
	attributes  []slog.Attr
	json        string
}

// Caller is copied business ingress information, without a native SDK object.
// PC is the adjusted instruction PC; Record.PC preserves the original return PC.
type Caller struct {
	Defined  bool
	PC       uintptr
	File     string
	Line     int
	Function string
}

// Time returns the observed UTC event time, or zero for an absent record.
func (record Record) Time() time.Time {
	if record.data == nil {
		return time.Time{}
	}
	return record.data.time
}

// PC returns the original caller-supplied return PC, or zero when absent.
func (record Record) PC() uintptr {
	if record.data == nil {
		return 0
	}
	return record.data.pc
}

// Caller returns the resolved caller when enabled and supplied, or zero.
func (record Record) Caller() Caller {
	if record.data == nil {
		return Caller{}
	}
	return record.data.caller
}

// Level returns severity, or the empty value for an absent record.
func (record Record) Level() Level {
	if record.data == nil {
		return ""
	}
	return record.data.level
}

// Message deliberately exposes log content; absent records return an empty string.
func (record Record) Message() string {
	if record.data == nil {
		return ""
	}
	return record.data.message
}

// Source returns independent original resource metadata, not a borrowing alias.
func (record Record) Source() resource.Info {
	if record.data == nil {
		return resource.Info{}
	}
	return resource.Info{Scope: record.data.source.Scope, Configuration: record.data.source.Configuration.Clone()}
}

// Correlation returns the frozen technical association; absence returns zero.
func (record Record) Correlation() fault.Correlation {
	if record.data == nil {
		return fault.Correlation{}
	}
	return record.data.correlation
}

// AttributesCopy returns independent attribute/slice storage. Replace attributes
// with native constructors to edit them; slog group values remain immutable.
func (record Record) AttributesCopy() []slog.Attr {
	if record.data == nil {
		return nil
	}
	return copyAttributes(record.data.attributes)
}

// JSONCopy copies the complete encoded JSON line; an absent record returns nil.
func (record Record) JSONCopy() []byte {
	if record.data == nil {
		return nil
	}
	return []byte(record.data.json)
}

func copyAttributes(attributes []slog.Attr) []slog.Attr {
	result := make([]slog.Attr, len(attributes))
	for index, attr := range attributes {
		attr.Key = strings.Clone(attr.Key)
		if attr.Value.Kind() == slog.KindGroup {
			attr.Value = slog.GroupValue(copyAttributes(attr.Value.Group())...)
		} else if attr.Value.Kind() == slog.KindTime {
			attr.Value = slog.TimeValue(attr.Value.Time().UTC().Round(0))
		} else if attr.Value.Kind() == slog.KindString {
			attr.Value = slog.StringValue(strings.Clone(attr.Value.String()))
		} else if attr.Value.Kind() == slog.KindAny {
			if value, ok := attr.Value.Any().(Value); ok {
				attr.Value = slog.AnyValue(copyValue(value))
			}
		}
		result[index] = attr
	}
	return result
}

func validateRecord(level Level, message string, attributes []slog.Attr, limit int) error {
	if level.rank() < 0 {
		return failure(ErrInput, "record")
	}
	remaining := limit - len(message)
	if remaining < 0 {
		return failure(ErrLimit, "record")
	}
	if !utf8.ValidString(message) {
		return failure(ErrInput, "record")
	}
	_, err := validateAttributes(attributes, remaining)
	return err
}

func validateAttributes(attributes []slog.Attr, limit int) (int, error) {
	if limit < 0 || limit > 1<<20 {
		return 0, failure(ErrLimit, "attributes")
	}
	budget := dataBudget{bytes: limit, nodes: MaxNodes}
	var visit func([]slog.Attr, int) error
	visit = func(attrs []slog.Attr, depth int) error {
		if depth > MaxDepth || len(attrs) > MaxAttributes-budget.attributes {
			return failure(ErrLimit, "attributes")
		}
		names := make(map[string]bool, len(attrs))
		for _, attr := range attrs {
			if names[attr.Key] {
				return failure(ErrInput, "attribute-key")
			}
			if err := budget.key(attr.Key); err != nil {
				return err
			}
			names[attr.Key] = true
			if err := budget.take(0, 1); err != nil {
				return err
			}
			switch attr.Value.Kind() {
			case slog.KindString:
				value := attr.Value.String()
				if err := budget.take(len(value), 0); err != nil {
					return err
				}
				if !utf8.ValidString(value) {
					return failure(ErrInput, "attribute-string")
				}
			case slog.KindBool, slog.KindInt64, slog.KindUint64, slog.KindDuration:
			case slog.KindFloat64:
				value := attr.Value.Float64()
				if !finite(value) {
					return failure(ErrInput, "attribute-number")
				}
			case slog.KindTime:
				if !validTime(attr.Value.Time()) {
					return failure(ErrInput, "attribute-time")
				}
			case slog.KindGroup:
				if depth > 1 && len(attr.Value.Group()) == 0 {
					return failure(ErrUnsupported, "noncanonical-group")
				}
				if err := visit(attr.Value.Group(), depth+1); err != nil {
					return err
				}
			case slog.KindAny:
				switch value := attr.Value.Any().(type) {
				case nil:
				case Value:
					budget.nodes++ // The closed value includes this attribute's root node.
					if err := validateValue(value, &budget, depth); err != nil {
						return err
					}
				default:
					return failure(ErrUnsupported, "attribute-value")
				}
			default:
				return failure(ErrUnsupported, "attribute-value")
			}
		}
		return nil
	}
	err := visit(attributes, 1)
	return limit - budget.bytes, err
}

type capture struct {
	limit int
	data  string
	err   error
}

func (writer *capture) Write(data []byte) (int, error) {
	switch {
	case len(data) > writer.limit:
		writer.err = failure(ErrLimit, "encoded-record")
	case !json.Valid(data):
		writer.err = failure(ErrUnsupported, "native-encoding")
	default:
		writer.data = string(data)
	}
	// SDK error callbacks/stderr are process-global. Encoding goes only to this
	// bounded private capture; actual sink errors belong to per-call evidence.
	return len(data), nil
}
func nativeProbe() error {
	writer := &capture{limit: 64}
	logger := sdk.New(writer)
	event := logger.Log()
	if !event.Enabled() {
		return failure(ErrUnsupported, "native-global-level")
	}
	event.Str("encoding", "json").Send()
	return writer.err
}

func encodeRecord(ctx context.Context, data *recordData, limit int) error {
	writer := &capture{limit: limit}
	logger := sdk.New(writer)
	event := logger.Log()
	if !event.Enabled() {
		return failure(ErrUnsupported, "native-global-level")
	}
	event.Ctx(ctx)
	if !data.time.IsZero() {
		event.Str("time", data.time.Format(time.RFC3339Nano))
	}
	event.Str("level", string(data.level)).Str("message", data.message)
	if data.caller.Defined {
		caller := event.CreateDict()
		caller.Str("file", data.caller.File).Int("line", data.caller.Line).Str("function", data.caller.Function)
		event.Dict("caller", caller)
	}
	source := event.CreateDict()
	source.Str("provider", ProviderID).Str("scope", data.source.Scope).
		Str("source", data.source.Configuration.Identity.Name).Str("revision", data.source.Configuration.Revision)
	event.Dict("resource", source)
	correlation := event.CreateDict()
	correlation.Str("call", data.correlation.Call).Str("parent", data.correlation.Parent).Str("owner", data.correlation.Owner)
	event.Dict("correlation", correlation)
	attrs := event.CreateDict()
	appendAttributes(attrs, data.attributes)
	event.Dict("attributes", attrs).Send()
	data.json = writer.data
	return writer.err
}
func appendAttributes(event *sdk.Event, attributes []slog.Attr) {
	for _, attr := range attributes {
		switch attr.Value.Kind() {
		case slog.KindString:
			event.Str(attr.Key, attr.Value.String())
		case slog.KindBool:
			event.Bool(attr.Key, attr.Value.Bool())
		case slog.KindInt64:
			event.Int64(attr.Key, attr.Value.Int64())
		case slog.KindUint64:
			event.Uint64(attr.Key, attr.Value.Uint64())
		case slog.KindFloat64:
			event.RawJSON(attr.Key, []byte(strconv.FormatFloat(attr.Value.Float64(), 'g', -1, 64)))
		case slog.KindDuration:
			event.Int64(attr.Key, int64(attr.Value.Duration()))
		case slog.KindTime:
			event.Str(attr.Key, attr.Value.Time().Format(time.RFC3339Nano))
		case slog.KindAny:
			if value, ok := attr.Value.Any().(Value); ok {
				appendValue(event, attr.Key, value)
			} else {
				event.RawJSON(attr.Key, []byte("null"))
			}
		case slog.KindGroup:
			group := event.CreateDict()
			appendAttributes(group, attr.Value.Group())
			event.Dict(attr.Key, group)
		}
	}
}
