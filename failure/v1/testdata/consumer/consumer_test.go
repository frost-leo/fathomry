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

package consumer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/failure/v1"
)

const sourceRejected failure.Condition = "example.source.rejected"
const writePartial failure.Condition = "other.writer.partial"

// Each fixture owns its detail contract. The core neither discovers nor prints it.
type sourceFailure struct {
	core        *failure.Error
	generations []uint64
}

func newSource(generations []uint64, causes ...error) (sourceFailure, error) {
	if len(generations) > 8 {
		return sourceFailure{}, errors.New("source: too many generations")
	}
	core, err := failure.New(sourceRejected, causes...)
	return sourceFailure{core: core, generations: slices.Clone(generations)}, err
}
func (err sourceFailure) Failure() *failure.Error { return err.core }
func (err sourceFailure) Error() string           { return err.core.Error() }
func (err sourceFailure) Unwrap() error           { return err.core }
func (err sourceFailure) Generations() []uint64   { return slices.Clone(err.generations) }
func (err sourceFailure) Format(state fmt.State, verb rune) {
	fmt.Fprintf(state, "%"+string(verb), err.core)
}
func (err *sourceFailure) LogValue() slog.Value {
	if err == nil {
		return slog.StringValue("<nil>")
	}
	return err.core.LogValue()
}
func (err sourceFailure) MarshalJSON() ([]byte, error) { return nil, failure.ErrSerialization }
func (*sourceFailure) UnmarshalJSON([]byte) error      { return failure.ErrSerialization }

type writeFailure struct {
	core     *failure.Error
	accepted []uint64
	private  []byte
}

func newWrite(accepted []uint64, private []byte, causes ...error) (writeFailure, error) {
	if len(accepted) > 8 || len(private) > 64 {
		return writeFailure{}, errors.New("writer: detail limit")
	}
	core, err := failure.New(writePartial, causes...)
	return writeFailure{core: core, accepted: slices.Clone(accepted), private: bytes.Clone(private)}, err
}
func (err writeFailure) Failure() *failure.Error { return err.core }
func (err writeFailure) Error() string           { return err.core.Error() }
func (err writeFailure) Unwrap() error           { return err.core }
func (err writeFailure) Accepted() []uint64      { return slices.Clone(err.accepted) }
func (err writeFailure) PrivateCopy() []byte     { return bytes.Clone(err.private) }
func (err writeFailure) Format(state fmt.State, verb rune) {
	fmt.Fprintf(state, "%"+string(verb), err.core)
}
func (err *writeFailure) LogValue() slog.Value {
	if err == nil {
		return slog.StringValue("<nil>")
	}
	return err.core.LogValue()
}
func (err writeFailure) MarshalJSON() ([]byte, error) { return nil, failure.ErrSerialization }
func (*writeFailure) UnmarshalJSON([]byte) error      { return failure.ErrSerialization }

var (
	_ failure.Occurrence = sourceFailure{}
	_ failure.Occurrence = (*sourceFailure)(nil)
	_ failure.Occurrence = writeFailure{}
	_ failure.Occurrence = (*writeFailure)(nil)
)

// Presentation selects both identity and facts from the supplied extension,
// never an errors.As search through another occurrence.
func present(err error, wording map[failure.Condition]string) string {
	current, ok := failure.Inspect(err)
	if !ok {
		return "operation failed"
	}
	prefix, ok := wording[current.Diagnostic().Condition]
	if !ok {
		return current.Error()
	}
	switch owned := err.(type) {
	case sourceFailure:
		return fmt.Sprintf("%s %v", prefix, owned.Generations())
	case *sourceFailure:
		return fmt.Sprintf("%s %v", prefix, owned.Generations())
	case writeFailure:
		return fmt.Sprintf("%s %v", prefix, owned.Accepted())
	case *writeFailure:
		return fmt.Sprintf("%s %v", prefix, owned.Accepted())
	default:
		return current.Error()
	}
}

func TestIndependentOwners(t *testing.T) {
	generations, accepted := []uint64{17}, []uint64{3, 4}
	private := []byte("consumer_secret_canary")
	source, err := newSource(generations)
	if err != nil {
		t.Fatal(err)
	}
	write, err := newWrite(accepted, private, source)
	if err != nil {
		t.Fatal(err)
	}
	generations[0], accepted[0], private[0] = 99, 99, 'X'
	source.Generations()[0], write.Accepted()[0], write.PrivateCopy()[0] = 88, 88, 'Y'
	if source.Generations()[0] != 17 || write.Accepted()[0] != 3 || string(write.PrivateCopy()) != "consumer_secret_canary" {
		t.Fatal("detail alias")
	}
	for _, value := range []error{source, &source, write, &write} {
		core, ok := failure.Inspect(value)
		if !ok || core != value.(failure.Occurrence).Failure() {
			t.Fatal("direct occurrence lost")
		}
		if !errors.Is(value, core.Diagnostic().Condition) {
			t.Fatal("identity unreachable")
		}
		if errors.Is(value, errors.New(value.Error())) {
			t.Fatal("text used as identity")
		}
		var native *failure.Error
		if !errors.As(value, &native) || native != core {
			t.Fatal("core As lost")
		}
		for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
			if strings.Contains(fmt.Sprintf(format, value), "consumer_secret_canary") {
				t.Fatal("detail formatted")
			}
		}
		var buffer bytes.Buffer
		slog.New(slog.NewJSONHandler(&buffer, nil)).Info("fixture", "err", value)
		if strings.Contains(buffer.String(), "consumer_secret_canary") {
			t.Fatal("detail logged")
		}
		if _, err := json.Marshal(value); !errors.Is(err, failure.ErrSerialization) {
			t.Fatalf("runtime serialized: %v", err)
		}
	}
	var sourceValue sourceFailure
	if !errors.As(write, &sourceValue) || sourceValue.Generations()[0] != 17 {
		t.Fatal("value extension unreachable")
	}
	pointerWrite, _ := newWrite(nil, nil, &source)
	var sourcePointer *sourceFailure
	if !errors.As(pointerWrite, &sourcePointer) || sourcePointer != &source {
		t.Fatal("pointer extension unreachable")
	}
	var missing *writeFailure
	if errors.As(source, &missing) {
		t.Fatal("unrelated type matched")
	}
	if !errors.As(&write, &missing) || missing != &write {
		t.Fatal("writer pointer extension unreachable")
	}
	var writerValue writeFailure
	if !errors.As(write, &writerValue) || writerValue.Accepted()[0] != 3 {
		t.Fatal("writer value extension unreachable")
	}
	var readers sync.WaitGroup
	for range 8 {
		readers.Go(func() {
			for range 100 {
				if source.Generations()[0] != 17 || write.Accepted()[0] != 3 {
					t.Error("concurrent detail changed")
				}
				source.Generations()[0] = 90
				write.PrivateCopy()[0] = 'Z'
				_ = present(write, nil)
			}
		})
	}
	readers.Wait()
	for _, value := range []any{source, &source, write, &write} {
		if _, err := json.Marshal(value); !errors.Is(err, failure.ErrSerialization) {
			t.Fatal("value serialization accepted")
		}
	}
	if err := json.Unmarshal([]byte("{}"), &source); !errors.Is(err, failure.ErrSerialization) {
		t.Fatal("source reconstructed")
	}
	if err := json.Unmarshal([]byte("{}"), &write); !errors.Is(err, failure.ErrSerialization) {
		t.Fatal("writer reconstructed")
	}
	if _, err := newSource(make([]uint64, 9)); err == nil {
		t.Fatal("source detail truncated")
	}
	if _, err := newWrite(make([]uint64, 9), nil); err == nil {
		t.Fatal("write detail truncated")
	}
	if _, err := newWrite(nil, make([]byte, 65)); err == nil {
		t.Fatal("private detail unbounded")
	}
}

func TestSameOccurrencePresentation(t *testing.T) {
	inner, _ := newSource([]uint64{11})
	outer, _ := newSource([]uint64{22}, inner)
	sibling, _ := newSource([]uint64{33})
	words := map[failure.Condition]string{sourceRejected: "Generation rejected", writePartial: "Accepted subset"}
	if got := present(outer, words); got != "Generation rejected [22]" {
		t.Fatal(got)
	}
	if got := present(&inner, words); got != "Generation rejected [11]" {
		t.Fatal(got)
	}
	for _, joined := range []error{errors.Join(outer, sibling), errors.Join(sibling, outer), fmt.Errorf("context: %w", inner)} {
		if _, ok := failure.Inspect(joined); ok {
			t.Fatal("fabricated primary")
		}
		if got := present(joined, words); got != "operation failed" {
			t.Fatal(got)
		}
		if !errors.Is(joined, sourceRejected) {
			t.Fatal("recursive membership lost")
		}
	}
	plain, _ := failure.New(sourceRejected, inner)
	if got := present(plain, words); got != string(sourceRejected) {
		t.Fatal("borrowed descendant detail:", got)
	}
	write, _ := newWrite([]uint64{44}, nil, inner)
	if got := present(write, words); got != "Accepted subset [44]" {
		t.Fatal(got)
	}
	words[sourceRejected] = "Candidate declined"
	if got := present(outer, words); got != "Candidate declined [22]" || !errors.Is(outer, sourceRejected) {
		t.Fatal(got)
	}
	if got := present(outer, nil); got != string(sourceRejected) || outer.Generations()[0] != 22 {
		t.Fatal("fallback changed original")
	}
}

func TestNilAndInvalidExtensions(t *testing.T) {
	valid, _ := failure.New(sourceRejected)
	zero := failure.Error{}
	for _, value := range []error{(*sourceFailure)(nil), (*writeFailure)(nil), sourceFailure{}, writeFailure{}, sourceFailure{core: &zero}} {
		if _, ok := failure.Inspect(value); ok {
			t.Fatal("nil/invalid selected")
		}
		if got := present(value, nil); got != "operation failed" {
			t.Fatal(got)
		}
	}
	for _, value := range []any{(*sourceFailure)(nil), (*writeFailure)(nil)} {
		for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
			if output := fmt.Sprintf(format, value); strings.Contains(output, "PANIC") || strings.Contains(output, "consumer_secret") {
				t.Fatal("unsafe nil formatting", output)
			}
		}
		if got, err := json.Marshal(value); err != nil || string(got) != "null" {
			t.Fatal("nil is not absence")
		}
	}
	for _, result := range []*failure.Error{nil, &zero} {
		invalid := invalidExtension{cause: valid, current: result}
		if _, ok := failure.Inspect(invalid); ok {
			t.Fatal("fallback searched descendant")
		}
		if !errors.Is(invalid, sourceRejected) {
			t.Fatal("recursive cause missing")
		}
	}
}

type invalidExtension struct {
	cause   error
	current *failure.Error
}

func (invalidExtension) Error() string               { return "invalid extension" }
func (err invalidExtension) Failure() *failure.Error { return err.current }
func (err invalidExtension) Unwrap() error           { return err.cause }

func TestNilExtensionLogging(t *testing.T) {
	for _, value := range []slog.LogValuer{(*sourceFailure)(nil), (*writeFailure)(nil), &sourceFailure{}, &writeFailure{}} {
		resolved := slog.AnyValue(value).Resolve()
		if resolved.Kind() != slog.KindString || resolved.String() != "<nil>" {
			t.Errorf("%T: nil/core-nil must resolve to absence, got %s", value, resolved.Kind())
		}
		for _, jsonLog := range []bool{false, true} {
			var buffer bytes.Buffer
			options := &slog.HandlerOptions{ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
				if attr.Key == slog.TimeKey {
					return slog.Attr{}
				}
				return attr
			}}
			var handler slog.Handler = slog.NewTextHandler(&buffer, options)
			expected := "level=INFO msg=nil err=<nil>\n"
			if jsonLog {
				handler = slog.NewJSONHandler(&buffer, options)
				expected = "{\"level\":\"INFO\",\"msg\":\"nil\",\"err\":\"<nil>\"}\n"
			}
			slog.New(handler).Info("nil", "err", value)
			if buffer.String() != expected {
				t.Errorf("%T JSON=%v: nil/core-nil did not log exact absence", value, jsonLog)
			}
		}
	}
	unsafe := slog.AnyValue((*unsafeValueLogger)(nil)).Resolve()
	if unsafe.Kind() != slog.KindAny || !strings.Contains(fmt.Sprint(unsafe.Any()), "LogValue panicked") {
		t.Fatal("rejecting control no longer demonstrates promoted nil LogValue failure")
	}
}

type unsafeValueLogger struct{}

func (unsafeValueLogger) LogValue() slog.Value { return slog.StringValue("not absence") }

func TestExtensionLoggingForms(t *testing.T) {
	source, _ := newSource([]uint64{17})
	write, _ := newWrite([]uint64{3}, []byte("consumer_secret_canary"))
	for _, value := range []error{source, write, &source, &write} {
		_, pointerLogger := value.(slog.LogValuer)
		resolved := slog.AnyValue(value).Resolve()
		if pointerLogger && (resolved.Kind() != slog.KindString || resolved.String() != value.Error()) {
			t.Fatal("pointer logging lost code")
		}
		if !pointerLogger && resolved.Kind() != slog.KindAny {
			t.Fatal("value method set changed")
		}
		for _, jsonLog := range []bool{false, true} {
			var buffer bytes.Buffer
			var handler slog.Handler = slog.NewTextHandler(&buffer, nil)
			if jsonLog {
				handler = slog.NewJSONHandler(&buffer, nil)
			}
			slog.New(handler).Info("fixture", "err", value)
			if !jsonLog {
				if !strings.HasSuffix(buffer.String(), " err="+value.Error()+"\n") {
					t.Fatal("text form lost safe code")
				}
				continue
			}
			var record map[string]any
			if err := json.Unmarshal(buffer.Bytes(), &record); err != nil {
				t.Fatal(err)
			}
			expected := value.Error()
			expectedAddressed := expected
			if !pointerLogger {
				expected = "!ERROR:json: error calling MarshalJSON for type " + fmt.Sprintf("%T", value) + ": " + failure.ErrSerialization.Error()
				// encoding/json may address its copied value before invoking MarshalJSON.
				expectedAddressed = strings.Replace(expected, "for type ", "for type *", 1)
			}
			if record["err"] != expected && record["err"] != expectedAddressed {
				t.Fatalf("%T JSON form: got %q, want %q", value, record["err"], expected)
			}
		}
	}
}
