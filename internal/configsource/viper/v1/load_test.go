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

package viper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"testing/iotest"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	sdk "github.com/spf13/viper"
)

func readerInput(encoding, content string) LoadInput {
	return LoadInput{Options: OptionsV1{Encoding: encoding}, Reader: strings.NewReader(content)}
}

func loadOne(t testing.TB, input LoadInput) *Document {
	t.Helper()
	documents, err := Load(context.Background(), []LoadInput{input})
	if err != nil || len(documents) != 1 {
		t.Fatal("fixture load failed", err)
	}
	return documents[0]
}

func TestFailuresNeverPublishPartialOrStaleBatch(t *testing.T) {
	good := loadOne(t, readerInput("yaml", "value: before"))
	cause := errors.New("private-reader-cause")
	for _, broken := range []LoadInput{
		readerInput("yaml", "{"),
		readerInput("json", "{"),
		{Options: OptionsV1{Encoding: "yaml"}, Reader: iotest.ErrReader(cause)},
		{Options: OptionsV1{Encoding: "json"}, Reader: io.MultiReader(strings.NewReader(`{"value":"partial"}`), iotest.ErrReader(cause))},
		{Options: OptionsV1{Encoding: "yaml"}, File: filepath.Join(t.TempDir(), "missing-private-path")},
	} {
		documents, err := Load(context.Background(), []LoadInput{readerInput("yaml", "value: new"), broken})
		if err == nil || documents != nil || value(t, good, "value") != "before" {
			t.Fatal("failed new load exposed partial or relabeled previous state")
		}
		conformance.Private(t, err, "private-reader-cause", "missing-private-path")
	}
	_, err := Load(context.Background(), []LoadInput{{Options: OptionsV1{Encoding: "yaml"}, Reader: iotest.ErrReader(cause)}})
	if !errors.Is(err, cause) || !errors.Is(err, ErrRead) {
		t.Fatal("reader cause lost")
	}
	for _, input := range []LoadInput{readerInput("json", "{"), readerInput("yaml", "[]")} {
		_, err := Load(context.Background(), []LoadInput{input})
		conformance.Cause[sdk.ConfigParseError](t, err, func(sdk.ConfigParseError) bool { return true })
	}
	_, err = Load(context.Background(), []LoadInput{readerInput("json", "{")})
	conformance.Cause[*json.SyntaxError](t, err, func(cause *json.SyntaxError) bool { return cause.Offset != 0 })
}

func TestInvalidBootstrapRejectedBeforeAnyRead(t *testing.T) {
	var typedNil *bytes.Reader
	cases := [][]LoadInput{
		nil, make([]LoadInput, MaxSources+1),
		{{Options: OptionsV1{Encoding: "yaml"}}}, {{Options: OptionsV1{Encoding: "yaml"}, Reader: typedNil}},
		{{Options: OptionsV1{Encoding: "yaml"}, File: "/literal", Reader: strings.NewReader("{}")}},
		{{Options: OptionsV1{Encoding: "yaml"}, File: "relative.yaml"}},
		{{Options: OptionsV1{Encoding: "yaml"}, File: "/bad\x00path"}},
		{readerInput("toml", "")}, {readerInput("YAML", "")},
		{{Options: OptionsV1{Encoding: "json", Defaults: []Default{{Key: "", Value: 1}}}, Reader: strings.NewReader("{}")}},
		{{Options: OptionsV1{Encoding: "json", Defaults: []Default{{Key: "value", Value: map[string]any{}}}}, Reader: strings.NewReader("{}")}},
		{{Options: OptionsV1{Encoding: "json", Defaults: []Default{{Key: "value", Value: math.NaN()}}}, Reader: strings.NewReader("{}")}},
		{{Options: OptionsV1{Encoding: "json", Defaults: []Default{{Key: "value", Value: strings.Repeat("x", MaxBootstrapBytes)}}}, Reader: strings.NewReader("{}")}},
		{{Options: OptionsV1{Encoding: "json", Environment: []Binding{{Key: "value"}}}, Reader: strings.NewReader("{}")}},
		{{Options: OptionsV1{Encoding: "json", Environment: []Binding{{Key: "value", Name: "INVALID=NAME"}}}, Reader: strings.NewReader("{}")}},
		{{Options: OptionsV1{Encoding: "json", Environment: make([]Binding, MaxEntries+1)}, Reader: strings.NewReader("{}")}},
	}
	for _, inputs := range cases {
		witness := &countReader{reader: strings.NewReader("{}")}
		inputs = append([]LoadInput{{Options: OptionsV1{Encoding: "yaml"}, Reader: witness}}, inputs...)
		// A missing trailing input is tested separately, not turned into a valid batch.
		if len(inputs) == 1 {
			inputs = nil
		}
		got, err := Load(context.Background(), inputs)
		if got != nil || err == nil || witness.reads != 0 {
			t.Fatal("invalid bootstrap reached acquisition")
		}
	}
	if got, err := Load(nil, []LoadInput{readerInput("yaml", "{}")}); got != nil || !errors.Is(err, ErrInput) {
		t.Fatal("nil context accepted")
	}
	var zero Document
	if got, err := zero.ValueCopy("key"); got != nil || !errors.Is(err, ErrInput) || zero.RawCopy() != nil || zero.Encoding() != "" {
		t.Fatal("zero document claimed loaded state")
	}
	document := loadOne(t, readerInput("yaml", "{}"))
	for _, key := range []string{"", strings.Repeat("x", MaxKeyBytes+1), strings.Repeat("a.", MaxDepth), "bad\x00key"} {
		if got, err := document.ValueCopy(key); got != nil || !errors.Is(err, ErrInput) {
			t.Fatal("invalid query reached native lookup")
		}
	}
}

type countReader struct {
	reader       io.Reader
	reads, bytes int
	closed       bool
}

func (reader *countReader) Read(buffer []byte) (int, error) {
	reader.reads++
	count, err := reader.reader.Read(buffer)
	reader.bytes += count
	return count, err
}
func (reader *countReader) Close() error { reader.closed = true; return nil }

func sizedJSON(size int) string {
	return `{"value":"` + strings.Repeat("x", size-len(`{"value":""}`)) + `"}`
}

func TestAcquisitionBoundsAndBorrowedReader(t *testing.T) {
	for _, size := range []int{MaxDocumentBytes, MaxDocumentBytes + 1, MaxDocumentBytes * 3} {
		witness := &countReader{reader: strings.NewReader(sizedJSON(size))}
		documents, err := Load(context.Background(), []LoadInput{{Options: OptionsV1{Encoding: "json"}, Reader: witness}})
		if witness.bytes > MaxDocumentBytes+1 || witness.closed {
			t.Fatal("input read exceeded its sentinel budget or borrowed reader was closed")
		}
		if size == MaxDocumentBytes {
			if err != nil || len(documents[0].RawCopy()) != size {
				t.Fatal("exact document bound rejected", err)
			}
		} else if !errors.Is(err, ErrLimit) || documents != nil {
			t.Fatal("oversized input accepted")
		}
	}
	inputs := make([]LoadInput, 5)
	for index := range inputs[:4] {
		inputs[index] = readerInput("json", sizedJSON(MaxDocumentBytes))
	}
	last := &countReader{reader: strings.NewReader("{}")}
	inputs[4] = LoadInput{Options: OptionsV1{Encoding: "json"}, Reader: last}
	if got, err := Load(context.Background(), inputs); got != nil || !errors.Is(err, ErrLimit) || last.bytes != 1 {
		t.Fatal("aggregate byte limit was not enforced during acquisition")
	}
	for _, encoding := range []string{"yaml", "json"} {
		for _, content := range []string{
			`{"value":` + strings.Repeat("[", MaxDepth+1) + "0" + strings.Repeat("]", MaxDepth+1) + "}",
			`{"value":[` + strings.Repeat("0,", MaxNodes) + "0]}",
		} {
			if got, err := Load(context.Background(), []LoadInput{readerInput(encoding, content)}); got != nil || !errors.Is(err, ErrLimit) {
				t.Fatal("structure bound accepted")
			}
		}
	}
	for _, content := range []string{"value: &value [a]\nother: *value", "{}\n---\n{}"} {
		if got, err := Load(context.Background(), []LoadInput{readerInput("yaml", content)}); got != nil || !errors.Is(err, ErrInput) {
			t.Fatal("unsupported expansion / multiple documents accepted")
		}
	}
}

type noProgress struct{}

func (noProgress) Read([]byte) (int, error) { return 0, nil }

type cancelReader struct {
	cancel context.CancelFunc
	cause  error
}

func (reader cancelReader) Read(buffer []byte) (int, error) {
	reader.cancel()
	return copy(buffer, "{}"), reader.cause
}

func TestCooperativeCancellationAndNonprogress(t *testing.T) {
	causeContext, cancelCause := context.WithCancelCause(context.Background())
	cancelReason := errors.New("private-cancel-reason")
	cancelCause(cancelReason)
	if got, err := Load(causeContext, []LoadInput{readerInput("yaml", "{}")}); got != nil || !errors.Is(err, context.Canceled) || !errors.Is(err, cancelReason) {
		t.Fatal("caller cancellation cause lost")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	witness := &countReader{reader: strings.NewReader("{}")}
	if got, err := Load(ctx, []LoadInput{{Options: OptionsV1{Encoding: "yaml"}, Reader: witness}}); got != nil || !errors.Is(err, context.Canceled) || witness.reads != 0 {
		t.Fatal("pre-cancelled input was read")
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	cause := errors.New("simultaneous-reader-error")
	if got, err := Load(ctx, []LoadInput{{Options: OptionsV1{Encoding: "yaml"}, Reader: cancelReader{cancel, cause}}}); got != nil || !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
		t.Fatal("cancellation hid read evidence")
	}
	if got, err := Load(context.Background(), []LoadInput{{Options: OptionsV1{Encoding: "yaml"}, Reader: noProgress{}}}); got != nil || !errors.Is(err, io.ErrNoProgress) {
		t.Fatal("nonprogressing input did not terminate")
	}
}

func TestLiteralFilesAndPrivacy(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "$FATHOMRY_VIPER_FILE-canary.yaml")
	content := "secret: credential-canary"
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal("fixture write failed")
	}
	t.Setenv("FATHOMRY_VIPER_FILE", "not-the-selected-file")
	input := LoadInput{Options: OptionsV1{Encoding: "yaml", Defaults: []Default{{Key: "fallback", Value: "defaults-canary"}}}, File: path}
	document := loadOne(t, input)
	if string(document.RawCopy()) != content {
		t.Fatal("literal file source changed")
	}
	conformance.Runtime(t, input, new(LoadInput), path, "defaults-canary")
	conformance.Runtime(t, input.Options.Defaults[0], new(Default), "defaults-canary")
	conformance.Runtime(t, Binding{Key: "private-key", Name: "private-environment"}, new(Binding), "private-key", "private-environment")
	conformance.Runtime(t, document, new(Document), path, "credential-canary", "defaults-canary")
	conformance.Runtime(t, *document, new(Document), path, "credential-canary", "defaults-canary")
	for _, path := range []string{directory, filepath.Join(directory, "missing-canary")} {
		got, err := Load(context.Background(), []LoadInput{{Options: OptionsV1{Encoding: "yaml"}, File: path}})
		if got != nil || err == nil {
			t.Fatal("invalid file produced configuration")
		}
		conformance.Private(t, err, path)
	}
	_, err := Load(context.Background(), []LoadInput{{Options: OptionsV1{Encoding: "yaml"}, File: filepath.Join(directory, "missing-canary")}})
	conformance.Cause[*fs.PathError](t, err, func(cause *fs.PathError) bool { return errors.Is(cause, fs.ErrNotExist) })
}

func TestSelectedPathDoesNotUseSDKLogHooks(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	old := slog.Default()
	slog.SetDefault(logger)
	sdk.SetOptions(sdk.WithLogger(logger))
	t.Cleanup(func() { sdk.Reset(); slog.SetDefault(old) })
	document := loadOne(t, readerInput("yaml", "secret: sdk-log-canary\nnested: {key: nested-canary}"))
	_ = value(t, document, "nested")
	_, _ = Load(context.Background(), []LoadInput{readerInput("yaml", "secret: [sdk-error-canary")})
	if output.Len() != 0 {
		t.Fatal("selected operations invoked a global SDK or process logger")
	}
	// Negative control: even an isolated native instance's merge leaks to the
	// package-level hook and silently retains an object on a scalar conflict.
	native := sdk.New()
	native.SetConfigType("yaml")
	if err := native.ReadConfig(strings.NewReader("secret: {token: merge-log-canary}")); err != nil {
		t.Fatal("native control read failed")
	}
	if err := native.MergeConfig(strings.NewReader("secret: scalar")); err != nil {
		t.Fatal("native control merge failed")
	}
	if !strings.Contains(output.String(), "merge-log-canary") || native.Get("secret.token") != "merge-log-canary" {
		t.Fatal("excluded native merge counterexample changed")
	}
}

func FuzzLoad(f *testing.F) {
	for _, seed := range []string{"{}", "value: null", "value: [false, 0, '']", "value: &v [a]\nother: *v", `{"value":9007199254740993}`, "value: !!str false", "value: [.nan]"} {
		f.Add([]byte(seed), false)
		f.Add([]byte(seed), true)
	}
	f.Fuzz(func(t *testing.T, raw []byte, jsonEncoding bool) {
		if len(raw) > 64<<10 {
			return
		}
		encoding := "yaml"
		if jsonEncoding {
			encoding = "json"
		}
		documents, err := Load(context.Background(), []LoadInput{{Options: OptionsV1{Encoding: encoding}, Reader: bytes.NewReader(raw)}})
		if err != nil {
			if documents != nil {
				t.Fatal("failure exposed a batch")
			}
			return
		}
		if len(documents) != 1 || !bytes.Equal(documents[0].RawCopy(), raw) {
			t.Fatal("raw handoff changed")
		}
		first := value(t, documents[0], "value")
		second := value(t, documents[0], "value")
		if !sameNativeValue(first, second) {
			t.Fatal("unchanged native query was unstable")
		}
	})
}

func TestOptionsV1Contract(t *testing.T) {
	config := OptionsV1{Encoding: "yaml", Defaults: []Default{
		{Key: "VALUE", Value: "first"}, {Key: "value", Value: "config-v1-canary"},
	}}
	conformance.Runtime(t, config, new(OptionsV1), "config-v1-canary")
	conformance.Runtime(t, &config, new(OptionsV1), "config-v1-canary")
	document := loadOne(t, LoadInput{Options: config, Reader: strings.NewReader("value: null")})
	if value(t, document, "value") != "config-v1-canary" {
		t.Fatal("ordered native scalar defaults changed")
	}
	config.Defaults[1].Value = "later"
	if value(t, document, "value") != "config-v1-canary" {
		t.Fatal("OptionsV1 retained caller list storage")
	}
	for _, config := range []OptionsV1{
		{}, {Encoding: "xml"}, {Encoding: "yaml", Defaults: make([]Default, MaxEntries+1)},
		{Encoding: "yaml", Defaults: []Default{{Key: "value", Value: math.Inf(1)}}},
		{Encoding: "yaml", Defaults: []Default{{Key: "value", Value: []string{"unsupported"}}}},
		{Encoding: "yaml", Defaults: []Default{{Key: "value", Value: "\xff"}}},
		{Encoding: "yaml", Environment: []Binding{{Key: "value", Name: "bad\x00name"}}},
	} {
		reader := &countReader{reader: strings.NewReader("{}")}
		if got, err := Load(context.Background(), []LoadInput{{Options: config, Reader: reader}}); got != nil || !errors.Is(err, ErrInput) || reader.reads != 0 {
			t.Fatal("invalid OptionsV1 reached I/O")
		}
	}
}

func TestVersionedProviderErrorIdentity(t *testing.T) {
	if ProviderID != "configsource.viper.v1" {
		t.Fatal("capability, provider and SDK-major identity changed")
	}
	cause := errors.New("private-native-cause")
	for _, test := range []struct {
		kind   fault.Kind
		suffix string
	}{
		{ErrInput, "input"}, {ErrRead, "read"}, {ErrDecode, "decode"}, {ErrLimit, "limit"}, {ErrClose, "close"},
	} {
		want := fault.Kind("fathomry.configsource.viper.v1." + test.suffix)
		if test.kind != want {
			t.Fatal("error kind escaped its versioned implementation namespace")
		}
		err := fail(test.kind, "read", cause)
		if !errors.Is(err, want) || !errors.Is(err, cause) {
			t.Fatal("kind or native identity lost")
		}
		conformance.Cause[*fault.Error](t, err, func(detail *fault.Error) bool {
			diagnostic := detail.Diagnostic()
			return diagnostic.Kind == want && diagnostic.Context.Provider == "configsource.viper.v1" &&
				diagnostic.Context.Operation == "read" && diagnostic.Context.Source == "" &&
				diagnostic.Context.Scope == ""
		})
		for _, other := range []fault.Kind{
			fault.Kind("fathomry.viper." + test.suffix),
			fault.Kind("fathomry.configsource.viper.v2." + test.suffix),
			fault.Kind("fathomry.configsource.other.v1." + test.suffix),
		} {
			if errors.Is(err, other) {
				t.Fatal("unrelated module/version falsely matched")
			}
		}
		conformance.Private(t, err, "private-native-cause")
	}
	_, err := Load(context.Background(), []LoadInput{{Options: OptionsV1{Encoding: "unsupported"}}})
	conformance.Cause[*fault.Error](t, err, func(detail *fault.Error) bool {
		diagnostic := detail.Diagnostic()
		return diagnostic.Kind == fault.Kind("fathomry.configsource.viper.v1.input") &&
			diagnostic.Context.Provider == "configsource.viper.v1"
	})
}

type ownedFile struct {
	io.Reader
	info              fs.FileInfo
	statErr, closeErr error
	closed            int
}

func (file *ownedFile) Stat() (fs.FileInfo, error) { return file.info, file.statErr }
func (file *ownedFile) Close() error               { file.closed++; return file.closeErr }

func TestOwnedFileCleanupPreservesIndependentCauses(t *testing.T) {
	readCause := errors.New("private-read-cause")
	closeCause := errors.New("private-close-cause")
	statCause := errors.New("private-stat-cause")
	fixture := fstest.MapFS{"input": &fstest.MapFile{Data: []byte("{}"), Mode: 0600}}
	info, err := fs.Stat(fixture, "input")
	if err != nil {
		t.Fatal(err)
	}
	for _, failure := range []string{"none", "read", "stat", "limit", "cancel"} {
		ctx, cancel := context.WithCancel(context.Background())
		file := &ownedFile{Reader: strings.NewReader("{}"), info: info, closeErr: closeCause}
		limit := MaxDocumentBytes
		switch failure {
		case "read":
			file.Reader = errorReader{readCause}
		case "stat":
			file.statErr = statCause
		case "limit":
			limit = 1
		case "cancel":
			cancel()
		}
		raw, err := readOwned(ctx, file, limit)
		cancel()
		if raw != nil || !errors.Is(err, closeCause) || !errors.Is(err, ErrClose) || file.closed != 1 {
			t.Fatal("owned file cleanup evidence lost")
		}
		switch failure {
		case "read":
			if !errors.Is(err, readCause) {
				t.Fatal("primary read cause lost")
			}
		case "stat":
			if !errors.Is(err, statCause) {
				t.Fatal("primary stat cause lost")
			}
		case "limit":
			if !errors.Is(err, ErrLimit) {
				t.Fatal("primary limit cause lost")
			}
		case "cancel":
			if !errors.Is(err, context.Canceled) {
				t.Fatal("primary cancellation lost")
			}
		}
		conformance.Private(t, err, "private-read-cause", "private-close-cause", "private-stat-cause")
	}
	file := &ownedFile{Reader: strings.NewReader("{}"), info: info}
	raw, err := readOwned(context.Background(), file, MaxDocumentBytes)
	if err != nil || string(raw) != "{}" || file.closed != 1 {
		t.Fatal("successful owned file was not closed")
	}
}

type errorReader struct{ err error }

func (reader errorReader) Read([]byte) (int, error) { return 0, reader.err }

func TestYAMLDuplicatePreflightBoundsNativeDiagnostics(t *testing.T) {
	for _, raw := range []string{
		"value: 1\nvalue: 2",
		"nested: {value: 1, value: 2}",
		"items: [{value: 1, value: 2}]",
		"'value': 1\n!!str value: 2",
		"1: first\n'1': second",
		strings.Repeat("duplicate: value\n", 1024),
	} {
		documents, err := Load(context.Background(), []LoadInput{readerInput("yaml", raw)})
		var native sdk.ConfigParseError
		var detail *fault.Error
		if documents != nil || !errors.Is(err, ErrInput) || errors.As(err, &native) ||
			!errors.As(err, &detail) || detail.Diagnostic().HasCauses {
			t.Fatal("duplicate YAML reached native diagnostic expansion or fabricated a native cause")
		}
	}
	document := loadOne(t, readerInput("yaml", "headers: {Key: first, key: second}\nleft: {value: 1}\nright: {value: 2}"))
	if value(t, document, "left.value") != 1 || value(t, document, "right.value") != 2 ||
		!bytes.Contains(document.RawCopy(), []byte("Key: first, key: second")) {
		t.Fatal("preflight applied case folding or cross-map duplicate policy")
	}
}

func TestOptionScalarTypesAndAggregateMetadata(t *testing.T) {
	for _, scalar := range []any{nil, true, int(-1), int8(-2), int16(3), int32(4), int64(5), uint(6), uint8(7), uint16(8), uint32(9), uint64(10), float32(1.5), float64(2.5), "text"} {
		document := loadOne(t, LoadInput{Options: OptionsV1{Encoding: "yaml", Defaults: []Default{{Key: "value", Value: scalar}}}, Reader: strings.NewReader("{}")})
		if value(t, document, "value") != scalar {
			t.Fatal("supported scalar default type/value changed")
		}
	}
	bindings := make([]Binding, MaxEntries)
	for index := range bindings {
		bindings[index] = Binding{Key: strings.Repeat("k", MaxKeyBytes), Name: strings.Repeat("N", MaxKeyBytes)}
	}
	reader := &countReader{reader: strings.NewReader("{}")}
	input := LoadInput{Options: OptionsV1{Encoding: "yaml", Environment: bindings}, Reader: reader}
	if got, err := Load(context.Background(), []LoadInput{input, input, input}); got != nil || !errors.Is(err, ErrLimit) || reader.reads != 0 {
		t.Fatal("aggregate option metadata exceeded its pre-I/O boundary")
	}
}

func TestOwnedInputRefusalAndFileCancellation(t *testing.T) {
	fixture := fstest.MapFS{"directory": &fstest.MapFile{Mode: fs.ModeDir | 0700}}
	info, err := fs.Stat(fixture, "directory")
	if err != nil {
		t.Fatal("directory metadata fixture failed")
	}
	reader := &countReader{reader: strings.NewReader("{}")}
	file := &ownedFile{Reader: reader, info: info}
	if raw, err := readOwned(context.Background(), file, MaxDocumentBytes); raw != nil || !errors.Is(err, ErrInput) || file.closed != 1 || reader.reads != 0 {
		t.Fatal("nonregular owned input was read or not closed")
	}
	path := filepath.Join(t.TempDir(), "cancelled.yaml")
	if err := os.WriteFile(path, []byte("{}"), 0600); err != nil {
		t.Fatal("file fixture write failed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if raw, err := readFile(ctx, path, MaxDocumentBytes); raw != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("file acquisition lost caller cancellation")
	}
}

type cancelAtCheck struct {
	context.Context
	cancel context.CancelFunc
	checks atomic.Int32
	target int32
}

func (ctx *cancelAtCheck) Err() error {
	if ctx.checks.Add(1) == ctx.target {
		ctx.cancel()
	}
	return ctx.Context.Err()
}

func TestLoadCancellationAtSynchronousPhaseBoundaries(t *testing.T) {
	// The context is used by this synchronous fixture only. Cancellation is
	// triggered when the loader observes it, without timing a parser or spawning work.
	for _, target := range []int32{5, 6} {
		parent, cancel := context.WithCancel(context.Background())
		ctx := &cancelAtCheck{Context: parent, cancel: cancel, target: target}
		reader := &countReader{reader: strings.NewReader("{}")}
		documents, err := Load(ctx, []LoadInput{{Options: OptionsV1{Encoding: "yaml"}, Reader: reader}})
		cancel()
		var detail *fault.Error
		if documents != nil || !errors.Is(err, context.Canceled) || reader.bytes != 2 ||
			!errors.As(err, &detail) || detail.Diagnostic().Context.Operation != "load" {
			t.Fatal("post-acquisition cancellation published a native document")
		}
	}
}

func TestStructuredLogPointersDoNotPanic(t *testing.T) {
	document := loadOne(t, readerInput("yaml", "secret: nil-log-secret"))
	options := OptionsV1{Encoding: "yaml", Defaults: []Default{{Key: "secret", Value: "nil-log-secret"}}}
	input := LoadInput{Options: options, File: "nil-log-private-path", Reader: strings.NewReader("nil-log-reader")}
	entry := Default{Key: "secret", Value: "nil-log-secret"}
	binding := Binding{Key: "secret", Name: "nil-log-environment"}
	for _, fixture := range []struct {
		name                string
		nilPointer, pointer any
	}{
		{"Document", (*Document)(nil), document},
		{"OptionsV1", (*OptionsV1)(nil), &options},
		{"LoadInput", (*LoadInput)(nil), &input},
		{"Default", (*Default)(nil), &entry},
		{"Binding", (*Binding)(nil), &binding},
	} {
		for _, state := range []struct {
			name  string
			value any
		}{
			{"nil", fixture.nilPointer}, {"populated", fixture.pointer},
		} {
			for _, encoding := range []string{"text", "json"} {
				t.Run(fixture.name+"/"+state.name+"/"+encoding, func(t *testing.T) {
					var output bytes.Buffer
					var handler slog.Handler = slog.NewTextHandler(&output, nil)
					if encoding == "json" {
						handler = slog.NewJSONHandler(&output, nil)
					}
					slog.New(handler).Info("diagnostic", slog.Any("value", state.value))
					result := output.String()
					for _, forbidden := range []string{"nil-log-", "panicked", "panic", "goroutine", "!ERROR", "!PANIC", "runtime/"} {
						if strings.Contains(result, forbidden) {
							t.Error("structured logging emitted panic/error/private diagnostics")
						}
					}
					if !strings.Contains(result, "viper[redacted]") {
						t.Error("structured logging lost its safe projection")
					}
					if encoding == "json" {
						var record map[string]any
						if err := json.Unmarshal(output.Bytes(), &record); err != nil || record["value"] != "viper[redacted]" {
							t.Error("JSON logging did not produce the redacted value")
						}
					}
				})
			}
		}
	}
}

func TestStructuredLogValuesRetainPrivacyAndJSONRefusal(t *testing.T) {
	document := loadOne(t, readerInput("yaml", "secret: nil-log-secret"))
	for _, value := range []any{
		*document,
		OptionsV1{Encoding: "yaml", Defaults: []Default{{Key: "secret", Value: "nil-log-secret"}}},
		LoadInput{File: "nil-log-private-path", Reader: strings.NewReader("nil-log-reader")},
		Default{Key: "secret", Value: "nil-log-secret"},
		Binding{Key: "secret", Name: "nil-log-environment"},
	} {
		if _, ok := value.(slog.LogValuer); ok {
			t.Fatal("value form unexpectedly regained unsafe nil-pointer promotion")
		}
		if _, err := json.Marshal(value); err == nil {
			t.Fatal("runtime value became serializable")
		}
		conformance.Private(t, value, "nil-log-secret", "nil-log-private-path", "nil-log-reader", "nil-log-environment")
		for _, encoding := range []string{"text", "json"} {
			var output bytes.Buffer
			var handler slog.Handler = slog.NewTextHandler(&output, nil)
			if encoding == "json" {
				handler = slog.NewJSONHandler(&output, nil)
			}
			slog.New(handler).Info("diagnostic", slog.Any("value", value))
			result := output.String()
			for _, forbidden := range []string{"nil-log-", "panicked", "panic", "goroutine", "!PANIC", "runtime/"} {
				if strings.Contains(result, forbidden) {
					t.Fatal("value logging emitted panic/private diagnostics")
				}
			}
			if encoding == "text" {
				if !strings.Contains(result, "viper[redacted]") {
					t.Fatal("value text projection changed")
				}
			} else {
				var record map[string]any
				if err := json.Unmarshal(output.Bytes(), &record); err != nil {
					t.Fatal("JSON handler output was invalid")
				}
				message, ok := record["value"].(string)
				if !ok || !strings.HasPrefix(message, "!ERROR:") || !strings.Contains(message, "runtime configuration serialization is unsupported") {
					t.Fatal("JSON value logging concealed the serialization refusal")
				}
			}
		}
	}
}
