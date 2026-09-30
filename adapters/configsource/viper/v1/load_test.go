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
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func testClient(t *testing.T, capacity int) (*Client, *adapters.Runtime, *adapters.Inbox[Evidence]) {
	t.Helper()
	runtime, err := adapters.New(context.Background(), adapters.Options{})
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[Evidence](adapters.EvidenceOptions{Capacity: capacity})
	if err != nil {
		t.Fatal(err)
	}
	client, err := New(Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return client, runtime, inbox
}
func TestLoad(t *testing.T) {
	t.Run("all formats and weak decode", func(t *testing.T) {
		for _, test := range []struct{ encoding, raw string }{{"yaml", "port: 42"}, {"yml", "port: 42"}, {"json", `{"port":42}`}, {"toml", "port = 42"}, {"dotenv", "PORT=42"}, {"env", "PORT=42"}} {
			t.Run(test.encoding, func(t *testing.T) {
				client, _, _ := testClient(t, 0)
				documents, err := client.Load(context.Background(), []Input{{Settings: Settings{Encoding: test.encoding}, Reader: strings.NewReader(test.raw)}})
				if err != nil || len(documents) != 1 {
					t.Fatal(err)
				}
				document := documents[0]
				if document.Encoding() != test.encoding || string(document.RawCopy()) != test.raw {
					t.Fatal("original bytes lost")
				}
				raw := document.RawCopy()
				raw[0] = 'x'
				if string(document.RawCopy()) != test.raw {
					t.Fatal("raw alias")
				}
				type value struct {
					Port int `mapstructure:"port"`
				}
				decoded, err := Decode[value](context.Background(), document)
				if err != nil || decoded.Port != 42 {
					t.Fatal(decoded, err)
				}
				if _, err := document.ValueCopy("port.-1"); !errors.Is(err, ErrInput) {
					t.Fatal(err)
				}
			})
		}
	})
	t.Run("loadable settings native precedence and frozen capture", func(t *testing.T) {
		declaration := `{"encoding":"json","defaults":[{"key":"number","value":{"kind":"uint64","text":"18446744073709551615"}},{"key":"fallback","value":{"kind":"string","text":"inherited"}}],"environment":[{"key":"port","name":"FATHOMRY_ADAPTER_EXPLICIT"}],"automatic_env":true,"env_prefix":"FATHOMRY_ADAPTER","env_key_replacements":[{"old":".","new":"_"}]}`
		prepared, err := configsource.Prepare(context.Background(), configsource.Schema[Settings]{Version: 1}, []configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: []byte(declaration)}})
		if err != nil {
			t.Fatal(err)
		}
		options, _ := prepared.ValueCopy()
		t.Setenv("FATHOMRY_ADAPTER_PORT", "42")
		t.Setenv("FATHOMRY_ADAPTER_EXPLICIT", "10")
		t.Setenv("FATHOMRY_ADAPTER_EXTRA_KEY", "initial")
		client, _, _ := testClient(t, 0)
		documents, err := client.Load(context.Background(), []Input{{Settings: options, Reader: strings.NewReader(`{"port":1,"nested":{"values":[1,2]}}`)}})
		if err != nil {
			t.Fatal(err)
		}
		document := documents[0]
		options.Defaults[1].Value.Text = "changed"
		number, err := document.ValueCopy("number")
		if err != nil || number != uint64(math.MaxUint64) {
			t.Fatal("precision lost", number, err)
		}
		captured, err := document.Capture(context.Background(), "extra.key")
		if err != nil {
			t.Fatal(err)
		}
		t.Setenv("FATHOMRY_ADAPTER_PORT", "43")
		t.Setenv("FATHOMRY_ADAPTER_EXTRA_KEY", "later")
		live, _ := document.ValueCopy("port")
		frozen, _ := captured.ValueCopy("port")
		extra, _ := captured.ValueCopy("extra.key")
		if live != "43" || frozen != "42" || extra != "initial" {
			t.Fatal(live, frozen, extra)
		}
		keys, err := document.Keys()
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range keys {
			if key == "extra.key" {
				t.Fatal("enumerated arbitrary environment")
			}
		}
		values, _ := captured.ValuesCopy()
		values["port"] = "mutated"
		frozen, _ = captured.ValueCopy("port")
		if frozen != "42" {
			t.Fatal("snapshot alias")
		}
		nested, _ := document.ValueCopy("nested")
		nested.(map[string]any)["values"] = nil
		again, _ := document.ValueCopy("nested")
		if again.(map[string]any)["values"] == nil {
			t.Fatal("query alias")
		}
		type envOnly struct {
			Extra struct {
				Key string `mapstructure:"key"`
			} `mapstructure:"extra"`
		}
		decoded, err := Decode[envOnly](context.Background(), document)
		if err != nil || decoded.Extra.Key != "later" {
			t.Fatal("schema key missing", err)
		}
	})
	t.Run("batch refusal original cause and borrowed readers", func(t *testing.T) {
		client, _, inbox := testClient(t, 0)
		marker := errors.New("reader-private-canary")
		reader := &failedReader{err: marker}
		documents, err := client.Load(context.Background(), []Input{{Settings: Settings{Encoding: "json"}, Reader: strings.NewReader("{}")}, {Settings: Settings{Encoding: "json"}, Reader: reader}})
		if len(documents) != 0 || !errors.Is(err, marker) || !errors.Is(err, ErrRead) || reader.closed {
			t.Fatal("partial batch or cause/ownership lost", err)
		}
		if strings.Contains(fmt.Sprintf("%+v", err), "reader-private-canary") {
			t.Fatal("cause leaked")
		}
		delivery, err := inbox.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		receipt, err := delivery.Receipt()
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := receipt.WaitReleased(context.Background())
		if err != nil || !errors.Is(snapshot.Primary(), marker) {
			t.Fatal("evidence lost")
		}
		if err := delivery.Retry(); err != nil {
			t.Fatal(err)
		}
		reads := reader.reads
		if err := inbox.DeliverOne(context.Background(), func(_ context.Context, value adapters.Snapshot[Evidence]) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if reader.reads != reads {
			t.Fatal("evidence replay reread")
		}
	})
	t.Run("raw positive absence empty and invalid", func(t *testing.T) {
		client, _, _ := testClient(t, 0)
		path := filepath.Join(t.TempDir(), "config")
		raw, missing, err := client.RawFile(context.Background(), path, MaxDocumentBytes)
		if err != nil || !missing || raw != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, nil, 0600); err != nil {
			t.Fatal(err)
		}
		raw, missing, err = client.RawFile(context.Background(), path, 0)
		if err != nil || missing || len(raw) != 0 {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("not yaml: ["), 0600); err != nil {
			t.Fatal(err)
		}
		raw, missing, err = client.RawFile(context.Background(), path, MaxDocumentBytes)
		if err != nil || missing || string(raw) != "not yaml: [" {
			t.Fatal(err)
		}
		if _, _, err := client.RawFile(context.Background(), path, 1); !errors.Is(err, ErrLimit) {
			t.Fatal(err)
		}
		if _, _, err := client.RawFile(context.Background(), "relative", 1); !errors.Is(err, ErrInput) {
			t.Fatal(err)
		}
	})
}

type failedReader struct {
	err    error
	reads  int
	closed bool
}

func (reader *failedReader) Read([]byte) (int, error) { reader.reads++; return 0, reader.err }
func (reader *failedReader) Close() error             { reader.closed = true; return nil }

type blockedReader struct {
	entered chan struct{}
	release chan struct{}
}

func (reader blockedReader) Read([]byte) (int, error) {
	close(reader.entered)
	<-reader.release
	return 0, io.EOF
}
func TestLoadOwnership(t *testing.T) {
	t.Run("required evidence before reader dispatch", func(t *testing.T) {
		client, _, inbox := testClient(t, 1)
		for index := range 2 {
			reader := &failedReader{err: io.EOF}
			_, err := client.Load(context.Background(), []Input{{Settings: Settings{Encoding: "yaml"}, Reader: reader}})
			if index == 0 {
				if err != nil || reader.reads != 1 {
					t.Fatal(err)
				}
			} else if !errors.Is(err, adapters.ErrEvidence) || reader.reads != 0 {
				t.Fatal("dispatched without evidence", err)
			}
		}
		if err := inbox.DeliverOne(context.Background(), func(context.Context, adapters.Snapshot[Evidence]) error { return errors.New("sink") }); err == nil {
			t.Fatal("failed reception acknowledged")
		}
		status, _ := inbox.Inspect()
		if status.Outstanding != 1 {
			t.Fatal(status)
		}
	})
	t.Run("blocked synchronous read retains actual work", func(t *testing.T) {
		client, runtime, _ := testClient(t, 0)
		reader := blockedReader{make(chan struct{}), make(chan struct{})}
		complete := make(chan error, 1)
		go func() {
			_, err := client.Load(context.Background(), []Input{{Settings: Settings{Encoding: "yaml"}, Reader: reader}})
			complete <- err
		}()
		<-reader.entered
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		if err := runtime.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		status, _ := runtime.Inspect()
		if status.Active != 1 || status.Closed {
			t.Fatal("released blocked reader", status)
		}
		close(reader.release)
		if err := <-complete; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if err := runtime.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}
func TestScalar(t *testing.T) {
	for _, test := range []struct {
		kind, text string
		value      any
	}{
		{"null", "", nil}, {"string", "", ""}, {"bool", "false", false}, {"int", "1", int(1)}, {"int8", "-128", int8(-128)}, {"int16", "-32768", int16(-32768)}, {"int32", "-2147483648", int32(-2147483648)}, {"int64", "-9223372036854775808", int64(math.MinInt64)},
		{"uint", "1", uint(1)}, {"uint8", "255", uint8(255)}, {"uint16", "65535", uint16(65535)}, {"uint32", "4294967295", uint32(math.MaxUint32)}, {"uint64", "18446744073709551615", uint64(math.MaxUint64)}, {"float32", "1.25", float32(1.25)}, {"float64", "1e2", float64(100)},
	} {
		value, err := scalar(Scalar{test.kind, test.text})
		if err != nil || !reflect.DeepEqual(value, test.value) {
			t.Fatal(test.kind, value, err)
		}
	}
	for _, value := range []Scalar{{}, {"null", "null"}, {"bool", "True"}, {"int8", "128"}, {"int64", "1e0"}, {"int64", "1.0"}, {"uint64", "-1"}, {"float32", "1e999"}, {"float64", "NaN"}} {
		if _, err := scalar(value); !errors.Is(err, ErrInput) {
			t.Fatal(value.Kind, err)
		}
	}
}
