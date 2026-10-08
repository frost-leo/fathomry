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

package conformance_test

import (
	"context"
	"encoding"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bogdanfinn/tls-client/profiles"
	nukiprofiles "github.com/nukilabs/tlsclient/profiles"

	kafka "github.com/frost-leo/fathomry/adapters/broker/kafka/v1"
	broker "github.com/frost-leo/fathomry/adapters/broker/v1"
	redis "github.com/frost-leo/fathomry/adapters/cache/redis/v1"
	cache "github.com/frost-leo/fathomry/adapters/cache/v1"
	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	mysql "github.com/frost-leo/fathomry/adapters/database/mysql/v1"
	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	database "github.com/frost-leo/fathomry/adapters/database/v1"
	httpcloak "github.com/frost-leo/fathomry/adapters/httpclient/httpcloak/v1"
	nethttp "github.com/frost-leo/fathomry/adapters/httpclient/nethttp/v1"
	nuki "github.com/frost-leo/fathomry/adapters/httpclient/nuki/v1"
	surf "github.com/frost-leo/fathomry/adapters/httpclient/surf/v1"
	tlsclient "github.com/frost-leo/fathomry/adapters/httpclient/tlsclient/v1"
	httpclient "github.com/frost-leo/fathomry/adapters/httpclient/v1"
	minio "github.com/frost-leo/fathomry/adapters/objectstore/minio/v1"
	objectstore "github.com/frost-leo/fathomry/adapters/objectstore/v1"
	doris "github.com/frost-leo/fathomry/adapters/sqlengine/doris/v1"
	duckdb "github.com/frost-leo/fathomry/adapters/sqlengine/duckdb/v1"
	trino "github.com/frost-leo/fathomry/adapters/sqlengine/trino/v1"
	sqlengine "github.com/frost-leo/fathomry/adapters/sqlengine/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/resource/v1"
)

type adapterPolicy struct {
	runtime  adapters.Options
	evidence adapters.EvidenceOptions
}

type adapterOwner interface {
	Close(context.Context) error
	Release(context.Context) resource.ReleaseResult
	ShutdownComplete() bool
}

func TestPublicAdapterContracts(t *testing.T) {
	const canary = "adapter-contract-private-canary"
	t.Run("httpcloak", func(t *testing.T) {
		dependencies := httpcloak.NativeOptions{}
		value := httpcloak.Settings{Name: "contract", PresetName: "chrome-148", ProxyURL: "http://" + canary + "@127.0.0.1:1"}
		checkPublicAdapter(t, value, httpcloak.Validate, func(value httpcloak.Settings) (httpclient.Policy, error) {
			return httpcloak.Recommend(value, dependencies)
		},
			func(value httpclient.Policy) adapterPolicy { return adapterPolicy{value.Runtime, value.Evidence} },
			func(runtime *adapters.Runtime, inbox *adapters.Inbox[httpcloak.Result]) httpcloak.Dependencies {
				return httpcloak.Dependencies{Runtime: runtime, Evidence: inbox, Native: dependencies}
			}, httpcloak.Open, canary)
	})
	t.Run("nuki", func(t *testing.T) {
		profile := nukiprofiles.Chrome150
		dependencies := nuki.NativeOptions{Profile: &profile}
		value := nuki.Settings{Name: "contract", ProxyURL: "http://" + canary + "@127.0.0.1:1"}
		checkPublicAdapter(t, value, nuki.Validate, func(value nuki.Settings) (httpclient.Policy, error) { return nuki.Recommend(value, dependencies) },
			func(value httpclient.Policy) adapterPolicy { return adapterPolicy{value.Runtime, value.Evidence} },
			func(runtime *adapters.Runtime, inbox *adapters.Inbox[nuki.Result]) nuki.Dependencies {
				return nuki.Dependencies{Runtime: runtime, Evidence: inbox, Native: dependencies}
			}, nuki.Open, canary)
	})
	t.Run("tlsclient", func(t *testing.T) {
		profile := profiles.Chrome_144
		native := tlsclient.NativeOptions{Profile: &profile}
		value := tlsclient.Settings{Name: "contract", ProxyURL: "http://" + canary + "@127.0.0.1:1"}
		checkPublicAdapter(t, value, tlsclient.Validate, func(value tlsclient.Settings) (httpclient.Policy, error) { return tlsclient.Recommend(value, native) },
			func(value httpclient.Policy) adapterPolicy { return adapterPolicy{value.Runtime, value.Evidence} },
			func(runtime *adapters.Runtime, inbox *adapters.Inbox[tlsclient.Result]) tlsclient.Dependencies {
				return tlsclient.Dependencies{Runtime: runtime, Evidence: inbox, Native: native}
			}, tlsclient.Open, canary)
	})
	t.Run("surf", func(t *testing.T) {
		value := surf.Settings{Name: "contract", ProxyURL: "http://" + canary + "@127.0.0.1:1"}
		checkPublicAdapter(t, value, surf.Validate, surf.Recommend,
			func(value httpclient.Policy) adapterPolicy { return adapterPolicy{value.Runtime, value.Evidence} },
			func(runtime *adapters.Runtime, inbox *adapters.Inbox[surf.Result]) surf.Dependencies {
				return surf.Dependencies{Runtime: runtime, Evidence: inbox}
			}, surf.Open, canary)
	})
	t.Run("nethttp", func(t *testing.T) {
		zero := time.Duration(0)
		disabled := false
		value := nethttp.Settings{Name: "contract", ProxyURL: "http://" + canary + "@127.0.0.1:1", HTTP2: &disabled, ExpectContinueTimeout: &zero}
		checkPublicAdapter(t, value, nethttp.Validate, nethttp.Recommend,
			func(value httpclient.Policy) adapterPolicy { return adapterPolicy{value.Runtime, value.Evidence} },
			func(runtime *adapters.Runtime, inbox *adapters.Inbox[nethttp.Result]) nethttp.Dependencies {
				return nethttp.Dependencies{Runtime: runtime, Evidence: inbox}
			},
			nethttp.Open, canary)
	})
	t.Run("postgres", func(t *testing.T) {
		value := postgres.Settings{Name: "contract", Address: "127.0.0.1", Port: 1, Database: "fixture", User: "fixture", Password: canary, Plaintext: true, ParserHome: os.Getenv("HOME")}
		checkPublicAdapter(t, value, postgres.Validate, postgres.Recommend,
			func(value database.Policy) adapterPolicy { return adapterPolicy{value.Runtime, value.Evidence} },
			func(runtime *adapters.Runtime, inbox *adapters.Inbox[postgres.Result]) postgres.Dependencies {
				return postgres.Dependencies{Runtime: runtime, Evidence: inbox}
			}, postgres.Open, canary)
	})
	t.Run("mysql", func(t *testing.T) {
		value := mysql.Settings{Name: "contract", Address: "127.0.0.1", Port: 1, User: "fixture", Password: canary, Plaintext: true}
		checkPublicAdapter(t, value, mysql.Validate, mysql.Recommend,
			func(value database.Policy) adapterPolicy { return adapterPolicy{value.Runtime, value.Evidence} },
			func(runtime *adapters.Runtime, inbox *adapters.Inbox[mysql.Result]) mysql.Dependencies {
				return mysql.Dependencies{Runtime: runtime, Evidence: inbox}
			}, mysql.Open, canary)
	})
	t.Run("minio", func(t *testing.T) {
		value := minio.Settings{Name: "contract", Endpoint: "http://127.0.0.1:1", Plaintext: true, Region: "us-east-1", Bucket: "fixture", Prefix: "owned/", AccessKey: "fixture-access", SecretKey: canary}
		checkPublicAdapter(t, value, minio.Validate, minio.Recommend,
			func(value objectstore.Policy) adapterPolicy { return adapterPolicy{value.Runtime, value.Evidence} },
			func(runtime *adapters.Runtime, inbox *adapters.Inbox[minio.Result]) minio.Dependencies {
				return minio.Dependencies{Runtime: runtime, Evidence: inbox}
			}, minio.Open, canary)
	})
	t.Run("kafka", func(t *testing.T) {
		value := kafka.Settings{Name: "contract", Brokers: []string{"127.0.0.1:1"}, ClusterID: canary, Topics: []string{"records"}, Plaintext: true}
		checkPublicAdapter(t, value, kafka.Validate, kafka.Recommend,
			func(value broker.Policy) adapterPolicy { return adapterPolicy{value.Runtime, value.Evidence} },
			func(runtime *adapters.Runtime, inbox *adapters.Inbox[kafka.Result]) kafka.Dependencies {
				return kafka.Dependencies{Runtime: runtime, Evidence: inbox}
			}, kafka.Open, canary)
	})
	t.Run("redis", func(t *testing.T) {
		idle := time.Duration(0)
		value := redis.Settings{Name: "contract", Mode: "standalone", Addrs: []string{"127.0.0.1:1"}, Plaintext: true, Password: canary, Commands: []string{"PING"}, MaxIdleTime: &idle}
		checkPublicAdapter(t, value, redis.Validate, redis.Recommend,
			func(value cache.Policy) adapterPolicy { return adapterPolicy{value.Runtime, value.Evidence} },
			func(runtime *adapters.Runtime, inbox *adapters.Inbox[redis.Result]) redis.Dependencies {
				return redis.Dependencies{Runtime: runtime, Evidence: inbox}
			}, redis.Open, canary)
	})
	t.Run("duckdb", func(t *testing.T) {
		value := duckdb.Settings{Name: "contract", Path: filepath.Join(t.TempDir(), canary+".duckdb")}
		checkPublicAdapter(t, value, duckdb.Validate, duckdb.Recommend,
			func(value sqlengine.Policy) adapterPolicy { return adapterPolicy{value.Runtime, value.Evidence} },
			func(runtime *adapters.Runtime, inbox *adapters.Inbox[duckdb.Result]) duckdb.Dependencies {
				return duckdb.Dependencies{Runtime: runtime, Evidence: inbox}
			}, duckdb.Open, canary)
		if _, err := os.Stat(value.Path); !os.IsNotExist(err) {
			t.Fatal("offline preparation or pre-admission rejection touched the native file", err)
		}
	})
	t.Run("trino", func(t *testing.T) {
		value := trino.Settings{Name: "contract", Endpoint: "http://127.0.0.1:1", Plaintext: true, User: canary}
		checkPublicAdapter(t, value, trino.Validate, trino.Recommend,
			func(value sqlengine.Policy) adapterPolicy { return adapterPolicy{value.Runtime, value.Evidence} },
			func(runtime *adapters.Runtime, inbox *adapters.Inbox[trino.Result]) trino.Dependencies {
				return trino.Dependencies{Runtime: runtime, Evidence: inbox}
			}, trino.Open, canary)
	})
	t.Run("doris", func(t *testing.T) {
		value := doris.Settings{Name: "contract", SQLAddress: "127.0.0.1:1", Database: "fixture", User: "fixture", Password: canary, Plaintext: true}
		checkPublicAdapter(t, value, doris.Validate, doris.Recommend,
			func(value sqlengine.Policy) adapterPolicy { return adapterPolicy{value.Runtime, value.Evidence} },
			func(runtime *adapters.Runtime, inbox *adapters.Inbox[doris.Result]) doris.Dependencies {
				return doris.Dependencies{Runtime: runtime, Evidence: inbox}
			}, doris.Open, canary)
	})
}

func checkPublicAdapter[Settings, Result, Dependencies, Owner, Policy any](t *testing.T, value Settings,
	validate func(Settings) error, recommend func(Settings) (Policy, error), policyOf func(Policy) adapterPolicy,
	dependencies func(*adapters.Runtime, *adapters.Inbox[Result]) Dependencies,
	open func(context.Context, Settings, Dependencies) (*Owner, error), canary string) {
	t.Helper()
	t.Run("settings", func(t *testing.T) {
		kind := reflect.TypeFor[Settings]()
		for _, codec := range []reflect.Type{reflect.TypeFor[json.Marshaler](), reflect.TypeFor[json.Unmarshaler](), reflect.TypeFor[encoding.TextMarshaler](), reflect.TypeFor[encoding.TextUnmarshaler]()} {
			if kind.Implements(codec) || reflect.PointerTo(kind).Implements(codec) {
				t.Fatal("settings acquired runtime/custom codecs")
			}
		}
		keys := map[string]bool{}
		for _, field := range reflect.VisibleFields(kind) {
			key := field.Tag.Get("json")
			if !field.IsExported() || field.Anonymous || key == "" || key == "-" || strings.Contains(key, ",") || keys[key] {
				t.Fatal("settings require explicit, unique, lossless JSON field names")
			}
			if tag := field.Tag.Get("mapstructure"); tag != "" && tag != key {
				t.Fatal("configuration key spellings diverged")
			}
			keys[key] = true
		}
		before, err := json.Marshal(value)
		if err != nil || !strings.Contains(string(before), canary) {
			t.Fatal("explicit settings JSON lost the synthetic input")
		}
		if err := validate(value); err != nil {
			t.Fatal("valid offline settings refused", err)
		}
		if _, err := recommend(value); err != nil {
			t.Fatal("valid offline recommendation refused", err)
		}
		after, err := json.Marshal(value)
		if err != nil || string(before) != string(after) {
			t.Fatal("preparation mutated caller settings")
		}
		conformance.Private(t, value, canary)
		conformance.Private(t, &value, canary)
		var caller Settings
		if json.Unmarshal(before, &caller) != nil {
			t.Fatal("settings round-trip failed")
		}
		schema := configsource.Schema[Settings]{Version: 1, Defaults: caller,
			Validate: func(_ context.Context, value Settings) error { return validate(value) }}
		prepared, err := configsource.Prepare(context.Background(), schema, nil)
		if err != nil {
			t.Fatal("settings not strict-loadable", err)
		}
		mutateSettings(reflect.ValueOf(&caller).Elem())
		first, err := prepared.ValueCopy()
		if err != nil {
			t.Fatal(err)
		}
		actual, err := json.Marshal(first)
		if err != nil || string(actual) != string(before) {
			t.Fatal("prepared settings borrow mutable input or lose zero/nil semantics")
		}
		mutateSettings(reflect.ValueOf(&first).Elem())
		second, err := prepared.ValueCopy()
		if err != nil {
			t.Fatal(err)
		}
		actual, err = json.Marshal(second)
		if err != nil || string(actual) != string(before) {
			t.Fatal("ValueCopy shares mutable settings")
		}
		schema.Defaults = second
		if _, err := configsource.Prepare(context.Background(), schema, nil); err != nil {
			t.Fatal("malformed-layer control has invalid defaults", err)
		}
		layered := second
		reflect.ValueOf(&layered).Elem().FieldByName("Name").SetString("layered")
		expected, err := json.Marshal(layered)
		if err != nil {
			t.Fatal(err)
		}
		for _, layer := range []configsource.Layer{
			{Kind: configsource.Local, Encoding: configsource.JSON, Content: expected},
			{Kind: configsource.Local, Encoding: configsource.YAML, Content: []byte("name: layered\n")},
		} {
			loaded, err := configsource.Prepare(context.Background(), schema, []configsource.Layer{layer})
			if err != nil {
				t.Fatal("valid strict configuration layer refused", err)
			}
			actual, err := loaded.ValueCopy()
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(actual)
			if err != nil || string(encoded) != string(expected) {
				t.Fatal("valid layer was ignored or changed settings semantics")
			}
		}
		for _, raw := range []string{`{"unknown_contract_field":true}`, `{"name":"first","name":"second"}`, `{"name":null}`, `null`, `[]`} {
			if _, err := configsource.Prepare(context.Background(), schema, []configsource.Layer{{Kind: configsource.Local, Encoding: configsource.JSON, Content: []byte(raw)}}); err == nil {
				t.Fatal("malformed/unknown/null settings were accepted")
			}
		}
		var invalid Settings
		if validate(invalid) == nil {
			t.Fatal("zero invalid settings accepted")
		}
		if _, err := recommend(invalid); err == nil {
			t.Fatal("invalid settings produced a usable policy")
		}
	})
	t.Run("ownership_surface", func(t *testing.T) {
		owner := reflect.TypeFor[*Owner]()
		if !owner.Implements(reflect.TypeFor[adapterOwner]()) {
			t.Fatal("source owner lost explicit repeatable shutdown authority")
		}
		for _, method := range []string{"Client", "Handle"} {
			declaration, exists := owner.MethodByName(method)
			if !exists || declaration.Type.NumIn() != 1 || declaration.Type.NumOut() != 1 {
				t.Fatal("owner lost its non-owning capability projection")
			}
			capability := declaration.Type.Out(0)
			if capability.Kind() == reflect.Pointer {
				capability = capability.Elem()
			}
			conformance.Runtime(t, reflect.Zero(capability).Interface(), reflect.New(capability).Interface())
			for _, field := range reflect.VisibleFields(capability) {
				if field.IsExported() {
					t.Fatal("non-owning capability exposes mutable authority")
				}
			}
			for _, forbidden := range []string{"Close", "Release", "ShutdownComplete", "Runtime", "Assembly"} {
				if _, exists := reflect.PointerTo(capability).MethodByName(forbidden); exists {
					t.Fatal("borrowed facade acquired source/runtime shutdown authority")
				}
			}
		}
	})
	t.Run("rejected_construction", func(t *testing.T) {
		policy, err := recommend(value)
		if err != nil {
			t.Fatal(err)
		}
		reservations := policyOf(policy)
		runtime, err := adapters.New(context.Background(), reservations.runtime)
		if err != nil {
			t.Fatal("recommended runtime rejected", err)
		}
		t.Cleanup(func() {
			if err := runtime.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
		inbox, err := adapters.NewInbox[Result](reservations.evidence)
		if err != nil {
			t.Fatal("recommended evidence rejected", err)
		}
		deps := dependencies(runtime, inbox)
		assertRejected := func(ctx context.Context, expected error) {
			t.Helper()
			owner, err := open(ctx, value, deps)
			if owner != nil {
				if owned, ok := any(owner).(adapterOwner); ok {
					cleanup, stop := context.WithTimeout(context.Background(), time.Second)
					defer stop()
					_ = owned.Close(cleanup)
				}
				t.Fatal("rejected setup returned native ownership")
			}
			if err == nil || expected != nil && !errors.Is(err, expected) {
				t.Fatal("rejected setup lost its boundary error", err)
			}
			status, err := inbox.Inspect()
			if err != nil || status != (adapters.EvidenceStatus{}) {
				t.Fatal("pre-admission rejection retained evidence")
			}
		}
		assertRejected(nil, nil)
		if err := runtime.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		assertRejected(context.Background(), adapters.ErrClosed)
	})
}

func mutateSettings(value reflect.Value) {
	switch value.Kind() {
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			mutateSettings(value.Field(index))
		}
	case reflect.Pointer:
		if !value.IsNil() {
			mutateSettings(value.Elem())
		}
	case reflect.Slice:
		if value.Len() > 0 {
			mutateSettings(value.Index(0))
		}
	case reflect.Map:
		if !value.IsNil() {
			value.Clear()
		}
	case reflect.String:
		value.SetString("changed")
	case reflect.Int, reflect.Int64:
		value.SetInt(123)
	}
}
