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

package nacos

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
)

func TestSettings(t *testing.T) {
	tls := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer tls.Close()
	original := Settings{
		Name: "settings", Namespace: "namespace", AppName: "app",
		Servers: []Server{{HTTPURL: tls.URL + "/nacos", GRPCAddress: "127.0.0.1:9848"}},
		Keys:    []Key{{Group: "group", DataID: "config"}}, DynamicKeys: true, Writable: true,
		Username: "username-canary", Password: "password-canary",
		RootCAPEM:      string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: tls.Certificate().Raw})),
		RequestTimeout: 11 * time.Second, RetryDelay: 12 * time.Millisecond, ReconcileInterval: 13 * time.Second,
		ConcurrentRequests: 5, QueuedRequests: 6, Subscriptions: 2, QueueCapacity: 7,
	}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := configsource.Prepare(context.Background(), configsource.Schema[Settings]{Version: 1}, []configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: raw}})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := prepared.ValueCopy()
	if err != nil || !reflect.DeepEqual(decoded, original) {
		t.Fatal("loadable settings changed", err)
	}
	if err := Validate(decoded); err != nil {
		t.Fatal(err)
	}
	converted, err := options(decoded)
	if err != nil {
		t.Fatal(err)
	}
	public := reflect.ValueOf(decoded)
	internal := reflect.ValueOf(converted)
	for index := 0; index < public.NumField(); index++ {
		name := public.Type().Field(index).Name
		if name == "Servers" || name == "Keys" {
			continue
		}
		if !reflect.DeepEqual(public.Field(index).Interface(), internal.FieldByName(name).Interface()) {
			t.Fatal("native field dropped", name)
		}
	}
	if converted.Servers[0].HTTPURL != decoded.Servers[0].HTTPURL || converted.Servers[0].GRPCAddress != decoded.Servers[0].GRPCAddress || converted.Keys[0].DataID != decoded.Keys[0].DataID || converted.Keys[0].Group != decoded.Keys[0].Group {
		t.Fatal("selection field dropped")
	}
	for _, change := range []func(*Settings){
		func(value *Settings) { value.Name = "Invalid" }, func(value *Settings) { value.Servers = nil },
		func(value *Settings) { value.Keys = nil; value.DynamicKeys = false }, func(value *Settings) { value.RootCAPEM = "invalid" },
		func(value *Settings) { value.Password = "" }, func(value *Settings) { value.RequestTimeout = -1 },
		func(value *Settings) { value.RetryDelay = time.Nanosecond }, func(value *Settings) { value.ReconcileInterval = time.Millisecond },
		func(value *Settings) { value.ConcurrentRequests = 1 }, func(value *Settings) { value.QueuedRequests = 65 },
		func(value *Settings) { value.Subscriptions = value.ConcurrentRequests }, func(value *Settings) { value.QueueCapacity = 65 },
		func(value *Settings) {
			value.Servers = []Server{{HTTPURL: "http://127.0.0.1:1/nacos", GRPCAddress: "127.0.0.1:2"}}
		},
		func(value *Settings) { value.Keys = append(value.Keys, value.Keys[0]) },
	} {
		candidate, _ := prepared.ValueCopy()
		change(&candidate)
		if err := Validate(candidate); err == nil {
			t.Fatal("invalid settings accepted")
		}
	}
	for _, value := range []any{decoded, &decoded, Key{DataID: "password-canary"}, Server{HTTPURL: "password-canary"}, (*Owner)(nil), Handle{}, new(Client), new(Document), new(Observation), new(Subscription)} {
		if strings.Contains(fmt.Sprintf("%v %+v %#v", value, value, value), "password-canary") {
			t.Fatal("format leak")
		}
	}
	for _, value := range []any{Handle{}, new(Owner), new(Document), new(Observation), new(Subscription)} {
		if _, err := json.Marshal(value); err == nil {
			t.Fatal("runtime serialized")
		}
	}
	if _, err := Open(context.Background(), Settings{}, Dependencies{}); err == nil {
		t.Fatal("missing explicit dependencies accepted")
	}
	if err := new(Owner).Close(context.Background()); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
}
