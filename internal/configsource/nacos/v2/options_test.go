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
	"errors"
	"github.com/frost-leo/fathomry/internal/conformance"
	"strings"
	"testing"
	"time"
)

func options() OptionsV1 {
	return OptionsV1{Name: "settings", Servers: []ServerV1{{HTTPURL: "http://127.0.0.1:8848/nacos", GRPCAddress: "127.0.0.1:9848"}}, Keys: []KeyV1{{DataID: "settings.yaml"}}, AllowInsecure: true}
}
func TestOptionsDefaultsBoundsAndCopies(t *testing.T) {
	input := options()
	value, _, err := prepareOptions(input)
	if err != nil || value.Keys[0].Group != "DEFAULT_GROUP" || value.Timeout != 10*time.Second || value.Active != 4 {
		t.Fatal("defaults changed", err)
	}
	input.Servers[0].HTTPURL = "http://changed:80"
	input.Keys[0].DataID = "changed"
	if value.Keys[0].DataID != "settings.yaml" || strings.Contains(value.Servers[0].HTTPURL, "changed") {
		t.Fatal("bootstrap aliases caller slices")
	}
	cases := []func(*OptionsV1){
		func(v *OptionsV1) { v.Name = "Invalid" }, func(v *OptionsV1) { v.Servers = nil }, func(v *OptionsV1) { v.Keys = nil },
		func(v *OptionsV1) { v.AllowInsecure = false }, func(v *OptionsV1) { v.Servers[0].HTTPURL = "http://user:secret@host/nacos" },
		func(v *OptionsV1) { v.Servers[0].HTTPURL = "http://host/nacos?query=secret" }, func(v *OptionsV1) { v.Servers[0].GRPCAddress = "host:99999" },
		func(v *OptionsV1) { v.RootCAPEM = "bad pem" }, func(v *OptionsV1) { v.Username = "reader" },
		func(v *OptionsV1) { v.RequestTimeout = -1 }, func(v *OptionsV1) { v.ConcurrentRequests = 1 },
		func(v *OptionsV1) { v.Subscriptions = 4 }, func(v *OptionsV1) { v.QueueCapacity = 65 },
		func(v *OptionsV1) { v.Keys = append(v.Keys, v.Keys[0]) }, func(v *OptionsV1) { v.Keys[0].DataID = strings.Repeat("x", 129) },
	}
	for index, change := range cases {
		input := options()
		change(&input)
		if _, _, err := prepareOptions(input); err == nil {
			t.Fatalf("invalid bootstrap accepted at case %d", index)
		}
	}
	bad := options()
	bad.Password = strings.Repeat("x", 4097)
	if _, _, err := prepareOptions(bad); !errors.Is(err, ErrInput) {
		t.Fatal("credential bound not applied")
	}
}
func TestOptionsPrivacy(t *testing.T) {
	input := options()
	input.Username = "identity-canary"
	input.Password = "credential-canary"
	input.RootCAPEM = "trust-canary"
	for _, value := range []any{input, &input} {
		conformance.Runtime(t, value, new(OptionsV1), "identity-canary", "credential-canary", "trust-canary")
	}
	conformance.Private(t, (*OptionsV1)(nil), "identity-canary", "credential-canary", "trust-canary")
}
func FuzzOptions(f *testing.F) {
	f.Add("settings", "http://localhost:8848/nacos", "localhost:9848", "DEFAULT_GROUP", "settings.yaml")
	f.Fuzz(func(t *testing.T, name, httpURL, address, group, dataID string) {
		if len(name)+len(httpURL)+len(address)+len(group)+len(dataID) > 8<<10 {
			return
		}
		input := options()
		input.Name = name
		input.Servers[0] = ServerV1{HTTPURL: httpURL, GRPCAddress: address}
		input.Keys[0] = KeyV1{Group: group, DataID: dataID}
		value, _, err := prepareOptions(input)
		if err == nil && (len(value.Keys) != 1 || len(value.Servers) != 1 || !sourceName(value.Name)) {
			t.Fatal("invalid accepted bootstrap")
		}
	})
}
