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

package nuki

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/nukilabs/tlsclient/profiles"
	tls "github.com/nukilabs/utls"
	"net"
)

func pointer[T any](value T) *T { return &value }
func testContext(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func testNative() NativeOptions {
	profile := profiles.Chrome150
	return NativeOptions{Profile: &profile}
}
func testSettings(name string) Settings {
	return Settings{Name: name, Mode: pointer(HTTP1Only), MaxActive: pointer(2), MaxRoutes: pointer(2),
		MaxConnections: pointer(4), MaxOrigins: pointer(4), MaxProxyTunnels: pointer(2),
		MaxRequestBytes: pointer[int64](64 << 10), MaxResponseBytes: pointer[int64](64 << 10), MaxEncodedBytes: pointer[int64](64 << 10)}
}
func testMechanisms(t testing.TB, values ...Prepared) Dependencies {
	t.Helper()
	policy, err := Compose(values...)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(testContext(t), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(testContext(t)); err != nil {
			t.Error(err)
		}
	})
	inbox, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	return Dependencies{Runtime: runtime, Evidence: inbox}
}
func testOwner(t testing.TB, value Settings, native NativeOptions) (*Owner, Dependencies) {
	t.Helper()
	prepared, err := Prepare(value, native)
	if err != nil {
		t.Fatal(err)
	}
	dependencies := testMechanisms(t, prepared)
	owner, err := prepared.Open(testContext(t), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(testContext(t)); err != nil {
			t.Error(err)
		}
		for {
			status, _ := dependencies.Evidence.Inspect()
			if status.Outstanding == 0 {
				break
			}
			delivery, err := dependencies.Evidence.NextReleased(testContext(t))
			if err != nil {
				t.Error(err)
				break
			}
			if err := delivery.Ack(); err != nil {
				t.Error(err)
				break
			}
		}
	})
	return owner, dependencies
}

func TestPreparationUsesFrozenNativeBudget(t *testing.T) {
	value := testSettings("frozen")
	encoded, _ := json.Marshal(value)
	prepared, err := Prepare(value, testNative())
	if err != nil {
		t.Fatal(err)
	}
	before, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	afterInput, _ := json.Marshal(value)
	if string(encoded) != string(afterInput) {
		t.Fatal("preparation mutated Settings")
	}
	*value.MaxResponseBytes = 1
	after, _ := prepared.Policy()
	if !reflect.DeepEqual(before, after) {
		t.Fatal("prepared budget aliased Settings")
	}
	twice, err := Compose(prepared, prepared)
	if err != nil || twice.SourceWorkBytes != 2*before.SourceWorkBytes || twice.SourceEvidenceBytes != 2*before.SourceEvidenceBytes ||
		twice.Runtime.MaxActive != 2*before.Runtime.MaxActive || twice.Evidence.Capacity != 2*before.Evidence.Capacity {
		t.Fatal("generation overlap was not fully reserved", err)
	}
	if _, err := Prepare(testSettings("missing"), NativeOptions{}); !errors.Is(err, ErrInput) {
		t.Fatal("missing required profile admitted", err)
	}
	invalid := testSettings("zero")
	invalid.Timeout = pointer(time.Duration(0))
	if err := Validate(invalid); err == nil {
		t.Fatal("explicit zero timeout silently defaulted")
	}
	if _, err := (Prepared{}).Policy(); err == nil {
		t.Fatal("zero preparation admitted")
	}
}

func TestPreparedOpenRejectsDifferentNativeAuthorityAndSmallRuntime(t *testing.T) {
	prepared, err := Prepare(testSettings("authority"), testNative())
	if err != nil {
		t.Fatal(err)
	}
	dependencies := testMechanisms(t, prepared)
	for _, native := range []NativeOptions{{Resolver: &net.Resolver{}}, {ProxyTLS: &tls.Config{}}, testNative()} {
		dependencies.Native = native
		if owner, err := prepared.Open(testContext(t), dependencies); owner != nil || !errors.Is(err, ErrInput) {
			t.Fatal("second native selection was silently ignored", err)
		}
	}
	dependencies.Native = NativeOptions{}
	policy, _ := prepared.Policy()
	policy.Runtime.MaxWorkBytes--
	runtime, err := adapters.New(testContext(t), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	dependencies.Runtime = runtime
	if owner, err := prepared.Open(testContext(t), dependencies); owner != nil || !errors.Is(err, ErrLimit) {
		t.Fatal("source constructed without full source reservation", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	if status.Outstanding != 0 {
		t.Fatal("pre-admission refusal retained evidence")
	}
}

func TestPreparationRejectsOversizedSettings(t *testing.T) {
	oversized := strings.Repeat("a", 1<<20)
	for _, test := range []struct {
		field string
		want  error
	}{{"name", ErrInput}, {"mode", ErrLimit}, {"proxy", ErrLimit}} {
		t.Run(test.field, func(t *testing.T) {
			value := testSettings("bounded")
			switch test.field {
			case "name":
				value.Name = oversized
			case "mode":
				value.Mode = pointer(ProtocolMode(oversized))
			case "proxy":
				value.ProxyURL = "http://" + oversized
			}
			if data, err := settingsData(value); data != nil || !errors.Is(err, test.want) {
				t.Fatal("oversized data escaped source-owned preflight", err)
			}
			if err := Validate(value); !errors.Is(err, test.want) {
				t.Fatal("data validation accepted oversized input", err)
			}
			if _, err := Prepare(value, testNative()); !errors.Is(err, test.want) {
				t.Fatal("source preparation accepted oversized input", err)
			}
		})
	}
}
