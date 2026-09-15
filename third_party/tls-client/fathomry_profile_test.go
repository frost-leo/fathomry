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

package tls_client

import (
	"errors"
	"testing"

	tls "github.com/bogdanfinn/utls"
)

func TestFathomryFactoryErrorsDoNotSelectNativeFallback(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		id := tls.HelloChrome_120
		cause := errors.New("synthetic factory refusal")
		calls := 0
		id.SpecFactory = func() (tls.ClientHelloSpec, error) {
			calls++
			if calls >= failAt {
				return tls.ClientHelloSpec{}, cause
			}
			return tls.UTLSIdToSpec(tls.HelloChrome_120)
		}
		_, err := supportsSessionResumption(id)
		if failAt == 2 {
			if err != nil {
				t.Fatal(err)
			}
			_, err = handshakeClientHelloID(id)
		}
		if !errors.Is(err, cause) || calls != failAt {
			t.Fatal("explicit factory refusal lost", err, calls)
		}
	}
	for _, id := range []tls.ClientHelloID{tls.HelloGolang, tls.HelloRandomized, tls.HelloRandomizedALPN, tls.HelloRandomizedNoALPN, tls.HelloChrome_120} {
		prepared, err := handshakeClientHelloID(id)
		if err != nil || prepared.Client != id.Client || prepared.Version != id.Version || !FathomryNativeSpecFactory(prepared.SpecFactory) {
			t.Fatal("native built-in path was rewritten", err)
		}
	}
}

func TestFathomryPinHostCanonicalization(t *testing.T) {
	for input, want := range map[string]string{
		"EXAMPLE.COM":      "example.com",
		"Example.Com.":     "example.com",
		"127.0.0.1":        "127.0.0.1",
		"::ffff:127.0.0.1": "127.0.0.1",
		"0:0:0:0:0:0:0:1":  "::1",
	} {
		got, err := canonicalPinHost(input)
		if err != nil || got != want {
			t.Fatalf("canonical host %q: %q %v", input, got, err)
		}
	}
	for _, input := range []string{"", ".", "example..com", "example.com..", "https://example.com", "foo*.example.com", "example.com:443", "example.com\t"} {
		if _, err := canonicalPinHost(input); err == nil {
			t.Fatalf("invalid pin host accepted: %q", input)
		}
	}
}

func TestFathomryConflictingCanonicalPinsAreRejected(t *testing.T) {
	for _, input := range []map[string][]string{
		{"example.com": {"one"}, "EXAMPLE.COM.": {"two"}},
		{"127.0.0.1": {"one"}, "::ffff:127.0.0.1": {"two"}},
		{"example.com": {"one"}, "*.example.com": {"one"}},
		{"*.127.0.0.1": {"one"}},
	} {
		if _, err := NewCertificatePinner(input); err == nil {
			t.Fatal("ambiguous or invalid pin input accepted")
		}
	}
	input := map[string][]string{"example.com": {"same"}, "EXAMPLE.COM.": {"same"}, "*.child.example.com.": {"child"}}
	value, err := NewCertificatePinner(input)
	if err != nil {
		t.Fatal(err)
	}
	pinner := value.(*certificatePinner)
	input["example.com"][0] = "mutated"
	if pin := pinner.storage.Lookup("example.com"); pin == nil || !pin.Matches("same") || pin.Matches("mutated") {
		t.Fatal("canonical pins alias caller containers")
	}
	if pin := pinner.storage.Lookup("sub.child.example.com"); pin == nil || !pin.Matches("child") {
		t.Fatal("native subdomain pin semantics lost")
	}
}
