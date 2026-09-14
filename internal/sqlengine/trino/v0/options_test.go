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

package trino

import (
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestOptionsRejectUnsafeProfiles(t *testing.T) {
	base := OptionsV1{Name: "trino", Endpoint: "https://example.invalid:8443", User: "test", Catalog: "lake", Schema: "gh41"}
	if _, err := Select(base); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*OptionsV1){
		func(o *OptionsV1) { o.Version = 2 }, func(o *OptionsV1) { o.Endpoint = "http://example.invalid" },
		func(o *OptionsV1) { o.Endpoint = "https://user:password@example.invalid" },
		func(o *OptionsV1) { o.Endpoint += "?password=canary" }, func(o *OptionsV1) { o.Endpoint += "/path" },
		func(o *OptionsV1) { o.Endpoint = "https://example.invalid:0" }, func(o *OptionsV1) { o.Endpoint = "https://example.invalid:" },
		func(o *OptionsV1) { o.Endpoint = "https://[::1%zone]:8443" },
		func(o *OptionsV1) { o.Password = "canary"; o.BearerToken = "token" },
		func(o *OptionsV1) { o.Endpoint = "http://example.invalid"; o.Plaintext = true; o.Password = "canary" },
		func(o *OptionsV1) { o.User = "test\r\nX-Trino-User:admin" },
		func(o *OptionsV1) { o.Catalog = "a,b" }, func(o *OptionsV1) { o.Catalog = "" },
		func(o *OptionsV1) { o.MaxActive = -1 }, func(o *OptionsV1) { o.MaxPages = 99999 },
		func(o *OptionsV1) { o.CleanupTimeout = -1 }, func(o *OptionsV1) { o.RootCAPEM = "not-a-cert" },
		func(o *OptionsV1) { o.Maintenance = true },
	} {
		o := base
		mutate(&o)
		if _, err := Select(o); err == nil {
			t.Fatal("unsafe option accepted")
		}
		conformance.Private(t, o, "canary", "token")
	}
	if _, err := Select(base, resource.Layer{Kind: resource.Local, Content: []byte("unknown: true")}); err == nil {
		t.Fatal("unknown overlay silently accepted")
	}
}
func TestVerifiedTLSAuthenticationAndTrust(t *testing.T) {
	for _, auth := range []string{"basic", "bearer"} {
		t.Run(auth, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if auth == "basic" {
					user, password, ok := r.BasicAuth()
					if !ok || user != "gh41_test" || password != "synthetic-password" {
						t.Error("basic identity mismatch")
					}
				} else if r.Header.Get("Authorization") != "Bearer synthetic-token" {
					t.Error("bearer identity mismatch")
				}
				body, _ := io.ReadAll(r.Body)
				if string(body) == "SELECT version()" {
					reply(t, w, map[string]any{"id": "ready", "columns": []any{column("version", "varchar", long(2147483647))}, "data": [][]string{{"483"}}})
				} else {
					reply(t, w, map[string]any{"id": "secure", "updateCount": 1})
				}
			}))
			defer server.Close()
			o := OptionsV1{Name: "secure", Endpoint: server.URL, User: "gh41_test", Writes: true,
				RootCAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))}
			if auth == "basic" {
				o.Password = "synthetic-password"
			} else {
				o.BearerToken = "synthetic-token"
			}
			f := bindFixture(t, o, 1)
			receipt, err := f.client.Execute(deadline(t), deadline(t), correlation("secure"), Statement{SQL: "INSERT INTO data VALUES (1)"})
			success(t, settle(t, receipt, err))
			conformance.Private(t, o, "synthetic-password", "synthetic-token")
			o.RootCAPEM = ""
			selected, err := Select(o)
			if err != nil {
				t.Fatal(err)
			}
			assembly, err := resource.Assemble(deadline(t), deadline(t), "untrusted", resource.WithLimits(selected, LimitsV1(o)))
			if err == nil {
				t.Fatal("untrusted TLS accepted")
			}
			if err = assembly.Close(deadline(t)); err != nil {
				t.Fatal(err)
			}
		})
	}
}
