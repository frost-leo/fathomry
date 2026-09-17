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

package mail

import (
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
)

func TestOptionsRejectUnsafeProfiles(t *testing.T) {
	base := OptionsV1{Name: "test", Host: "smtp.fixture.test", Port: 465, Hello: "worker.fixture.test", TLSMode: "implicit", Auth: "none"}
	for name, change := range map[string]func(*OptionsV1){
		"version":               func(o *OptionsV1) { o.Version = 2 },
		"name":                  func(o *OptionsV1) { o.Name = "secret@example.test" },
		"implicit-host":         func(o *OptionsV1) { o.Host = "" },
		"host-injection":        func(o *OptionsV1) { o.Host = "smtp.fixture.test\r\n" },
		"host-url":              func(o *OptionsV1) { o.Host = "smtp://fixture.test" },
		"port":                  func(o *OptionsV1) { o.Port = 0 },
		"hello":                 func(o *OptionsV1) { o.Hello = "" },
		"plaintext":             func(o *OptionsV1) { o.TLSMode = "none" },
		"opportunistic":         func(o *OptionsV1) { o.TLSMode = "optional" },
		"auth-fallback":         func(o *OptionsV1) { o.Auth = "auto" },
		"missing-auth":          func(o *OptionsV1) { o.Auth = "" },
		"credentials-with-none": func(o *OptionsV1) { o.Password = "private" },
		"empty-password":        func(o *OptionsV1) { o.Auth = "plain"; o.Username = "user" },
		"control-credential":    func(o *OptionsV1) { o.Auth = "xoauth2"; o.Username = "user"; o.Password = "token\x01" },
		"active":                func(o *OptionsV1) { o.MaxActive = 17 },
		"queue":                 func(o *OptionsV1) { o.QueuedCalls = 33 },
		"input":                 func(o *OptionsV1) { o.MaxMessageBytes = 17 << 20 },
		"mime":                  func(o *OptionsV1) { o.MaxMIMEBytes = 33 << 20 },
		"reply":                 func(o *OptionsV1) { o.MaxReplyBytes = 3 << 20 },
		"timeout":               func(o *OptionsV1) { o.Timeout = -time.Second },
		"trust":                 func(o *OptionsV1) { o.RootCAPEM = "not a certificate" },
	} {
		t.Run(name, func(t *testing.T) {
			value := base
			change(&value)
			if _, err := Select(value); err == nil {
				t.Fatal("invalid profile selected")
			}
		})
	}
	for _, overlay := range []string{"unknown_field: true\n", "tls_mode: optional\n", "max_active: 0\n", "password: secret\n"} {
		if _, err := Select(base, resource.Layer{Kind: resource.Local, Content: []byte(overlay)}); err == nil {
			t.Fatal("unsafe overlay selected")
		}
	}
	if _, err := Select(base); err != nil {
		t.Fatal(err)
	}
}
