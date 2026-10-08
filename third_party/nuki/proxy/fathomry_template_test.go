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

package proxy

import (
	"github.com/yosida95/uritemplate/v3"
	"net/url"
	"strings"
	"testing"
)

func TestFathomryTemplateOperatorsAndIPv6(t *testing.T) {
	for _, test := range []struct {
		name, raw string
		valid     bool
	}{
		{"path", "https://proxy.invalid:443/udp/{target_host}/{target_port}/", true},
		{"query", "https://proxy.invalid:443/udp{?target_host,target_port}", true},
		{"empty-path", "https://proxy.invalid:443?target_host={target_host}&target_port={target_port}", false},
		{"continuation", "https://proxy.invalid:443/udp?h={target_host}{&target_port}", true},
		{"encoded-literal", "https://proxy.invalid:443/caf%C3%A9/{target_host}/{target_port}/", true},
		{"reserved", "https://proxy.invalid:443/udp/{+target_host}/{target_port}/", false},
		{"label", "https://proxy.invalid:443/udp/{.target_host}/{target_port}/", false},
		{"path-segment", "https://proxy.invalid:443/udp{/target_host,target_port}", false},
		{"path-parameter", "https://proxy.invalid:443/udp{;target_host,target_port}", false},
		{"prefix", "https://proxy.invalid:443/udp/{target_host:1}/{target_port}/", false},
		{"explode", "https://proxy.invalid:443/udp/{target_host*}/{target_port}/", false},
		{"raw-query-unicode", "https://proxy.invalid:443/udp?h={target_host}&p={target_port}&label=测试", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			parsed, err := url.Parse(test.raw)
			if err != nil {
				t.Fatal(err)
			}
			err = ValidateFathomryTemplate(parsed)
			if (err == nil) != test.valid {
				t.Fatalf("template validity=%v want%v: %v", err == nil, test.valid, err)
			}
			if !test.valid {
				return
			}
			template, err := uritemplate.New(unescapeBraces(parsed.String()))
			if err != nil {
				t.Fatal(err)
			}
			dialer := &Dialer{proxyURL: parsed, template: template}
			target, err := dialer.expandTemplate("[2001:db8::1]:443")
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(target.EscapedPath(), "2001:db8::1") || strings.Contains(target.RawQuery, "2001:db8::1") {
				t.Fatal("IPv6 colons were not percent encoded")
			}
			if !strings.Contains(strings.ToLower(target.String()), "2001%3adb8%3a%3a1") {
				t.Fatalf("expanded IPv6 target lost native wire identity: %s", target)
			}
		})
	}
}
