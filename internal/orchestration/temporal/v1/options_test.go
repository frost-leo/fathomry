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

package temporal_test

import (
	"testing"

	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
)

func TestConfigurationRefusesInvalidOrAmbientAuthority(t *testing.T) {
	valid := temporal.OptionsV1{Name: "test", Endpoint: "127.0.0.1:7233", Namespace: "test", Plaintext: true}
	for _, test := range []struct {
		name   string
		change func(*temporal.OptionsV1)
	}{
		{"missing-endpoint", func(value *temporal.OptionsV1) { value.Endpoint = "" }},
		{"named-port", func(value *temporal.OptionsV1) { value.Endpoint = "127.0.0.1:unknown" }},
		{"unknown-resolver", func(value *temporal.OptionsV1) { value.Endpoint = "custom:///localhost:7233" }},
		{"dns-missing-port", func(value *temporal.OptionsV1) { value.Endpoint = "dns:///localhost" }},
		{"dns-empty-target", func(value *temporal.OptionsV1) { value.Endpoint = "dns:///" }},
		{"dns-credentials", func(value *temporal.OptionsV1) { value.Endpoint = "dns://user:secret@127.0.0.1:53/localhost:7233" }},
		{"dns-query", func(value *temporal.OptionsV1) { value.Endpoint = "dns:///localhost:7233?target=other" }},
		{"dns-fragment", func(value *temporal.OptionsV1) { value.Endpoint = "dns:///localhost:7233#other" }},
		{"dns-extra-path", func(value *temporal.OptionsV1) { value.Endpoint = "dns:///localhost:7233/other" }},
		{"dns-encoded-target", func(value *temporal.OptionsV1) { value.Endpoint = "dns:///local%68ost:7233" }},
		{"dns-invalid-authority", func(value *temporal.OptionsV1) { value.Endpoint = "dns://127.0.0.1:99999/localhost:7233" }},
		{"passthrough-authority", func(value *temporal.OptionsV1) { value.Endpoint = "passthrough://other/localhost:7233" }},
		{"missing-namespace", func(value *temporal.OptionsV1) { value.Namespace = "" }},
		{"future-format", func(value *temporal.OptionsV1) { value.Version = 2 }},
		{"plaintext-key", func(value *temporal.OptionsV1) { value.APIKey = "private-token" }},
		{"unknown-rpc", func(value *temporal.OptionsV1) { value.RPCs = []string{workflowPrefix + "Unknown"} }},
		{"duplicate-rpc", func(value *temporal.OptionsV1) { value.RPCs = []string{countMethod, countMethod} }},
		{"incomplete-mtls", func(value *temporal.OptionsV1) { value.Plaintext = false; value.CertificatePEM = "invalid" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := valid
			test.change(&options)
			if _, err := temporal.Select(options); err == nil {
				t.Fatal("invalid authority configuration accepted")
			}
		})
	}
	if _, err := temporal.Select(valid); err != nil {
		t.Fatal(err)
	}
}

func TestNativeResolverTargetConfiguration(t *testing.T) {
	for _, endpoint := range []string{
		"localhost:7233", "[::1]:7233", "dns:///localhost:7233", "dns:///[::1]:7233",
		"dns://127.0.0.1:5353/localhost:7233", "dns://resolver.test/localhost:7233",
		"dns://[::1]/localhost:7233", "passthrough:///127.0.0.1:7233",
	} {
		t.Run(endpoint, func(t *testing.T) {
			if _, err := temporal.Select(temporal.OptionsV1{Name: "resolver", Endpoint: endpoint, Namespace: "test", Plaintext: true}); err != nil {
				t.Fatal("explicit native target was rejected", err)
			}
		})
	}
}
