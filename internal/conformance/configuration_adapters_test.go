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
	"encoding/json"
	"testing"

	nacos "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1"
	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	viper "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
	"github.com/frost-leo/fathomry/internal/conformance"
)

func TestPublicAdapterConfigurationSettingsContracts(t *testing.T) {
	const canary = "private-configuration-canary"
	t.Run("viper", func(t *testing.T) {
		value := viper.Settings{Encoding: "json", Defaults: []viper.Default{{Key: "value", Value: viper.Scalar{Kind: "string", Text: canary}}}}
		checkConfigurationSettings(t, value, nil, canary)
	})
	t.Run("nacos", func(t *testing.T) {
		value := nacos.Settings{Name: "contract", Servers: []nacos.Server{{HTTPURL: "http://127.0.0.1:1", GRPCAddress: "127.0.0.1:2"}},
			Keys: []nacos.Key{{DataID: "fixture"}}, Username: "fixture", Password: canary, AllowInsecure: true}
		checkConfigurationSettings(t, value, func(_ context.Context, value nacos.Settings) error { return nacos.Validate(value) }, canary)
	})
}

func checkConfigurationSettings[Settings any](t *testing.T, value Settings, validate func(context.Context, Settings) error, canary string) {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	schema := configsource.Schema[Settings]{Version: 1, Validate: validate}
	prepared, err := configsource.Prepare(context.Background(), schema,
		[]configsource.Layer{{Kind: configsource.Local, Encoding: configsource.JSON, Content: encoded}})
	if err != nil {
		t.Fatal("valid configuration-provider settings refused", err)
	}
	copied, err := prepared.ValueCopy()
	if err != nil {
		t.Fatal(err)
	}
	actual, err := json.Marshal(copied)
	if err != nil || string(actual) != string(encoded) {
		t.Fatal("configuration-provider field mapping changed")
	}
	conformance.Private(t, copied, canary)
	conformance.Private(t, &copied, canary)
	if _, err := configsource.Prepare(context.Background(), schema,
		[]configsource.Layer{{Kind: configsource.Local, Encoding: configsource.JSON, Content: []byte(`{"unrecognized_field":true}`)}}); err == nil {
		t.Fatal("unknown configuration field accepted")
	}
}
