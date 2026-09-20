/*
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

package main

import (
	"context"
	"errors"
	"testing"

	"github.com/frost-leo/fathomry/adapters/configuration/remote/nacos"
	"github.com/frost-leo/fathomry/framework/configuration"
)

func TestConsumerDeclarationsAndPreflight(t *testing.T) {
	options := nacos.Options{Servers: []nacos.Server{{HTTPURL: "http://127.0.0.1:9/nacos", GRPCAddress: "127.0.0.1:9"}},
		Sources: []nacos.Source{{Name: "base", DataID: "settings.yaml", Layer: configuration.Base}}, AllowInsecure: true}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loaded, err := loadProject(ctx, options)
	if !errors.Is(err, configuration.Cancelled) || !errors.Is(err, context.Canceled) {
		t.Fatal("consumer cancellation contract changed")
	}
	if _, err := loaded.Value(); !errors.Is(err, configuration.InvalidInput) {
		t.Fatal("failed consumer received settings")
	}
	_, err = loadProject(context.Background(), nacos.Options{})
	if !errors.Is(err, configuration.InvalidInput) {
		t.Fatal("consumer invalid bootstrap accepted")
	}
}
