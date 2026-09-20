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

// This business entry declares remote bootstrap and settings, not native clients.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/frost-leo/fathomry/adapters/configuration/remote/nacos"
	"github.com/frost-leo/fathomry/framework/configuration"
)

type settings struct {
	Name    string            `json:"name"`
	Large   uint64            `json:"large"`
	Headers map[string]string `json:"headers"`
	Items   []string          `json:"items"`
}

func loadProject(ctx context.Context, options nacos.Options) (configuration.Configuration[settings], error) {
	provider, err := nacos.New(options)
	if err != nil {
		return configuration.Configuration[settings]{}, err
	}
	return configuration.Load(ctx, configuration.Schema[settings]{SchemaVersion: 1, Defaults: settings{Name: "default"}}, configuration.Request{Provider: provider})
}

func main() {
	if len(os.Args) != 5 {
		fmt.Fprintln(os.Stderr, "supply HTTP URL, gRPC address, namespace and data ID")
		os.Exit(2)
	}
	loaded, err := loadProject(context.Background(), nacos.Options{
		Servers:   []nacos.Server{{HTTPURL: os.Args[1], GRPCAddress: os.Args[2]}},
		Namespace: os.Args[3], Sources: []nacos.Source{{Name: "base", DataID: os.Args[4], Layer: configuration.Base}},
		Username:      os.Getenv("FATHOMRY_EXAMPLE_NACOS_USERNAME"),
		Password:      os.Getenv("FATHOMRY_EXAMPLE_NACOS_PASSWORD"),
		RootCAPEM:     os.Getenv("FATHOMRY_EXAMPLE_NACOS_ROOT_CA_PEM"),
		AllowInsecure: os.Getenv("FATHOMRY_EXAMPLE_NACOS_INSECURE") == "true",
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	info := loaded.Description()
	fmt.Printf("configuration loaded: provider=%s schema=%d\n", info.Provider, info.SchemaVersion)
}
