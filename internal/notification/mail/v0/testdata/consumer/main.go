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

package main

import (
	"context"
	"encoding/json"
	"os"
	"runtime"
	"runtime/debug"

	"github.com/frost-leo/fathomry/internal/invocation"
	mail "github.com/frost-leo/fathomry/internal/notification/mail/v0"
	"github.com/frost-leo/fathomry/internal/resource"
)

func main() {
	options := mail.OptionsV1{Name: "consumer", Host: "smtp.invalid", Port: 465, TLSMode: "implicit", Auth: "none", Hello: "worker.invalid"}
	selected, err := mail.Select(options)
	if err != nil {
		os.Exit(1)
	}
	selected = resource.WithLimits(selected, mail.LimitsV1(options))
	assembly, err := resource.Assemble(context.Background(), context.Background(), "consumer", selected)
	if err != nil {
		os.Exit(2)
	}
	inbox, err := invocation.NewInbox[mail.Result](1, 8<<20)
	if err != nil {
		os.Exit(3)
	}
	client, err := mail.Bind(assembly, selected, inbox, nil)
	if err != nil || client.Profile().SDKMode != "mail-smtp" {
		os.Exit(4)
	}
	if err = assembly.Close(context.Background()); err != nil {
		os.Exit(5)
	}
	version := ""
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dependency := range info.Deps {
			if dependency.Path == "github.com/wneessen/go-mail" && dependency.Replace == nil {
				version = dependency.Version
			}
		}
	}
	_ = json.NewEncoder(os.Stdout).Encode(struct {
		Go, SDK                        string
		ConstructedAndClosed, MailSent bool
	}{Go: runtime.Version(), SDK: version, ConstructedAndClosed: true})
}
