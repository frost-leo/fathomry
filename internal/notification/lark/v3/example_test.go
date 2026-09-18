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

package lark_test

import (
	"context"
	"fmt"

	"github.com/frost-leo/fathomry/internal/invocation"
	lark "github.com/frost-leo/fathomry/internal/notification/lark/v3"
	"github.com/frost-leo/fathomry/internal/resource"
)

func ExampleSelect() {
	options := lark.OptionsV1{Name: "reports", Profile: "application", AppID: "app-example", AppSecret: "caller-supplied-secret"}
	selected, err := lark.Select(options)
	if err != nil {
		panic(err)
	}
	selected = resource.WithLimits(selected, lark.LimitsV1(options))
	assembly, err := resource.Assemble(context.Background(), context.Background(), "example", selected)
	if err != nil {
		panic(err)
	}
	defer assembly.Close(context.Background())
	inbox, err := invocation.NewInbox[lark.Result](1, lark.EvidenceBytesV1(options))
	if err != nil {
		panic(err)
	}
	client, err := lark.Bind(assembly, selected, inbox, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(client.Profile().SDKMode)
	// Construction does not acquire a token or send a message.
	// Output: lark-application
}
func ExampleChart() {
	spec, err := lark.NewJSON([]byte(`{"type":"line","data":{"values":[{"month":"Jan","items":80}]},"xField":"month","yField":"items"}`))
	if err != nil {
		panic(err)
	}
	chart, err := lark.Chart("trend", spec)
	if err != nil {
		panic(err)
	}
	card, err := lark.ComposeCard("Monthly items", chart)
	if err != nil {
		panic(err)
	}
	fmt.Println(card.Type())
	// Output: interactive
}
