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

package tlsclient_test

import (
	"context"
	"fmt"

	"github.com/bogdanfinn/tls-client/profiles"
	tlsclient "github.com/frost-leo/fathomry/internal/httpclient/tlsclient/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func ExampleSelect() {
	profile := profiles.Chrome_144
	options := tlsclient.OptionsV1{
		Name:   "external-instance",
		Native: tlsclient.NativeOptionsV1{Profile: &profile},
	}
	selected, err := tlsclient.Select(options)
	if err != nil {
		panic(err)
	}
	limits, err := tlsclient.LimitsV1(options)
	if err != nil {
		panic(err)
	}
	selected = resource.WithLimits(selected, limits)
	ctx := context.Background()
	assembly, err := resource.Assemble(ctx, ctx, "example", selected)
	if err != nil {
		panic(err)
	}
	defer assembly.Close(ctx)
	inbox, err := invocation.NewInbox[tlsclient.Result](1, 32<<20)
	if err != nil {
		panic(err)
	}
	client, err := tlsclient.Bind(assembly, selected, inbox, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(tlsclient.ProviderID, client.Profile().SDKMode)
	// Output: httpclient.tlsclient.v1 tlsclient-v1
}
