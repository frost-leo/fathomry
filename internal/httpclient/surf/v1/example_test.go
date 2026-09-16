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

package surf_test

import (
	"context"
	"fmt"
	"io"
	stdhttp "net/http"
	"net/http/httptest"

	http "github.com/enetx/http"
	"github.com/frost-leo/fathomry/internal/fault"
	surf "github.com/frost-leo/fathomry/internal/httpclient/surf/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func ExampleClient_Do() {
	peer := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) { _, _ = io.WriteString(w, "bounded") }))
	defer peer.Close()
	ctx := context.Background()
	options := surf.OptionsV1{Name: "external", Mode: surf.HTTP1Only}
	selected, err := surf.Select(options)
	if err != nil {
		panic(err)
	}
	limits, err := surf.LimitsV1(options)
	if err != nil {
		panic(err)
	}
	selected = resource.WithLimits(selected, limits)
	assembly, err := resource.Assemble(ctx, ctx, "example", selected)
	if err != nil {
		panic(err)
	}
	defer assembly.Close(ctx)
	inbox, err := invocation.NewInbox[surf.Result](1, 16<<20)
	if err != nil {
		panic(err)
	}
	client, err := surf.Bind(assembly, selected, inbox, nil)
	if err != nil {
		panic(err)
	}
	request, err := http.NewRequest("GET", peer.URL, nil)
	if err != nil {
		panic(err)
	}
	receipt, err := client.Do(ctx, ctx, fault.Correlation{Call: "one"}, request)
	if err != nil {
		panic(err)
	}
	result, err := receipt.WaitReleased(ctx)
	if err != nil {
		panic(err)
	}
	delivery, err := inbox.Next(ctx)
	if err != nil {
		panic(err)
	}
	if err := delivery.Release(); err != nil {
		panic(err)
	}
	fmt.Println(result.Outcome.Value.Complete(), string(result.Outcome.Value.DataCopy()))
	// Output: true bounded
}
