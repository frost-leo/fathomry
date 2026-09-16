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

package nuki_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	nuki "github.com/frost-leo/fathomry/internal/httpclient/nuki/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	nativehttp "github.com/nukilabs/http"
	"github.com/nukilabs/tlsclient/profiles"
)

func ExampleClient_Do() {
	peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { writer.WriteHeader(204) }))
	defer peer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// An external selection, not a Provider default or approved profile list.
	profile := profiles.Safari17
	options := nuki.OptionsV1{Name: "explicit-instance", Mode: nuki.HTTP2Negotiated, Native: nuki.NativeOptionsV1{Profile: &profile}}
	selected, err := nuki.Select(options)
	must(err)
	limits, err := nuki.LimitsV1(options)
	must(err)
	selected = resource.WithLimits(selected, limits)
	assembly, err := resource.Assemble(ctx, ctx, "example", selected)
	must(err)
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		must(assembly.Close(cleanup))
	}()
	inbox, err := invocation.NewInbox[nuki.Result](1, 32<<20)
	must(err)
	client, err := nuki.Bind(assembly, selected, inbox, nil)
	must(err)
	request, err := nativehttp.NewRequest("GET", peer.URL, nil)
	must(err)
	receipt, err := client.Do(ctx, fault.Correlation{Call: "request"}, request)
	must(err)
	result, err := receipt.WaitReleased(ctx)
	must(err)
	must(result.Err())
	fmt.Println(result.Outcome.Value.Metadata().StatusCode(), result.Outcome.Value.Complete())
	delivery, err := inbox.Next(ctx)
	must(err)
	must(delivery.Release())
	// Output: 204 true
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
