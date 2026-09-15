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

package nethttp

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func ExampleClient_Do() {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(writer, "received")
	}))
	defer server.Close()
	options := OptionsV1{Name: "example", HTTP1: true, MaxActive: 1, MaxResponseBytes: 1024, MaxHeaderBytes: 1024}
	selected, err := Select(options)
	if err != nil {
		panic(err)
	}
	limits, err := LimitsV1(options)
	if err != nil {
		panic(err)
	}
	selected = resource.WithLimits(selected, limits)
	work, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cleanup, stopCleanup := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCleanup()
	assembly, err := resource.Assemble(work, cleanup, "example", selected)
	if err != nil {
		panic(err)
	}
	inbox, err := invocation.NewInbox[Result](1, 16<<10)
	if err != nil {
		panic(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		panic(err)
	}
	request, err := http.NewRequest("GET", server.URL, nil)
	if err != nil {
		panic(err)
	}
	receipt, err := client.Do(work, cleanup, fault.Correlation{Call: "example"}, request)
	if err != nil {
		panic(err)
	}
	result, err := receipt.WaitReleased(cleanup)
	if err != nil {
		panic(err)
	}
	if err := result.Err(); err != nil {
		panic(err)
	}
	delivery, err := inbox.Next(cleanup)
	if err != nil {
		panic(err)
	}
	if err := delivery.Release(); err != nil {
		panic(err)
	}
	if err := assembly.Close(cleanup); err != nil {
		panic(err)
	}
	fmt.Println(result.Outcome.Value.Metadata().StatusCode(), string(result.Outcome.Value.DataCopy()), result.Outcome.Value.Complete())
	// Output: 202 received true
}
