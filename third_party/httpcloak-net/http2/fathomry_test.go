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

package http2

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	http "github.com/sardanioss/http"
)

func TestFathomryEmptyBodyRequiresPeerEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	peer := make(chan struct{})
	abort := make(chan struct{})
	stream := &clientStream{ctx: ctx, peerClosed: peer, abort: abort}
	body := fathomryEmptyBody{stream}
	cause := errors.New("test incomplete stream")
	stream.abortErr = cause
	close(abort)
	if _, err := body.Read(make([]byte, 1)); !errors.Is(err, cause) {
		t.Fatal("bodyless response certified without peer end")
	}
	stream.abort = make(chan struct{})
	close(peer)
	if _, err := body.Read(make([]byte, 1)); err != io.EOF {
		t.Fatal("completed bodyless response rejected")
	}
}

func TestFathomryExactCookiePairsAreRequestLocal(t *testing.T) {
	request, _ := http.NewRequest("GET", "https://example.invalid", nil)
	request.Header = http.Header{"Cookie": {"a=1; b=2", "c=3; d=4"}, "X-Between": {"middle"}, http.HeaderOrderKey: {"cookie", "x-between", "cookie"}}
	for _, exact := range []bool{true, false, true} {
		input := request
		expected := []string{"cookie:a=1", "cookie:b=2", "x-between:middle", "cookie:c=3", "cookie:d=4"}
		if exact {
			input = FathomryWithExactHeaders(request)
			expected = []string{"cookie:a=1; b=2", "x-between:middle", "cookie:c=3; d=4"}
		}
		var fields []string
		_, err := encodeRequestHeaders(input, false, 64<<10, nil, nil, "", false, func(name, value string) {
			if name == "cookie" || name == "x-between" {
				fields = append(fields, name+":"+value)
			}
		})
		if err != nil || !reflect.DeepEqual(fields, expected) {
			t.Fatalf("request-local header pairs changed: got %q want %q error=%v", fields, expected, err)
		}
	}
}
