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
	"io"

	http "github.com/sardanioss/http"
)

const FathomryCompatibilityRevision = "v1"

type fathomryExactHeadersKey struct{}

// FathomryWithExactHeaders preserves Cookie field pairs for this request only,
// without modifying connection or transport settings shared by other requests.
func FathomryWithExactHeaders(request *http.Request) *http.Request {
	return request.WithContext(context.WithValue(request.Context(), fathomryExactHeadersKey{}, true))
}

// FathomryWaitRequests joins request writers, body-close tasks and native trace
// callbacks. The owning integration must stop new RoundTrip calls before waiting.
// It must not be invoked from a callback of one of these requests.
func (client *ClientConn) FathomryWaitRequests() {
	client.fathomryRequests.Wait()
}

type fathomryEmptyBody struct{ stream *clientStream }

func (body fathomryEmptyBody) Read(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	select {
	case <-body.stream.peerClosed:
		return 0, io.EOF
	default:
	}
	select {
	case <-body.stream.peerClosed:
		return 0, io.EOF
	case <-body.stream.abort:
		return 0, body.stream.abortErr
	case <-body.stream.ctx.Done():
		return 0, body.stream.ctx.Err()
	}
}
func (body fathomryEmptyBody) Close() error {
	body.stream.abortStream(errClosedResponseBody)
	return nil
}
