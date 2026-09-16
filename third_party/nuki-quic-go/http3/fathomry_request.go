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

package http3

import (
	"errors"
	"io"
	"sync"
)

type requestUpload struct {
	done chan struct{}
	err  error
}
type ownedUploadBody struct {
	io.ReadCloser
	once sync.Once
	err  error
}

func (body *ownedUploadBody) Close() error {
	body.once.Do(func() { body.err = body.ReadCloser.Close() })
	return body.err
}

// A response EOF is not permission to abandon an asynchronous request writer.
// This joins the request's cancellation/close path without closing other streams.
type joinedResponseBody struct {
	io.ReadCloser
	done   <-chan struct{}
	upload *requestUpload
	input  *ownedUploadBody
}

func (body *joinedResponseBody) Read(data []byte) (int, error) {
	count, err := body.ReadCloser.Read(data)
	if err != nil {
		<-body.done
	}
	return count, err
}
func (body *joinedResponseBody) Close() error {
	err := body.ReadCloser.Close()
	<-body.done
	if body.input != nil {
		err = errors.Join(err, body.input.err)
	}
	return err
}

// UploadError is available only after response completion/Close. It describes
// the joined native upload, not whether a remote mutation happened.
func (body *joinedResponseBody) UploadError() error { <-body.done; return body.upload.err }
