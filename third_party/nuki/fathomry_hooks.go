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

package tlsclient

import (
	"context"
	"errors"
	"io"

	"github.com/nukilabs/http"
)

// FathomryCompatibilityRevision identifies this local correction, not an upstream release.
const FathomryCompatibilityRevision = "v1"

var ErrClientClosed = errors.New("tlsclient: client closed")
var ErrInvalidHook = errors.New("tlsclient: invalid hook result")

type hookContextKey struct{}

func closeBody(body io.ReadCloser) error {
	if body == nil {
		return nil
	}
	return body.Close()
}

func (c *Client) doControlled(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, ErrInvalidHook
	}
	work := req.Context()
	nested, _ := work.Value(hookContextKey{}).(bool)
	hooks := !nested && !c.inHook.Load()
	if hooks {
		for _, hook := range c.preHooks {
			previous := req
			var err error
			req, err = hook(c, req.WithContext(context.WithValue(work, hookContextKey{}, true)))
			if err != nil {
				return nil, errors.Join(err, closeBody(previous.Body))
			}
			if req == nil {
				return nil, errors.Join(ErrInvalidHook, closeBody(previous.Body))
			}
			req = req.WithContext(work)
		}
	}
	res, err := c.Client.Do(req)
	if err != nil {
		return res, err
	}
	if res == nil {
		return nil, ErrInvalidHook
	}
	if c.AutoDecompress {
		DecompressBody(res)
	}
	if hooks {
		for _, hook := range c.postHooks {
			previous := res
			res, err = hook(c, req.WithContext(context.WithValue(work, hookContextKey{}, true)), res)
			if err != nil {
				return nil, errors.Join(err, closeBody(previous.Body))
			}
			if res == nil {
				return nil, errors.Join(ErrInvalidHook, closeBody(previous.Body))
			}
		}
	}
	return res, nil
}
