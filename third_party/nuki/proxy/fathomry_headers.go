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

package proxy

import (
	"bufio"
	"bytes"
	"errors"
	"net"

	"github.com/nukilabs/http"
)

var ErrProxyHeaderLimit = errors.New("tlsclient: CONNECT response header limit exceeded")

func readConnectResponse(reader *bufio.Reader, request *http.Request, limit int64) (*http.Response, error) {
	var header []byte
	lineBytes := 0
	for {
		line, err := reader.ReadSlice('\n')
		if int64(len(header))+int64(len(line)) > limit {
			return nil, ErrProxyHeaderLimit
		}
		header = append(header, line...)
		lineBytes += len(line)
		if err != nil && !errors.Is(err, bufio.ErrBufferFull) {
			return nil, err
		}
		if err == nil {
			if lineBytes == 2 && len(line) >= 2 && line[len(line)-2] == '\r' || lineBytes == 1 {
				break
			}
			lineBytes = 0
		}
	}
	return http.ReadResponse(bufio.NewReader(bytes.NewReader(header)), request)
}

type bufferedTunnel struct {
	net.Conn
	reader *bufio.Reader
}

func (conn *bufferedTunnel) Read(data []byte) (int, error) { return conn.reader.Read(data) }
