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

package redis

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

type transport struct {
	mu       sync.Mutex
	settings settings
	sockets  map[*wireConn]struct{}
	pending  int
	sealed   bool
	changed  chan struct{}
	lifetime context.Context
	stop     context.CancelFunc
}

var errSocketCapacity = errors.New("redis: local socket capacity exhausted")

func newTransport(value settings) *transport {
	ctx, cancel := context.WithCancel(context.Background())
	return &transport{settings: value, sockets: make(map[*wireConn]struct{}), changed: make(chan struct{}), lifetime: ctx, stop: cancel}
}
func (owner *transport) signal() { close(owner.changed); owner.changed = make(chan struct{}) }
func (owner *transport) dial(ctx context.Context, network, address string) (net.Conn, error) {
	owner.mu.Lock()
	if owner.sealed || network != "tcp" || !owner.settings.allowed(address) {
		owner.mu.Unlock()
		return nil, failure(ErrAuthority, "dial")
	}
	if owner.pending+len(owner.sockets) >= owner.settings.MaxConnections {
		owner.mu.Unlock()
		return nil, failure(ErrLimit, "connections", errSocketCapacity)
	}
	owner.pending++
	owner.mu.Unlock()
	defer func() { owner.mu.Lock(); owner.pending--; owner.signal(); owner.mu.Unlock() }()
	work, cancel := context.WithTimeout(ctx, owner.settings.Timeout)
	stop := context.AfterFunc(owner.lifetime, cancel)
	defer func() { stop(); cancel() }()
	connection, err := (&net.Dialer{}).DialContext(work, network, address)
	if err != nil {
		return nil, failure(ErrCommand, "dial", err)
	}
	config, _ := owner.settings.tls()
	if config != nil {
		if config.ServerName == "" {
			config.ServerName, _, _ = net.SplitHostPort(address)
		}
		secure := tls.Client(connection, config)
		if err := secure.HandshakeContext(work); err != nil {
			_ = connection.Close()
			return nil, failure(ErrCommand, "tls", err)
		}
		connection = secure
	}
	conn := &wireConn{Conn: connection, owner: owner, reader: bufio.NewReaderSize(connection, 4096)}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.sealed {
		_ = connection.Close()
		return nil, failure(ErrState, "dial")
	}
	owner.sockets[conn] = struct{}{}
	return conn, nil
}
func (owner *transport) seal() {
	owner.mu.Lock()
	owner.sealed = true
	owner.stop()
	sockets := make([]*wireConn, 0, len(owner.sockets))
	for conn := range owner.sockets {
		sockets = append(sockets, conn)
	}
	owner.mu.Unlock()
	for _, conn := range sockets {
		_ = conn.Close()
	}
}
func (owner *transport) drained(ctx context.Context) bool {
	for {
		owner.mu.Lock()
		done := owner.pending == 0 && len(owner.sockets) == 0
		changed := owner.changed
		owner.mu.Unlock()
		if done {
			return true
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return false
		}
	}
}

// wireConn validates one whole frame before the native parser sees its lengths.
// The per-frame element bound also protects tiny-wire, huge-allocation aggregates.
type wireConn struct {
	net.Conn
	owner     *transport
	reader    *bufio.Reader
	pending   []byte
	closeOnce sync.Once
	closeErr  error
	errorMu   sync.Mutex
	// SDK follow-up deadline probes must not erase the first framing failure.
	terminal error
}

func (conn *wireConn) Close() error {
	conn.closeOnce.Do(func() {
		conn.closeErr = conn.Conn.Close()
		conn.owner.mu.Lock()
		delete(conn.owner.sockets, conn)
		conn.owner.signal()
		conn.owner.mu.Unlock()
	})
	return conn.closeErr
}
func (conn *wireConn) Read(output []byte) (int, error) {
	if err := conn.failure(); err != nil {
		return 0, err
	}
	if len(output) == 0 {
		return 0, nil
	}
	if len(conn.pending) == 0 {
		var err error
		conn.pending, err = readFrame(conn.reader, conn.owner.settings.MaxReplyBytes, conn.owner.settings.MaxReplyElements)
		if err != nil {
			// A partial frame cannot be safely resumed by the SDK after an idle timeout.
			if len(conn.pending) > 0 || !timeout(err) {
				conn.errorMu.Lock()
				conn.terminal = err
				conn.errorMu.Unlock()
				_ = conn.Close()
			}
			conn.pending = nil
			return 0, err
		}
	}
	count := copy(output, conn.pending)
	conn.pending = conn.pending[count:]
	if len(conn.pending) == 0 {
		conn.pending = nil
	}
	return count, nil
}
func (conn *wireConn) failure() error {
	conn.errorMu.Lock()
	defer conn.errorMu.Unlock()
	return conn.terminal
}
func timeout(err error) bool {
	var network net.Error
	return errors.As(err, &network) && network.Timeout()
}
func (conn *wireConn) SetReadDeadline(deadline time.Time) error {
	if err := conn.failure(); err != nil {
		return err
	}
	return conn.Conn.SetReadDeadline(conn.bounded(deadline))
}
func (conn *wireConn) SetWriteDeadline(deadline time.Time) error {
	if err := conn.failure(); err != nil {
		return err
	}
	return conn.Conn.SetWriteDeadline(conn.bounded(deadline))
}
func (conn *wireConn) SetDeadline(deadline time.Time) error {
	if err := conn.failure(); err != nil {
		return err
	}
	return conn.Conn.SetDeadline(conn.bounded(deadline))
}
func (conn *wireConn) bounded(deadline time.Time) time.Time {
	bound := time.Now().Add(conn.owner.settings.Timeout)
	if deadline.IsZero() || bound.Before(deadline) {
		return bound
	}
	return deadline
}

func readFrame(reader *bufio.Reader, maxBytes, maxElements int) ([]byte, error) {
	frame := make([]byte, 0, min(4096, maxBytes))
	elements := 0
	var read func(int, bool) error
	read = func(depth int, attributesAllowed bool) error {
		elements++
		if depth > 32 || elements > maxElements {
			return failure(ErrLimit, "reply")
		}
		start := len(frame)
		for {
			line, err := reader.ReadSlice('\n')
			if len(line) > maxBytes-len(frame) {
				return failure(ErrLimit, "reply")
			}
			frame = append(frame, line...)
			if err == bufio.ErrBufferFull {
				continue
			}
			if err != nil {
				return err
			}
			break
		}
		line := frame[start:]
		if len(line) < 3 || line[len(line)-2] != '\r' {
			return failure(ErrProtocol, "frame")
		}
		value := line[1 : len(line)-2]
		switch line[0] {
		case '+', '-':
			if bytes.ContainsAny(value, "\r\n") {
				return failure(ErrProtocol, "line")
			}
			return nil
		case ':':
			if _, err := strconv.ParseInt(string(value), 10, 64); err != nil {
				return failure(ErrProtocol, "integer")
			}
			return nil
		case ',':
			if _, err := strconv.ParseFloat(string(value), 64); err != nil {
				return failure(ErrProtocol, "double")
			}
			return nil
		case '(':
			if len(value) == 0 || len(value) == 1 && value[0] == '-' {
				return failure(ErrProtocol, "integer")
			}
			for index, char := range value {
				if char < '0' || char > '9' {
					if index != 0 || char != '-' {
						return failure(ErrProtocol, "integer")
					}
				}
			}
			return nil
		case '_':
			if len(value) != 0 {
				return failure(ErrProtocol, "null")
			}
			return nil
		case '#':
			if string(value) != "t" && string(value) != "f" {
				return failure(ErrProtocol, "boolean")
			}
			return nil
		case '$', '!', '=':
			size, err := strconv.ParseInt(string(value), 10, 64)
			if err != nil || size < -1 || size == -1 && line[0] != '$' {
				return failure(ErrProtocol, "bulk")
			}
			if size == -1 {
				return nil
			}
			if size > int64(maxBytes-len(frame)-2) {
				return failure(ErrLimit, "reply")
			}
			offset := len(frame)
			frame = append(frame, make([]byte, int(size)+2)...)
			if _, err := io.ReadFull(reader, frame[offset:]); err != nil {
				return err
			}
			if frame[len(frame)-2] != '\r' || frame[len(frame)-1] != '\n' {
				return failure(ErrProtocol, "bulk")
			}
			if line[0] == '=' && (size < 4 || frame[offset+3] != ':') {
				return failure(ErrProtocol, "verbatim")
			}
			return nil
		case '*', '~', '>', '%', '|':
			if line[0] == '|' && !attributesAllowed {
				return failure(ErrProtocol, "nested-attributes")
			}
			count, err := strconv.ParseInt(string(value), 10, 64)
			if err != nil || count < -1 {
				return failure(ErrProtocol, "aggregate")
			}
			if count == -1 {
				if line[0] != '*' {
					return failure(ErrProtocol, "aggregate")
				}
				return nil
			}
			if count > int64(maxElements-elements) {
				return failure(ErrLimit, "reply")
			}
			if line[0] == '%' || line[0] == '|' {
				count *= 2
			}
			if count > int64(maxElements-elements) {
				return failure(ErrLimit, "reply")
			}
			for range count {
				if err := read(depth+1, attributesAllowed && line[0] != '|'); err != nil {
					return err
				}
			}
			if line[0] == '|' {
				return read(depth+1, true)
			}
			return nil
		default:
			return failure(ErrProtocol, "frame")
		}
	}
	err := read(0, true)
	return frame, err
}
