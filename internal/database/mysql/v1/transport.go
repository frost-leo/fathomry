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

package mysql

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// wire owns framing above the selected TLS transport. It never lets an unchecked
// packet header reach the SDK's size-based allocation. Calls serialize protocol
// use; Close and deadline cancellation may run concurrently.
type wire struct {
	raw             net.Conn
	transport       net.Conn
	settings        settings
	ctx             context.Context
	pending         []byte
	phase           byte
	command         byte
	authOffset      byte
	firstAuth       bool
	longDataPending bool
	columns         int
	remaining       int
	rowCount        int
	inTransaction   bool
	responseBytes   int
	fields          []byte
	version         string
	tlsState        tls.ConnectionState
	secured         func(*tls.Config)
	closed          atomic.Bool
	once            sync.Once
	mu              sync.Mutex
	deadline        time.Time
	primary         error
	closeError      error
}

const (
	phaseAuth byte = iota
	phaseHeader
	phaseColumns
	phaseColumnEnd
	phaseRows
	phasePrepare
	phaseIdle
	phasePrepareParams
	phasePrepareParamEnd
	phasePrepareColumns
	phasePrepareColumnEnd
)

type nativeContext struct{ context.Context }

func (nativeContext) Value(any) any { return nil }

func openWire(ctx context.Context, s settings, dial func(context.Context, string, string) (net.Conn, error)) (*wire, error) {
	raw, err := dial(nativeContext{ctx}, s.Network, s.address())
	if err != nil {
		return nil, err
	}
	w := &wire{raw: raw, transport: raw, settings: s, ctx: ctx, firstAuth: true}
	return w, nil
}
func (w *wire) fail(err error) error {
	w.mu.Lock()
	if w.primary == nil {
		w.primary = err
	}
	w.mu.Unlock()
	_ = w.Close()
	return err
}
func (w *wire) evidence() (error, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.primary, w.closeError
}
func (w *wire) clearEvidence() {
	w.mu.Lock()
	w.primary = nil
	w.mu.Unlock()
}
func (w *wire) Close() error {
	w.once.Do(func() {
		w.closed.Store(true)
		err := w.raw.Close()
		w.mu.Lock()
		w.closeError = err
		w.mu.Unlock()
	})
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closeError
}
func (w *wire) LocalAddr() net.Addr  { return w.raw.LocalAddr() }
func (w *wire) RemoteAddr() net.Addr { return w.raw.RemoteAddr() }
func (w *wire) clipped(deadline time.Time) time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.deadline.IsZero() && (deadline.IsZero() || w.deadline.Before(deadline)) {
		return w.deadline
	}
	return deadline
}
func (w *wire) SetDeadline(d time.Time) error      { return w.raw.SetDeadline(w.clipped(d)) }
func (w *wire) SetReadDeadline(d time.Time) error  { return w.raw.SetReadDeadline(w.clipped(d)) }
func (w *wire) SetWriteDeadline(d time.Time) error { return w.raw.SetWriteDeadline(w.clipped(d)) }

// activate owns cancellation throughout native row drainage and context-less
// finalization. stop joins an entered callback before the connection can be reused.
func (w *wire) activate(ctx context.Context) func() {
	deadline, _ := ctx.Deadline()
	w.mu.Lock()
	w.deadline = deadline
	w.mu.Unlock()
	_ = w.raw.SetDeadline(deadline)
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); _ = w.fail(errors.Join(ctx.Err(), context.Cause(ctx))) })
	var once sync.Once
	return func() {
		once.Do(func() {
			if !stop() {
				<-done
			}
			w.mu.Lock()
			w.deadline = time.Time{}
			w.mu.Unlock()
			_ = w.raw.SetDeadline(time.Time{})
		})
	}
}
func (w *wire) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if len(w.pending) == 0 {
		frame, err := w.readFrame()
		if err != nil {
			return 0, w.fail(err)
		}
		if err = w.inspect(frame[4:]); err != nil {
			return 0, w.fail(err)
		}
		if w.phase == phaseAuth && w.authOffset != 0 {
			frame[3] -= w.authOffset
		}
		// The terminal auth packet still carries the translated authentication sequence.
		if w.phase == phaseIdle && w.command == 0 && w.authOffset != 0 {
			frame[3] -= w.authOffset
			w.authOffset = 0
		}
		if w.phase == phaseIdle && w.command == 0 {
			w.ctx = nil
			w.secured = nil
		}
		w.pending = frame
	}
	count := copy(dst, w.pending)
	w.pending = w.pending[count:]
	return count, nil
}
func (w *wire) readFrame() ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(w.transport, header[:]); err != nil {
		return nil, err
	}
	size := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	if size == 0 || size > w.settings.MaxPacketBytes {
		return nil, failure(ErrLimit, "packet")
	}
	if size+4 > w.settings.MaxResponseBytes-w.responseBytes {
		return nil, failure(ErrLimit, "response")
	}
	w.responseBytes += size + 4
	frame := make([]byte, size+4)
	copy(frame, header[:])
	if _, err := io.ReadFull(w.transport, frame[4:]); err != nil {
		return nil, err
	}
	return frame, nil
}
func (w *wire) Write(frame []byte) (int, error) {
	if len(frame) < 4 || len(frame)-4 != int(frame[0])|int(frame[1])<<8|int(frame[2])<<16 ||
		len(frame) == 4 && (w.phase != phaseAuth || w.firstAuth) {
		return 0, w.fail(failure(ErrProtocol, "write-frame"))
	}
	if w.firstAuth {
		w.firstAuth = false
		if len(frame) < 36 {
			return 0, w.fail(failure(ErrProtocol, "auth-response"))
		}
		if !w.settings.Plaintext {
			trust, err := trust(w.settings)
			if err != nil {
				return 0, w.fail(err)
			}
			request := make([]byte, 36)
			copy(request[4:], frame[4:36])
			request[0], request[3] = 32, 1
			flags := binary.LittleEndian.Uint32(request[4:]) | 1<<11
			binary.LittleEndian.PutUint32(request[4:], flags)
			if _, err = w.transport.Write(request); err != nil {
				return 0, w.fail(err)
			}
			secure := tls.Client(w.raw, trust)
			if err = secure.HandshakeContext(nativeContext{w.ctx}); err != nil {
				return 0, w.fail(err)
			}
			w.tlsState = secure.ConnectionState()
			w.transport = secure
			w.authOffset = 1
			if w.secured != nil {
				w.secured(trust)
				w.secured = nil
			}
		}
	}
	outgoing := frame
	if w.phase == phaseAuth && w.authOffset != 0 {
		outgoing = bytes.Clone(frame)
		outgoing[3] += w.authOffset
		if frame[3] == 1 {
			flags := binary.LittleEndian.Uint32(outgoing[4:]) | 1<<11
			binary.LittleEndian.PutUint32(outgoing[4:], flags)
		}
	} else if w.phase != phaseAuth {
		if len(w.pending) != 0 || frame[3] != 0 {
			return 0, w.fail(failure(ErrProtocol, "command-state"))
		}
		w.command = frame[4]
		w.responseBytes, w.rowCount = 0, 0
		w.fields = nil
		switch w.command {
		case 3, 0x17, 14:
			w.phase = phaseHeader
		case 0x16:
			w.phase = phasePrepare
		case 1, 0x19, 0x18:
			w.phase = phaseIdle
		default:
			return 0, w.fail(failure(ErrUnsupported, "command"))
		}
	}
	count, err := w.transport.Write(outgoing)
	if err == nil && count != len(outgoing) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return count, w.fail(err)
	}
	if w.phase != phaseAuth {
		switch w.command {
		case 0x18:
			w.longDataPending = true
		case 0x17:
			w.longDataPending = false
		}
	}
	return count, nil
}
func lengthNumber(data []byte) (uint64, int, bool) {
	if len(data) == 0 {
		return 0, 0, false
	}
	switch data[0] {
	case 0xfb, 0xff:
		return 0, 0, false
	case 0xfc:
		if len(data) < 3 {
			return 0, 0, false
		}
		return uint64(binary.LittleEndian.Uint16(data[1:])), 3, true
	case 0xfd:
		if len(data) < 4 {
			return 0, 0, false
		}
		return uint64(data[1]) | uint64(data[2])<<8 | uint64(data[3])<<16, 4, true
	case 0xfe:
		if len(data) < 9 {
			return 0, 0, false
		}
		return binary.LittleEndian.Uint64(data[1:]), 9, true
	default:
		return uint64(data[0]), 1, true
	}
}
func statusOK(data []byte) (uint16, bool) {
	_, first, ok := lengthNumber(data[1:])
	if !ok {
		return 0, false
	}
	_, second, ok := lengthNumber(data[1+first:])
	if !ok {
		return 0, false
	}
	pos := 1 + first + second
	if len(data) < pos+4 {
		return 0, false
	}
	return binary.LittleEndian.Uint16(data[pos:]), true
}
func (w *wire) inspect(data []byte) error {
	bad := func() error { return failure(ErrProtocol, "response") }
	if len(data) == 0 {
		return bad()
	}
	if w.phase == phaseAuth {
		if data[0] == 10 && w.version == "" {
			return w.greeting(data)
		}
		switch data[0] {
		case 0:
			if _, ok := statusOK(data); !ok {
				return bad()
			}
			w.phase = phaseIdle
		case 0xff:
			if len(data) < 9 || data[3] != '#' {
				return bad()
			}
			w.phase = phaseIdle
		case 0xfe:
			end := bytes.IndexByte(data[1:], 0)
			if end < 0 || !w.authenticationAllowed(string(data[1:1+end])) || len(data) != end+23 {
				return bad()
			}
		case 1:
			if len(data) == 2 && (data[1] == 3 || data[1] == 4) {
				return nil
			}
			block, rest := pem.Decode(data[1:])
			if block == nil || len(rest) != 0 {
				return bad()
			}
			key, err := x509.ParsePKIXPublicKey(block.Bytes)
			rsaKey, ok := key.(*rsa.PublicKey)
			if err != nil || !ok || rsaKey.N.BitLen() < 2048 || rsaKey.N.BitLen() > 8192 {
				return bad()
			}
		default:
			return bad()
		}
		return nil
	}
	if data[0] == 0xff {
		if w.phase != phaseHeader && w.phase != phaseRows && w.phase != phasePrepare {
			return bad()
		}
		if len(data) < 9 || data[3] != '#' {
			return bad()
		}
		w.phase = phaseIdle
		return nil
	}
	if w.phase == phaseHeader {
		if data[0] == 0xfb {
			return failure(ErrUnsupported, "local-infile")
		}
		if data[0] == 0 {
			status, ok := statusOK(data)
			if !ok {
				return bad()
			}
			if status&8 != 0 {
				return failure(ErrUnsupported, "multiple-results")
			}
			w.inTransaction = status&1 != 0
			w.phase = phaseIdle
			return nil
		}
		if w.command == 14 {
			return bad()
		}
		count, _, ok := lengthNumber(data)
		if !ok || count == 0 {
			return bad()
		}
		if count > MaxColumns {
			return failure(ErrLimit, "columns")
		}
		w.columns, w.remaining = int(count), int(count)
		w.phase = phaseColumns
		return nil
	}
	if w.phase == phasePrepare {
		if data[0] == 0xfb {
			return failure(ErrUnsupported, "local-infile")
		}
		if data[0] != 0 || len(data) < 12 {
			return bad()
		}
		columns := int(binary.LittleEndian.Uint16(data[5:]))
		params := int(binary.LittleEndian.Uint16(data[7:]))
		if columns > MaxColumns || params > MaxArguments {
			return failure(ErrLimit, "prepare-metadata")
		}
		w.columns = columns
		w.phase = phaseIdle
		if params > 0 {
			w.remaining, w.phase = params, phasePrepareParams
		} else if columns > 0 {
			w.remaining, w.phase = columns, phasePrepareColumns
		}
		return nil
	}
	if w.phase == phasePrepareParams || w.phase == phasePrepareColumns {
		if _, ok := columnType(data); !ok {
			return bad()
		}
		w.remaining--
		if w.remaining == 0 {
			w.phase++
		}
		return nil
	}
	if w.phase == phasePrepareParamEnd || w.phase == phasePrepareColumnEnd {
		if data[0] != 0xfe || len(data) != 5 {
			return bad()
		}
		if w.phase == phasePrepareParamEnd && w.columns > 0 {
			w.remaining, w.phase = w.columns, phasePrepareColumns
		} else {
			w.phase = phaseIdle
		}
		return nil
	}
	if w.phase == phaseColumns {
		kind, ok := columnType(data)
		if !ok {
			return bad()
		}
		w.fields = append(w.fields, kind)
		w.remaining--
		if w.remaining == 0 {
			w.phase = phaseColumnEnd
		}
		return nil
	}
	if w.phase == phaseColumnEnd || w.phase == phaseRows && data[0] == 0xfe {
		if data[0] != 0xfe || len(data) != 5 {
			return bad()
		}
		if binary.LittleEndian.Uint16(data[3:])&8 != 0 {
			return failure(ErrUnsupported, "multiple-results")
		}
		w.inTransaction = binary.LittleEndian.Uint16(data[3:])&1 != 0
		if w.phase == phaseColumnEnd {
			w.phase = phaseRows
		} else {
			w.phase = phaseIdle
		}
		return nil
	}
	if w.phase == phaseRows {
		w.rowCount++
		if w.rowCount > w.settings.MaxRows {
			return failure(ErrLimit, "rows")
		}
		if w.command == 0x17 {
			return w.binaryRow(data)
		}
		return w.textRow(data)
	}
	return bad()
}

func (w *wire) textRow(data []byte) error {
	position := 0
	for range w.columns {
		if position == len(data) {
			return failure(ErrProtocol, "text-row")
		}
		if data[position] == 0xfb {
			position++
			continue
		}
		size, count, ok := lengthNumber(data[position:])
		if !ok || size > uint64(len(data)-position-count) {
			return failure(ErrProtocol, "text-row")
		}
		position += count + int(size)
	}
	if position != len(data) {
		return failure(ErrProtocol, "text-row")
	}
	return nil
}
func (w *wire) greeting(data []byte) error {
	end := bytes.IndexByte(data[1:], 0)
	if end < 1 || end > 256 {
		return failure(ErrProtocol, "greeting")
	}
	pos := end + 15
	if len(data) < pos+32 {
		return failure(ErrProtocol, "greeting")
	}
	version := string(data[1 : 1+end])
	flags := uint32(binary.LittleEndian.Uint16(data[pos:])) | uint32(binary.LittleEndian.Uint16(data[pos+5:]))<<16
	required := uint32(1 | 1<<9 | 1<<15 | 1<<19)
	if flags&required != required || !strings.HasPrefix(version, "8.") || strings.Contains(strings.ToLower(version), "mariadb") ||
		!w.authenticationAllowed(string(bytes.TrimSuffix(data[pos+31:], []byte{0}))) || data[pos+7] != 21 {
		return failure(ErrUnsupported, "greeting-profile")
	}
	if !w.settings.Plaintext && flags&(1<<11) == 0 {
		return failure(ErrUnsupported, "tls-required")
	}
	// Negotiate one ordinary MySQL result set and legacy EOF markers. Denial of
	// LOCAL INFILE also happens above on response dispatch, not just this flag.
	flags &^= uint32(1<<7 | 1<<16 | 1<<17 | 1<<24 | 1<<5)
	binary.LittleEndian.PutUint16(data[pos:], uint16(flags))
	binary.LittleEndian.PutUint16(data[pos+5:], uint16(flags>>16))
	w.version = version
	return nil
}
func (w *wire) authenticationAllowed(plugin string) bool {
	return plugin == "caching_sha2_password" || plugin == "mysql_native_password" && w.settings.Authentication == plugin
}
func columnType(data []byte) (byte, bool) {
	position := 0
	for range 6 {
		size, count, ok := lengthNumber(data[position:])
		if !ok || size > uint64(len(data)-position-count) {
			return 0, false
		}
		position += count + int(size)
	}
	if len(data) != position+13 || data[position] != 12 {
		return 0, false
	}
	return data[position+7], true
}
func (w *wire) binaryRow(data []byte) error {
	bad := func() error { return failure(ErrProtocol, "binary-row") }
	bitmap := (w.columns + 9) / 8
	if data[0] != 0 || len(data) < 1+bitmap {
		return bad()
	}
	pos := 1 + bitmap
	for column, kind := range w.fields {
		if data[1+(column+2)/8]&(1<<uint((column+2)%8)) != 0 {
			continue
		}
		size := 0
		switch kind {
		case 6:
			continue
		case 1:
			size = 1
		case 2, 13:
			size = 2
		case 3, 9, 4:
			size = 4
		case 8, 5:
			size = 8
		case 7, 10, 11, 12, 14:
			if pos >= len(data) {
				return bad()
			}
			size = int(data[pos]) + 1
			length := size - 1
			if kind == 11 {
				if length != 0 && length != 8 && length != 12 {
					return bad()
				}
			} else if length != 0 && length != 4 && length != 7 && length != 11 {
				return bad()
			}
		default:
			length, count, ok := lengthNumber(data[pos:])
			if !ok || length > uint64(len(data)-pos-count) {
				return bad()
			}
			size = count + int(length)
		}
		if size > len(data)-pos {
			return bad()
		}
		pos += size
	}
	if pos != len(data) {
		return bad()
	}
	return nil
}
