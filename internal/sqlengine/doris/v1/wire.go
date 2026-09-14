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

package doris

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"
)

const (
	wireGreeting byte = iota
	wireAuth
	wireIdle
	wireHeader
	wireColumns
	wireColumnEnd
	wireRows
)

// wire validates the selected text-only protocol before the native parser.
// It owns TLS upgrade so framing is above encryption, not over ciphertext.
type wire struct {
	raw           net.Conn
	transport     net.Conn
	settings      settings
	ctx           context.Context
	deadline      time.Time
	pending       []byte
	phase         byte
	sequence      byte
	offset        byte
	firstAuth     bool
	columns       int
	remaining     int
	rows          int
	responseBytes int
	version       string
	once          sync.Once
	mu            sync.Mutex
	primary       error
	cleanup       error
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
	return w.primary, w.cleanup
}
func (w *wire) Close() error {
	w.once.Do(func() { err := w.raw.Close(); w.mu.Lock(); w.cleanup = err; w.mu.Unlock() })
	_, err := w.evidence()
	return err
}
func (w *wire) LocalAddr() net.Addr  { return w.raw.LocalAddr() }
func (w *wire) RemoteAddr() net.Addr { return w.raw.RemoteAddr() }
func (w *wire) clip(value time.Time) time.Time {
	if !w.deadline.IsZero() && (value.IsZero() || w.deadline.Before(value)) {
		return w.deadline
	}
	return value
}
func (w *wire) SetDeadline(value time.Time) error      { return w.raw.SetDeadline(w.clip(value)) }
func (w *wire) SetReadDeadline(value time.Time) error  { return w.raw.SetReadDeadline(w.clip(value)) }
func (w *wire) SetWriteDeadline(value time.Time) error { return w.raw.SetWriteDeadline(w.clip(value)) }

func (w *wire) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if len(w.pending) == 0 {
		var header [4]byte
		if _, err := io.ReadFull(w.transport, header[:]); err != nil {
			return 0, w.fail(err)
		}
		size := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
		if size == 0 || size > w.settings.MaxPacketBytes || size+4 > w.settings.MaxResponseBytes-w.responseBytes {
			return 0, w.fail(failure(ErrLimit, "packet"))
		}
		if header[3] != w.sequence {
			return 0, w.fail(failure(ErrProtocol, "sequence"))
		}
		w.sequence++
		w.responseBytes += size + 4
		frame := make([]byte, size+4)
		copy(frame, header[:])
		if _, err := io.ReadFull(w.transport, frame[4:]); err != nil {
			return 0, w.fail(err)
		}
		wasAuth := w.phase == wireAuth
		if err := w.inspect(frame[4:]); err != nil {
			return 0, w.fail(err)
		}
		if wasAuth {
			frame[3] -= w.offset
		}
		w.pending = frame
	}
	count := copy(dst, w.pending)
	w.pending = w.pending[count:]
	return count, nil
}
func (w *wire) Write(frame []byte) (int, error) {
	// A single-use connection is retired locally, never drained or returned to
	// a pool. Native Close still runs to release the driver's context watcher.
	if len(frame) == 5 && frame[3] == 0 && frame[4] == 1 {
		return len(frame), w.Close()
	}
	if len(frame) < 4 || len(frame)-4 != int(frame[0])|int(frame[1])<<8|int(frame[2])<<16 || len(w.pending) != 0 ||
		len(frame) == 4 && (w.phase != wireAuth || !w.firstAuth) {
		return 0, w.fail(failure(ErrProtocol, "write-frame"))
	}
	outgoing := frame
	if w.phase == wireAuth {
		if !w.firstAuth {
			w.firstAuth = true
			if len(frame) < 36 {
				return 0, w.fail(failure(ErrProtocol, "auth-response"))
			}
			if !w.settings.Plaintext {
				config, err := w.settings.trust()
				if err != nil {
					return 0, w.fail(err)
				}
				config.ServerName = w.settings.SQLServerName
				request := make([]byte, 36)
				copy(request[4:], frame[4:36])
				request[0], request[3] = 32, 1
				binary.LittleEndian.PutUint32(request[4:], binary.LittleEndian.Uint32(request[4:])|1<<11)
				if count, err := w.raw.Write(request); err != nil || count != len(request) {
					return 0, w.fail(joined(ErrTransport, "tls-request", err, io.ErrShortWrite))
				}
				secure := tls.Client(w.raw, config)
				if err := secure.HandshakeContext(w.ctx); err != nil {
					return 0, w.fail(err)
				}
				w.transport, w.offset = secure, 1
				w.sequence++
			}
		}
		outgoing = bytes.Clone(frame)
		outgoing[3] += w.offset
		if frame[3] == 1 && w.offset != 0 {
			binary.LittleEndian.PutUint32(outgoing[4:], binary.LittleEndian.Uint32(outgoing[4:])|1<<11)
		}
		if outgoing[3] != w.sequence {
			return 0, w.fail(failure(ErrProtocol, "auth-sequence"))
		}
		w.sequence++
	} else {
		if w.phase != wireIdle || frame[3] != 0 || (frame[4] != 3 && frame[4] != 1) {
			return 0, w.fail(failure(ErrUnsupported, "command"))
		}
		w.sequence = 1
		if frame[4] == 3 {
			w.phase = wireHeader
		}
	}
	count, err := w.transport.Write(outgoing)
	if err == nil && count != len(outgoing) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return count, w.fail(err)
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
func okStatus(data []byte) (uint16, bool) {
	if len(data) == 0 || data[0] != 0 {
		return 0, false
	}
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
	if data[0] == 0xff {
		if (w.phase != wireGreeting && w.phase != wireAuth && w.phase != wireHeader && w.phase != wireRows) || len(data) < 9 || data[3] != '#' {
			return bad()
		}
		w.phase = wireIdle
		return nil
	}
	if w.phase == wireGreeting {
		return w.greeting(data)
	}
	if w.phase == wireAuth {
		switch data[0] {
		case 0:
			if _, ok := okStatus(data); !ok {
				return bad()
			}
			w.phase = wireIdle
		case 0xfe:
			plugin := []byte("mysql_native_password")
			if len(data) != len(plugin)+23 || !bytes.Equal(data[1:1+len(plugin)], plugin) || data[len(plugin)+1] != 0 {
				return bad()
			}
		default:
			return failure(ErrUnsupported, "authentication")
		}
		return nil
	}
	switch w.phase {
	case wireHeader:
		if data[0] == 0xfb {
			return failure(ErrUnsupported, "local-infile")
		}
		if data[0] == 0 {
			status, ok := okStatus(data)
			if !ok {
				return bad()
			}
			if status&8 != 0 {
				return failure(ErrUnsupported, "multiple-results")
			}
			w.phase = wireIdle
			return nil
		}
		count, size, ok := lengthNumber(data)
		if !ok || count == 0 || size != len(data) {
			return bad()
		}
		if count > MaxColumns {
			return failure(ErrLimit, "columns")
		}
		w.columns, w.remaining, w.phase = int(count), int(count), wireColumns
	case wireColumns:
		pos := 0
		for range 6 {
			count, size, ok := lengthNumber(data[pos:])
			if !ok || count > uint64(len(data)-pos-size) {
				return bad()
			}
			pos += size + int(count)
		}
		if len(data) != pos+13 || data[pos] != 12 {
			return bad()
		}
		w.remaining--
		if w.remaining == 0 {
			w.phase = wireColumnEnd
		}
	case wireColumnEnd, wireRows:
		if data[0] == 0xfe {
			if len(data) != 5 {
				return bad()
			}
			if binary.LittleEndian.Uint16(data[3:])&8 != 0 {
				return failure(ErrUnsupported, "multiple-results")
			}
			if w.phase == wireColumnEnd {
				w.phase = wireRows
			} else {
				w.phase = wireIdle
			}
			return nil
		}
		if w.phase == wireColumnEnd {
			return bad()
		}
		w.rows++
		if w.rows > w.settings.MaxRows {
			return failure(ErrLimit, "rows")
		}
		pos := 0
		for range w.columns {
			if pos == len(data) {
				return bad()
			}
			if data[pos] == 0xfb {
				pos++
				continue
			}
			count, size, ok := lengthNumber(data[pos:])
			if !ok || count > uint64(len(data)-pos-size) {
				return bad()
			}
			pos += size + int(count)
		}
		if pos != len(data) {
			return bad()
		}
	default:
		return bad()
	}
	return nil
}
func (w *wire) greeting(data []byte) error {
	if len(data) == 0 || data[0] != 10 {
		return failure(ErrUnsupported, "greeting")
	}
	end := bytes.IndexByte(data[1:], 0)
	pos := end + 15
	if end < 1 || end > 256 || len(data) < pos+32 {
		return failure(ErrProtocol, "greeting")
	}
	flags := uint32(binary.LittleEndian.Uint16(data[pos:])) | uint32(binary.LittleEndian.Uint16(data[pos+5:]))<<16
	required := uint32(1<<9 | 1<<15 | 1<<19)
	if flags&required != required || data[pos+7] != 21 || !bytes.Equal(data[pos+31:], []byte("mysql_native_password\x00")) {
		return failure(ErrUnsupported, "greeting-profile")
	}
	// Without CLIENT_LONG_PASSWORD the driver reads reserved bytes as MariaDB
	// extensions, including a result layout this framing boundary does not own.
	for _, value := range data[pos+8 : pos+18] {
		if value != 0 {
			return failure(ErrUnsupported, "greeting-extensions")
		}
	}
	if !w.settings.Plaintext && flags&(1<<11) == 0 {
		return failure(ErrUnsupported, "tls-required")
	}
	flags &^= uint32(1<<5 | 1<<7 | 1<<16 | 1<<17 | 1<<24)
	binary.LittleEndian.PutUint16(data[pos:], uint16(flags))
	binary.LittleEndian.PutUint16(data[pos+5:], uint16(flags>>16))
	w.version, w.phase = string(data[1:1+end]), wireAuth
	return nil
}
