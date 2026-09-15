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

package transport

import (
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	http "github.com/sardanioss/http"
	"github.com/sardanioss/httpcloak/dns"
	"github.com/sardanioss/httpcloak/fingerprint"
	http2 "github.com/sardanioss/net/http2"
)

const FathomryCompatibilityRevision = "v1"

// FathomryErrHeaderLimit identifies a managed send/receive header budget failure.
var FathomryErrHeaderLimit = errors.New("httpcloak: managed header limit")

// FathomryErrRequestLength identifies inconsistent declared request framing.
var FathomryErrRequestLength = errors.New("httpcloak: request body length mismatch")

// FathomryTransport owns one immutable protocol transport. The Provider must
// serialize checkouts through response/input completion and retire failed uses.
// It is a composition capability, never a consumer-facing native handle.
type FathomryTransport struct {
	closeDone chan struct{}
	retryMu   sync.Mutex
	h1        *HTTP1Transport
	h2        *HTTP2Transport
	h3        *HTTP3Transport
	preset    *fingerprint.Preset
	config    *TransportConfig
	protocol  Protocol
	tlsOnly   bool
	closeOnce sync.Once
	closeErr  error
}

// NewFathomryTransport consumes a private preset instead of registering a global
// name. The configuration must already own its containers and borrowed hooks.
func NewFathomryTransport(preset *fingerprint.Preset, config *TransportConfig, protocol Protocol, verify *TLSVerify, insecure bool, disableECH bool, maxHeaders int64) (*FathomryTransport, error) {
	if preset == nil || config == nil || maxHeaders < 1024 || maxHeaders > 64<<20 ||
		protocol < ProtocolHTTP1 || protocol > ProtocolHTTP3 {
		return nil, errors.New("httpcloak: invalid managed transport")
	}
	if config.SessionCacheBackend != nil || config.SessionCacheErrorCallback != nil {
		return nil, errors.New("httpcloak: unmanaged distributed session cache")
	}
	preset = fingerprint.Clone(preset)
	if len(preset.HeaderOrder) > 0 {
		preset.Headers = nil
	}
	for key := range preset.Headers {
		if strings.EqualFold(key, "Authorization") || strings.EqualFold(key, "Cookie") {
			delete(preset.Headers, key)
		}
	}
	kept := preset.HeaderOrder[:0]
	for _, entry := range preset.HeaderOrder {
		if !strings.EqualFold(entry.Key, "Authorization") && !strings.EqualFold(entry.Key, "Cookie") {
			kept = append(kept, entry)
		}
	}
	preset.HeaderOrder = kept
	copied := *config
	if copied.KeyLogWriter == nil {
		copied.KeyLogWriter = io.Discard
	}
	managed := &FathomryTransport{preset: preset, config: &copied, protocol: protocol, tlsOnly: copied.TLSOnly, closeDone: make(chan struct{})}
	copied.TLSOnly = true
	copied.FathomryMaxHeaderBytes = maxHeaders
	cache := dns.NewCache()
	switch protocol {
	case ProtocolHTTP1:
		managed.h1 = NewHTTP1TransportWithConfig(preset, cache, nil, &copied)
		managed.h1.SetTLSVerify(verify)
		managed.h1.SetInsecureSkipVerify(insecure)
	case ProtocolHTTP2:
		if preset.DisableHTTP2 {
			return nil, ErrProtocol
		}
		managed.h2 = NewHTTP2TransportWithConfig(preset, cache, nil, &copied)
		managed.h2.SetTLSVerify(verify)
		managed.h2.SetInsecureSkipVerify(insecure)
	case ProtocolHTTP3:
		if !preset.SupportHTTP3 {
			return nil, ErrProtocol
		}
		var err error
		managed.h3, err = NewHTTP3TransportWithTransportConfig(preset, cache, &copied)
		if err != nil {
			return nil, err
		}
		managed.h3.SetTLSVerify(verify)
		managed.h3.SetInsecureSkipVerify(insecure)
		managed.h3.SetDisableECH(disableECH)
		managed.h3.transport.DisableCompression = true
		managed.h3.transport.MaxResponseHeaderBytes = int(maxHeaders)
	}
	return managed, nil
}

// RoundTrip retains native header ordering and fingerprint behavior but returns
// the wire body, before decompression. No Session cookies, cache or business
// retries are installed. The caller retains every request body and replay reader.
func (managed *FathomryTransport) RoundTrip(request *http.Request, headers *Request) (*http.Response, error) {
	if request == nil || request.URL == nil || headers == nil {
		return nil, ErrRequest
	}
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	copied := request.Clone(request.Context())
	copied.Header = make(http.Header)
	input := *headers
	input.Headers = request.Header
	tlsOnly := managed.tlsOnly
	if input.TLSOnly != nil {
		tlsOnly = *input.TLSOnly
	}
	protocol := [...]string{"", "h1", "h2", "h3"}[managed.protocol]
	applyPresetHeaders(copied, managed.preset, input.HeaderOrder, managed.config.CustomPseudoOrder, tlsOnly, protocol, input.Headers, input.DisableClientHints, input.ExactHeaders)
	mergeCallerHeaders(copied, &input)
	var headerBytes int64
	for key, values := range copied.Header {
		if key == http.HeaderOrderKey || key == http.PHeaderOrderKey || key == h1HeaderCasingKey || key == exactHeadersKey {
			continue
		}
		for _, value := range values {
			headerBytes += int64(len(key) + len(value) + 4)
		}
	}
	if headerBytes > managed.config.FathomryMaxHeaderBytes {
		return nil, FathomryErrHeaderLimit
	}
	if managed.protocol != ProtocolHTTP1 && (tlsOnly || len(input.ExactHeaders) > 0) {
		if _, present := copied.Header["User-Agent"]; !present {
			copied.Header["User-Agent"] = []string{""}
			copied.Header[http.HeaderOrderKey] = append(copied.Header[http.HeaderOrderKey], "user-agent")
		}
	}
	var response *http.Response
	var err error
	switch managed.protocol {
	case ProtocolHTTP1:
		response, err = managed.h1.RoundTrip(copied)
	case ProtocolHTTP2:
		if len(input.ExactHeaders) > 0 {
			copied = http2.FathomryWithExactHeaders(copied)
		}
		response, err = managed.h2.RoundTrip(copied)
	case ProtocolHTTP3:
		// The high-level header pipeline already ran, and the request is private.
		// No shared Setter, protocol-racing leg or unbounded status retry runs.
		if len(input.ExactHeaders) == 0 && !managed.preset.H2DisableCookieSplit() {
			var crumbs []string
			for _, value := range copied.Header.Values("Cookie") {
				crumbs = append(crumbs, crumbleCookie(value)...)
			}
			if len(crumbs) > 0 {
				copied.Header["Cookie"] = crumbs
			}
		}
		response, err = managed.h3.transport.RoundTrip(copied)
	}
	var mismatch *ALPNMismatchError
	if errors.As(err, &mismatch) && mismatch.TLSConn != nil {
		owned := mismatch.TLSConn
		mismatch.TLSConn = nil
		err = errors.Join(err, owned.Close())
	}
	return response, err
}

// FathomryHeaderMetadata separates native bookkeeping from actual wire fields.
func FathomryHeaderMetadata(header http.Header) (http.Header, []string, []string) {
	copy := header.Clone()
	order := append([]string(nil), copy[http.HeaderOrderKey]...)
	casing := append([]string(nil), copy[h1HeaderCasingKey]...)
	delete(copy, http.HeaderOrderKey)
	delete(copy, http.PHeaderOrderKey)
	delete(copy, h1HeaderCasingKey)
	delete(copy, exactHeadersKey)
	return copy, order, casing
}

// Close joins native cleanup after all checkouts end. The owning Provider uses
// an independently bounded wait; a timeout must retain this cleanup obligation.
func (managed *FathomryTransport) Close() error {
	managed.closeOnce.Do(func() {
		defer close(managed.closeDone)
		if managed.h1 != nil {
			managed.h1.Close()
			<-managed.h1.cleanupDone
			managed.h1.cleanupJobs.Wait()
		}
		if managed.h2 != nil {
			managed.h2.Close()
			<-managed.h2.cleanupDone
			managed.h2.cleanupJobs.Wait()
		}
		if managed.h3 != nil {
			managed.closeErr = managed.h3.Close()
		}
	})
	managed.retryMu.Lock()
	defer managed.retryMu.Unlock()
	if managed.h3 != nil && !managed.ResourcesReleased() {
		managed.closeErr = errors.Join(managed.closeErr, managed.h3.quicTransport.Conn.Close())
	}
	return managed.closeErr
}

// ResourcesReleased observes shutdown completion and, for direct H3, the actual
// owned socket's closed state. An error return alone does not supply this fact.
func (managed *FathomryTransport) ResourcesReleased() bool {
	select {
	case <-managed.closeDone:
	default:
		return false
	}
	if managed.h3 == nil {
		return true
	}
	return errors.Is(managed.h3.quicTransport.Conn.SetReadDeadline(time.Time{}), net.ErrClosed)
}
func (transport *HTTP1Transport) fathomryCleanup(action func()) {
	transport.cleanupJobs.Add(1)
	go func() { defer transport.cleanupJobs.Done(); action() }()
}
func (transport *HTTP2Transport) fathomryCleanup(action func()) {
	transport.cleanupJobs.Add(1)
	go func() { defer transport.cleanupJobs.Done(); action() }()
}

type headerLimitReader struct {
	reader    io.Reader
	remaining int64
	ending    uint32
	finished  bool
}

func (reader *headerLimitReader) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	if reader.finished {
		return reader.reader.Read(buffer)
	}
	if reader.remaining == 0 {
		return 0, FathomryErrHeaderLimit
	}
	if int64(len(buffer)) > reader.remaining {
		buffer = buffer[:reader.remaining]
	}
	buffer = buffer[:1]
	count, err := reader.reader.Read(buffer)
	for _, char := range buffer[:count] {
		reader.ending = reader.ending<<8 | uint32(char)
		reader.remaining--
		if reader.ending == 0x0d0a0d0a {
			reader.finished = true
			break
		}
	}
	return count, err
}

type echFailures struct {
	mu    sync.Mutex
	until map[string]time.Time
}

func (state *echFailures) disabled(config *TransportConfig, host string) bool {
	if config == nil || config.FathomryMaxHeaderBytes == 0 {
		return dns.IsECHIncompatible(host)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	until, ok := state.until[host]
	if ok && !time.Now().Before(until) {
		delete(state.until, host)
		return false
	}
	return ok
}
func (state *echFailures) mark(config *TransportConfig, host string) {
	if config == nil || config.FathomryMaxHeaderBytes == 0 {
		dns.MarkECHIncompatible(host)
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.until == nil {
		state.until = make(map[string]time.Time)
	}
	state.until[host] = time.Now().Add(10 * time.Minute)
}
