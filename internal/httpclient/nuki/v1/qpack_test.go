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

package nuki

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	nativetls "github.com/nukilabs/utls"
	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/quicvarint"
)

// The peer sends a valid blocked QPACK section and leaves its encoder stream
// open without the required insert. A request cancellation must end only that
// request, not wait for connection retirement or another stream's instructions.
func TestProviderCancelsBlockedDynamicQPACK(t *testing.T) {
	certificate := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer certificate.Close()
	listener, err := quic.ListenAddr("127.0.0.1:0", &tls.Config{Certificates: certificate.TLS.Certificates, NextProtos: []string{"h3"}}, &quic.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	fixtureCtx, stopFixture := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopFixture()
	sent := make(chan *quic.Conn, 1)
	peerDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept(fixtureCtx)
		if err != nil {
			peerDone <- err
			return
		}
		control, err := conn.OpenUniStreamSync(fixtureCtx)
		if err != nil {
			peerDone <- err
			return
		}
		if _, err := control.Write([]byte{0, 4, 0}); err != nil {
			peerDone <- err
			return
		}
		encoder, err := conn.OpenUniStreamSync(fixtureCtx)
		if err != nil {
			peerDone <- err
			return
		}
		if _, err := encoder.Write([]byte{2}); err != nil {
			peerDone <- err
			return
		}
		request, err := conn.AcceptStream(fixtureCtx)
		if err != nil {
			peerDone <- err
			return
		}
		reader := quicvarint.NewReader(request)
		kind, err := quicvarint.Read(reader)
		if err != nil || kind != 1 {
			peerDone <- io.ErrUnexpectedEOF
			return
		}
		size, err := quicvarint.Read(reader)
		if err != nil || size > 64<<10 {
			peerDone <- io.ErrUnexpectedEOF
			return
		}
		if _, err := io.CopyN(io.Discard, request, int64(size)); err != nil {
			peerDone <- err
			return
		}
		block := []byte{2, 0, 0xd9, 0x80}
		frame := quicvarint.Append(nil, 1)
		frame = quicvarint.Append(frame, uint64(len(block)))
		frame = append(frame, block...)
		if _, err := request.Write(frame); err != nil {
			peerDone <- err
			return
		}
		sent <- conn
		second, err := conn.AcceptStream(fixtureCtx)
		if err != nil {
			peerDone <- nil
			return
		}
		reader = quicvarint.NewReader(second)
		kind, err = quicvarint.Read(reader)
		if err != nil || kind != 1 {
			peerDone <- io.ErrUnexpectedEOF
			return
		}
		size, err = quicvarint.Read(reader)
		if err != nil || size > 64<<10 {
			peerDone <- io.ErrUnexpectedEOF
			return
		}
		if _, err := io.CopyN(io.Discard, second, int64(size)); err != nil {
			peerDone <- err
			return
		}
		instructions := append([]byte{0x3f, 0x21, 0x47}, []byte("x-qpack")...)
		instructions = append(instructions, 5)
		instructions = append(instructions, []byte("value")...)
		if _, err := encoder.Write(instructions); err != nil {
			peerDone <- err
			return
		}
		if _, err := second.Write(append(frame, 0, 2, 'o', 'k')); err != nil {
			peerDone <- err
			return
		}
		if err := second.Close(); err != nil {
			peerDone <- err
			return
		}
		<-conn.Context().Done()
		peerDone <- nil
	}()
	options := providerOptions()
	options.Mode = HTTP3Only
	options.Native.TLS = &nativetls.Config{InsecureSkipVerify: true}
	fixture := bindProvider(t, options)
	requestCtx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	receipt, err := fixture.client.Do(requestCtx, fault.Correlation{Call: "qpack-cancel"}, nativeRequest(t, "GET", "https://"+listener.Addr().String(), nil))
	if err != nil {
		t.Fatal(err)
	}
	var peer *quic.Conn
	select {
	case peer = <-sent:
	case err := <-peerDone:
		t.Fatal("native peer failed", err)
	case <-fixtureCtx.Done():
		t.Fatal("native peer did not send blocked headers")
	}
	defer peer.CloseWithError(0, "")
	time.Sleep(20 * time.Millisecond)
	cancel()
	waiting, stop := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer stop()
	_, waitErr := receipt.WaitReleased(waiting)
	if waitErr != nil {
		t.Error("blocked QPACK decoding survived request cancellation")
	}
	// Release the withholding peer even when the rejecting control fails, so
	// original SDK failures do not turn into hanging test cleanup.
	if waitErr != nil {
		_ = peer.CloseWithError(0, "")
	}
	result := outcome(t, fixture, receipt)
	if result.Err() == nil || result.Outcome.Value.Complete() {
		t.Error("blocked request became complete")
	}
	if waitErr == nil {
		next, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "qpack-survivor"}, nativeRequest(t, "GET", "https://"+listener.Addr().String(), nil))
		if err != nil {
			t.Fatal(err)
		}
		healthy := outcome(t, fixture, next)
		if healthy.Err() != nil || !healthy.Outcome.Value.Complete() || string(healthy.Outcome.Value.DataCopy()) != "ok" || healthy.Outcome.Value.Metadata().HeadersCopy().Get("X-Qpack") != "value" {
			t.Error("cancellation broke the shared connection or dynamic decoding", healthy.Err())
		}
	}
	_ = peer.CloseWithError(0, "")
	select {
	case err := <-peerDone:
		if err != nil {
			t.Error(err)
		}
	case <-fixtureCtx.Done():
		t.Error("peer cleanup incomplete")
	}
}
