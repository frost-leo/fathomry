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
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	nativehttp "github.com/nukilabs/http"
	"github.com/nukilabs/tlsclient/profiles"
	nativetls "github.com/nukilabs/utls"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

type priorityObservation struct {
	present    bool
	weight     uint8
	exclusive  bool
	dependency uint32
	header     string
	fields     []string
}

func TestPublicPriorityHasIndependentH2WireEffect(t *testing.T) {
	certificate := httptest.NewTLSServer(nil)
	t.Cleanup(certificate.Close)
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := tls.NewListener(raw, &tls.Config{Certificates: certificate.TLS.Certificates, NextProtos: []string{"h2"}})
	t.Cleanup(func() { _ = listener.Close() })
	observations := make(chan priorityObservation, 2)
	peerDone := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			peerDone <- err
			return
		}
		defer connection.Close()
		_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
		peerDone <- servePriorityPeer(connection, observations)
	}()
	profile := profiles.Chrome120
	roots := x509.NewCertPool()
	roots.AddCert(certificate.Certificate())
	value := testSettings("priority-wire")
	value.Mode = pointer(HTTP2Negotiated)
	native := NativeOptions{Profile: &profile, TLS: &nativetls.Config{RootCAs: roots}}
	owner, dependencies := testOwner(t, value, native)
	for _, test := range []struct {
		priority nativehttp.Priority
		weight   uint8
		header   string
	}{
		{nativehttp.PriorityHighest, 255, "u=0"},
		{nativehttp.PriorityLowest, 109, "u=4"},
	} {
		request := nativeRequest(t, "GET", "https://"+raw.Addr().String()+"/", nil)
		request.SetPriority(test.priority, false)
		request.Header["X-First"] = []string{"one"}
		request.Header["X-Last"] = []string{"two"}
		request.Header[nativehttp.HeaderOrderKey] = []string{"x-last", "priority", "x-first"}
		receipt, err := owner.Client().Do(testContext(t), request)
		result, err := publicResult(t, dependencies, receipt, err)
		if err != nil || !result.Complete() || result.Metadata().Protocol() != "HTTP/2.0" || string(result.DataCopy()) != "ok" {
			select {
			case peerErr := <-peerDone:
				t.Log("independent peer:", peerErr)
			default:
			}
			t.Fatal("native H2 priority request failed", err)
		}
		select {
		case observed := <-observations:
			if !observed.present || observed.weight != test.weight || !observed.exclusive || observed.dependency != 0 || observed.header != test.header {
				t.Fatalf("native request priority lacked H2 wire effect: %+v, want weight=%d header=%q", observed, test.weight, test.header)
			}
			if len(observed.fields) < 4 || !slices.Equal(observed.fields[:4], profile.PseudoHeaderOrder) {
				t.Fatalf("native pseudo-header order changed on the wire: %q", observed.fields)
			}
			last, priority, first := slices.Index(observed.fields, "x-last"), slices.Index(observed.fields, "priority"), slices.Index(observed.fields, "x-first")
			if last < 4 || priority <= last || first <= priority {
				t.Fatalf("native requested header order changed on the wire: %q", observed.fields)
			}
		case <-testContext(t).Done():
			t.Fatal("independent priority observation missing")
		}
	}
	select {
	case err := <-peerDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-testContext(t).Done():
		t.Fatal("priority peer did not finish")
	}
}

func servePriorityPeer(connection net.Conn, observations chan<- priorityObservation) error {
	preface := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(connection, preface); err != nil {
		return err
	}
	if string(preface) != http2.ClientPreface {
		return errors.New("independent peer received no H2 preface")
	}
	framer := http2.NewFramer(connection, connection)
	if err := framer.WriteSettings(); err != nil {
		return err
	}
	decoder := hpack.NewDecoder(4096, nil)
	for received := 0; received < 2; {
		frame, err := framer.ReadFrame()
		if err != nil {
			return err
		}
		switch frame := frame.(type) {
		case *http2.SettingsFrame:
			if !frame.IsAck() {
				if err := framer.WriteSettingsAck(); err != nil {
					return err
				}
			}
		case *http2.HeadersFrame:
			observation := priorityObservation{present: frame.HasPriority(), weight: frame.Priority.Weight, exclusive: frame.Priority.Exclusive, dependency: frame.Priority.StreamDep}
			stream := frame.StreamID
			block := append([]byte(nil), frame.HeaderBlockFragment()...)
			ended := frame.HeadersEnded()
			for !ended {
				next, err := framer.ReadFrame()
				if err != nil {
					return err
				}
				continuation, ok := next.(*http2.ContinuationFrame)
				if !ok || continuation.StreamID != stream {
					return errors.New("invalid request continuation")
				}
				block = append(block, continuation.HeaderBlockFragment()...)
				ended = continuation.HeadersEnded()
				if len(block) > 64<<10 {
					return errors.New("oversized fixture header block")
				}
			}
			fields, err := decoder.DecodeFull(block)
			if err != nil {
				return fmt.Errorf("independent HPACK decode: %w", err)
			}
			for _, field := range fields {
				observation.fields = append(observation.fields, field.Name)
				if field.Name == "priority" {
					observation.header = field.Value
				}
			}
			observations <- observation
			var response bytes.Buffer
			encoder := hpack.NewEncoder(&response)
			if err := encoder.WriteField(hpack.HeaderField{Name: ":status", Value: "200"}); err != nil {
				return err
			}
			if err := encoder.WriteField(hpack.HeaderField{Name: "content-length", Value: "2"}); err != nil {
				return err
			}
			if err := framer.WriteHeaders(http2.HeadersFrameParam{StreamID: stream, BlockFragment: response.Bytes(), EndHeaders: true}); err != nil {
				return err
			}
			if err := framer.WriteData(stream, true, []byte("ok")); err != nil {
				return err
			}
			received++
		}
	}
	return nil
}
