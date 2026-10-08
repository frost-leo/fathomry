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

package httpcloak

import (
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/sardanioss/httpcloak/fingerprint"
	"github.com/sardanioss/httpcloak/transport"
	nativeh2 "github.com/sardanioss/net/http2"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

func TestH2ExplicitZeroCreditAndHeaderTableAreEffective(t *testing.T) {
	for _, scenario := range []struct {
		name           string
		window, table  uint32
		body           bool
		failure        bool
		tableViolation bool
		failureCode    nativeh2.ErrCode
	}{
		{name: "zero-empty-control"},
		{name: "positive-credit-control", window: 8, table: 64, body: true},
		{name: "zero-credit-refusal", body: true, failure: true, failureCode: nativeh2.ErrCodeFlowControl},
		{name: "zero-table-refusal", window: 8, failure: true, tableViolation: true, failureCode: nativeh2.ErrCodeCompression},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			certificate, roots := protocolCertificate(t)
			listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{certificate}, NextProtos: []string{"h2"}})
			if err != nil {
				t.Fatal(err)
			}
			settings := make(chan map[http2.SettingID]uint32, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				preface := make([]byte, len(http2.ClientPreface))
				if _, err := io.ReadFull(conn, preface); err != nil || string(preface) != http2.ClientPreface {
					return
				}
				framer := http2.NewFramer(conn, conn)
				framer.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
				if err := framer.WriteSettings(); err != nil {
					return
				}
				for {
					frame, err := framer.ReadFrame()
					if err != nil {
						return
					}
					switch frame := frame.(type) {
					case *http2.SettingsFrame:
						if frame.IsAck() {
							continue
						}
						values := make(map[http2.SettingID]uint32)
						_ = frame.ForeachSetting(func(setting http2.Setting) error { values[setting.ID] = setting.Val; return nil })
						settings <- values
						if err := framer.WriteSettingsAck(); err != nil {
							return
						}
					case *http2.MetaHeadersFrame:
						var block bytes.Buffer
						encoder := hpack.NewEncoder(&block)
						table := scenario.table
						if scenario.tableViolation {
							table = 1
						}
						encoder.SetMaxDynamicTableSizeLimit(table)
						encoder.SetMaxDynamicTableSize(table)
						status := "204"
						if scenario.body {
							status = "200"
						}
						_ = encoder.WriteField(hpack.HeaderField{Name: ":status", Value: status})
						if scenario.body {
							_ = encoder.WriteField(hpack.HeaderField{Name: "content-length", Value: "1"})
						}
						if err := framer.WriteHeaders(http2.HeadersFrameParam{StreamID: frame.StreamID, BlockFragment: block.Bytes(), EndHeaders: true, EndStream: !scenario.body}); err != nil {
							return
						}
						if scenario.body {
							if err := framer.WriteData(frame.StreamID, true, []byte("a")); err != nil {
								return
							}
						}
					}
				}
			}()
			t.Cleanup(func() {
				_ = listener.Close()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("raw H2 peer did not stop")
				}
			})
			options := OptionsV1{Name: "zero", PresetName: "chrome-148", Protocol: HTTP2, Native: NativeOptionsV1{Verify: &transport.TLSVerify{RootCAs: roots}, Transport: &transport.TransportConfig{CustomH2Settings: &fingerprint.HTTP2Settings{InitialWindowSize: scenario.window, HeaderTableSize: scenario.table}}}}
			fixture := bindFixture(t, options, 1)
			receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "credit"}, request(t, "GET", "https://"+listener.Addr().String(), nil))
			result := settle(t, fixture, receipt)
			select {
			case values := <-settings:
				if window, present := values[http2.SettingInitialWindowSize]; !present || window != scenario.window {
					t.Fatal("wire stream credit differs", values)
				}
				if table, present := values[http2.SettingHeaderTableSize]; !present || table != scenario.table {
					t.Fatal("wire table capacity differs", values)
				}
			case <-time.After(time.Second):
				t.Fatal("native SETTINGS not observed")
			}
			if scenario.failure {
				var connectionError nativeh2.ConnectionError
				if err == nil || result.Outcome.Value.Complete() || !errors.As(result.Err(), &connectionError) || connectionError != nativeh2.ConnectionError(scenario.failureCode) {
					t.Fatal("zero advertised limit lacked the exact native protocol refusal", err, result.Err(), connectionError)
				}
			} else if err != nil || !result.Outcome.Value.Complete() {
				t.Fatal("valid zero/positive native credit rejected", err, result.Err())
			}
		})
	}
}

func TestH2ConnectionCreditMatchesAdvertisedWindow(t *testing.T) {
	const nativeDefaultIncrement = 15663105
	for _, scenario := range []struct {
		name      string
		increment uint32
		payload   int
		failure   bool
	}{
		{name: "zero-keeps-native-default", payload: 8},
		{name: "small-exact-credit", increment: 1, payload: 65536},
		{name: "small-one-over-credit", increment: 1, payload: 65537, failure: true},
		{name: "larger-legal-credit", increment: 16 << 20, payload: nativeDefaultIncrement + 65536},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			certificate, roots := protocolCertificate(t)
			listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{certificate}, NextProtos: []string{"h2"}})
			if err != nil {
				t.Fatal(err)
			}
			type observation struct {
				increment uint32
				ack       bool
				err       error
			}
			observed, done := make(chan observation, 1), make(chan struct{})
			go func() {
				defer close(done)
				var facts observation
				reported := false
				defer func() {
					if !reported {
						observed <- facts
					}
				}()
				conn, err := listener.Accept()
				if err != nil {
					facts.err = err
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
				preface := make([]byte, len(http2.ClientPreface))
				if _, facts.err = io.ReadFull(conn, preface); facts.err != nil || string(preface) != http2.ClientPreface {
					return
				}
				framer := http2.NewFramer(conn, conn)
				framer.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
				if facts.err = framer.WriteSettings(); facts.err != nil {
					return
				}
				var streamID uint32
				for streamID == 0 {
					frame, err := framer.ReadFrame()
					if err != nil {
						facts.err = err
						return
					}
					switch frame := frame.(type) {
					case *http2.SettingsFrame:
						if !frame.IsAck() {
							if facts.err = framer.WriteSettingsAck(); facts.err != nil {
								return
							}
						}
					case *http2.WindowUpdateFrame:
						if frame.StreamID == 0 {
							facts.increment = frame.Increment
						}
					case *http2.MetaHeadersFrame:
						streamID = frame.StreamID
					}
				}
				var header bytes.Buffer
				encoder := hpack.NewEncoder(&header)
				_ = encoder.WriteField(hpack.HeaderField{Name: ":status", Value: "200"})
				_ = encoder.WriteField(hpack.HeaderField{Name: "content-length", Value: strconv.Itoa(scenario.payload)})
				if facts.err = framer.WriteHeaders(http2.HeadersFrameParam{StreamID: streamID, BlockFragment: header.Bytes(), EndHeaders: true}); facts.err != nil {
					return
				}
				data := bytes.Repeat([]byte{'a'}, 16<<10)
				for remaining := scenario.payload; remaining > 0; {
					count := min(remaining, len(data))
					if facts.err = framer.WriteData(streamID, false, data[:count]); facts.err != nil {
						return
					}
					remaining -= count
				}
				marker := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
				if facts.err = framer.WritePing(false, marker); facts.err != nil {
					return
				}
				for {
					frame, err := framer.ReadFrame()
					if err != nil {
						facts.err = err
						return
					}
					if ping, ok := frame.(*http2.PingFrame); ok && ping.IsAck() && ping.Data == marker {
						facts.ack = true
						facts.err = framer.WriteData(streamID, true, nil)
						observed <- facts
						reported = true
						_, _ = io.Copy(io.Discard, conn)
						return
					}
					if goaway, ok := frame.(*http2.GoAwayFrame); ok {
						facts.err = http2.ConnectionError(goaway.ErrCode)
						return
					}
				}
			}()
			t.Cleanup(func() {
				_ = listener.Close()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("connection-credit peer did not terminate")
				}
			})
			options := OptionsV1{Name: "connection-credit", PresetName: "chrome-148", Protocol: HTTP2, MaxResponseBytes: 32 << 20, MaxWireBytes: 32 << 20,
				Native: NativeOptionsV1{Verify: &transport.TLSVerify{RootCAs: roots}, Transport: &transport.TransportConfig{CustomH2Settings: &fingerprint.HTTP2Settings{InitialWindowSize: 32 << 20, HeaderTableSize: 4096, ConnectionWindowUpdate: scenario.increment}}}}
			prepared, err := PrepareV1(options)
			if err != nil {
				t.Fatal(err)
			}
			wantIncrement := scenario.increment
			if wantIncrement == 0 {
				wantIncrement = nativeDefaultIncrement
			}
			if prepared.Metadata().Native.H2ConnectionBytes != uint64(wantIncrement)+65535 {
				t.Fatal("connection residence differs from effective wire credit", prepared.Metadata().Native.H2ConnectionBytes)
			}
			fixture := preparedFixture(t, prepared, 1)
			stream, receipt, openErr := fixture.client.Open(testContext(t), fault.Correlation{Call: "connection-credit"}, request(t, "GET", "https://"+listener.Addr().String(), nil))
			var facts observation
			select {
			case facts = <-observed:
			case <-time.After(5 * time.Second):
				t.Fatal("peer did not observe processing through DATA and PING")
			}
			if facts.increment != wantIncrement {
				t.Fatal("actual WINDOW_UPDATE differs from selected increment", facts.increment, wantIncrement)
			}
			var data []byte
			var readErr error
			if stream != nil {
				data, readErr = io.ReadAll(stream)
				_ = stream.Close(testContext(t))
			}
			result := settle(t, fixture, receipt)
			if scenario.failure {
				var connectionError nativeh2.ConnectionError
				if facts.ack || !errors.As(result.Err(), &connectionError) || connectionError != nativeh2.ConnectionError(nativeh2.ErrCodeFlowControl) || result.Outcome.Value.Complete() {
					t.Fatal("one-over connection credit was not refused with native FLOW_CONTROL_ERROR", facts.ack, openErr, readErr, result.Err())
				}
			} else if !facts.ack || facts.err != nil || openErr != nil || readErr != nil || len(data) != scenario.payload || !result.Outcome.Value.Complete() {
				t.Fatal("legal advertised connection credit was rejected", facts.ack, facts.err, openErr, readErr, len(data), result.Err())
			}
		})
	}
}

func TestPreparationRejectsH2ConnectionCreditOverflow(t *testing.T) {
	for _, increment := range []uint32{math.MaxInt32 - 65535, math.MaxInt32 - 65534, math.MaxUint32} {
		options := OptionsV1{Name: "connection-bound", PresetName: "chrome-148", Protocol: HTTP2, Native: NativeOptionsV1{Transport: &transport.TransportConfig{CustomH2Settings: &fingerprint.HTTP2Settings{ConnectionWindowUpdate: increment}}}}
		prepared, err := PrepareV1(options)
		if increment == math.MaxInt32-65535 {
			if err != nil || prepared.Metadata().Native.H2ConnectionBytes != math.MaxInt32 {
				t.Fatal("maximum legal connection credit changed", err)
			}
		} else if !errors.Is(err, ErrInput) {
			t.Fatal("overflowing connection credit entered source construction", increment, err)
		}
	}
}
