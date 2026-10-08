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
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dnswire "github.com/miekg/dns"
)

func dnsPeer(t *testing.T, handler dnswire.HandlerFunc) string {
	t.Helper()
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &dnswire.Server{PacketConn: packet, Handler: handler}
	done := make(chan struct{})
	go func() { defer close(done); _ = server.ActivateAndServe() }()
	t.Cleanup(func() {
		_ = server.Shutdown()
		_ = packet.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("DNS peer did not stop")
		}
	})
	return packet.LocalAddr().String()
}

func echDNSAnswer(writer dnswire.ResponseWriter, request *dnswire.Msg, data []byte) {
	response := new(dnswire.Msg)
	response.SetReply(request)
	if len(request.Question) == 1 && request.Question[0].Qtype == dnswire.TypeHTTPS {
		response.Answer = []dnswire.RR{&dnswire.HTTPS{SVCB: dnswire.SVCB{Hdr: dnswire.RR_Header{Name: request.Question[0].Name, Rrtype: dnswire.TypeHTTPS, Class: dnswire.ClassINET, Ttl: 60}, Priority: 1, Target: ".", Value: []dnswire.SVCBKeyValue{&dnswire.SVCBECHConfig{ECH: data}}}}}
	}
	_ = writer.WriteMsg(response)
}

func TestSourceECHCacheCapacityCopiesAndIsolation(t *testing.T) {
	var calls atomic.Int32
	firstPeer := dnsPeer(t, func(writer dnswire.ResponseWriter, request *dnswire.Msg) {
		calls.Add(1)
		echDNSAnswer(writer, request, []byte{1, 2, 3})
	})
	secondPeer := dnsPeer(t, func(writer dnswire.ResponseWriter, request *dnswire.Msg) {
		echDNSAnswer(writer, request, []byte{4, 5, 6})
	})
	options := OptionsV1{Name: "cache", PresetName: "chrome-148", Protocol: HTTP2, ResolverAddress: firstPeer, MaxECHEntries: 2}
	first := bindFixture(t, options, 1).client.owner
	options.Name, options.ResolverAddress = "other", secondPeer
	second := bindFixture(t, options, 1).client.owner
	value, err := first.echConfig(testContext(t), "shared.invalid")
	if err != nil {
		t.Fatal(err)
	}
	value[0] = 9
	again, err := first.echConfig(testContext(t), "shared.invalid")
	if err != nil || !bytes.Equal(again, []byte{1, 2, 3}) || calls.Load() != 1 {
		t.Fatal("cache copy or hit changed", again, err, calls.Load())
	}
	other, err := second.echConfig(testContext(t), "shared.invalid")
	if err != nil || !bytes.Equal(other, []byte{4, 5, 6}) {
		t.Fatal("independent sources shared discovery", other, err)
	}
	for _, host := range []string{"second.invalid", "third.invalid"} {
		if _, err := first.echConfig(testContext(t), host); err != nil {
			t.Fatal(err)
		}
	}
	first.mu.Lock()
	count := len(first.ech)
	entry := first.ech["third.invalid"]
	entry.expires = time.Now().Add(-time.Minute)
	first.ech["third.invalid"] = entry
	first.mu.Unlock()
	if count != 2 {
		t.Fatal("fresh entries exceeded source capacity", count)
	}
	before := calls.Load()
	if _, err := first.echConfig(testContext(t), "third.invalid"); err != nil || calls.Load() != before+1 {
		t.Fatal("expired entry did not refresh", err)
	}
	first.mu.Lock()
	dns, sockets := first.dnsActive, first.connections
	first.mu.Unlock()
	if dns != 0 || sockets != 0 {
		t.Fatal("discovery retained actual work or sockets", dns, sockets)
	}
}

func TestSourceECHIndependentCancellationJoinsActualSockets(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	defer finish()
	address := dnsPeer(t, func(writer dnswire.ResponseWriter, request *dnswire.Msg) {
		entered <- struct{}{}
		<-release
		echDNSAnswer(writer, request, []byte{7})
	})
	fixture := bindFixture(t, OptionsV1{Name: "cancel", PresetName: "chrome-148", ResolverAddress: address, MaxDNSActive: 2}, 1)
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { _, err := fixture.client.owner.echConfig(ctx, "same.invalid"); first <- err }()
	go func() { _, err := fixture.client.owner.echConfig(testContext(t), "same.invalid"); second <- err }()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("lookup did not enter independent peer")
		}
	}
	if _, err := fixture.client.owner.echConfig(testContext(t), "saturated.invalid"); !errors.Is(err, ErrLimit) {
		t.Fatal("source DNS capacity did not reject before another query", err)
	}
	cancel()
	select {
	case err := <-first:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("caller cancellation lost", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled DNS work was not joined")
	}
	select {
	case err := <-second:
		t.Fatal("one canceled caller ended its peer", err)
	default:
	}
	fixture.client.owner.mu.Lock()
	dns, sockets := fixture.client.owner.dnsActive, fixture.client.owner.connections
	fixture.client.owner.mu.Unlock()
	if dns != 1 || sockets != 1 {
		t.Fatal("source physical accounting after cancellation", dns, sockets)
	}
	finish()
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("healthy lookup did not finish")
	}
	fixture.client.owner.mu.Lock()
	dns, sockets = fixture.client.owner.dnsActive, fixture.client.owner.connections
	fixture.client.owner.mu.Unlock()
	if dns != 0 || sockets != 0 {
		t.Fatal("terminal lookup retained source resources", dns, sockets)
	}
}

func TestSourceECHStaleFallbackDoesNotMaskCancellation(t *testing.T) {
	for _, mode := range []string{"resolver-failure", "expired-grace", "caller-canceled"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			entered, release := make(chan struct{}), make(chan struct{})
			defer close(release)
			resolver := dnsPeer(t, func(writer dnswire.ResponseWriter, input *dnswire.Msg) {
				if calls.Add(1) == 1 {
					echDNSAnswer(writer, input, []byte{7, 8, 9})
					return
				}
				if mode == "caller-canceled" {
					close(entered)
					<-release
				}
				response := new(dnswire.Msg)
				response.SetRcode(input, dnswire.RcodeServerFailure)
				_ = writer.WriteMsg(response)
			})
			own := bindFixture(t, OptionsV1{Name: "stale-ech", PresetName: "chrome-148", ResolverAddress: resolver}, 1).client.owner
			if _, err := own.echConfig(testContext(t), "cached.invalid"); err != nil {
				t.Fatal(err)
			}
			own.mu.Lock()
			entry := own.ech["cached.invalid"]
			entry.expires = time.Now().Add(-time.Minute)
			if mode == "expired-grace" {
				entry.expires = time.Now().Add(-6 * time.Minute)
			}
			own.ech["cached.invalid"] = entry
			own.mu.Unlock()
			ctx, cancel := context.WithCancelCause(testContext(t))
			defer cancel(nil)
			cause := errors.New("canceled ECH refresh")
			type outcome struct {
				value []byte
				err   error
			}
			returned := make(chan outcome, 1)
			go func() {
				value, err := own.echConfig(ctx, "cached.invalid")
				returned <- outcome{value, err}
			}()
			if mode == "caller-canceled" {
				select {
				case <-entered:
					cancel(cause)
				case <-time.After(time.Second):
					t.Fatal("refresh did not enter independent DNS peer")
				}
			}
			select {
			case result := <-returned:
				switch mode {
				case "resolver-failure":
					if result.err != nil || !bytes.Equal(result.value, []byte{7, 8, 9}) {
						t.Fatal("native best-effort stale fallback disappeared", result.value, result.err)
					}
				case "expired-grace":
					if result.err == nil || result.value != nil {
						t.Fatal("stale entry outlived its bounded grace", result.value, result.err)
					}
				case "caller-canceled":
					if !errors.Is(result.err, context.Canceled) || !errors.Is(result.err, cause) || result.value != nil {
						t.Fatal("cached bytes concealed canceled source work", result.value, result.err)
					}
				}
			case <-time.After(time.Second):
				t.Fatal("ECH refresh retained its network operation")
			}
			own.mu.Lock()
			defer own.mu.Unlock()
			if own.dnsActive != 0 || own.connections != 0 {
				t.Fatal("refresh returned before actual DNS resources were reclaimed")
			}
		})
	}
}

func TestSourceECHConfigByteBoundPreservesHealthyCache(t *testing.T) {
	var calls atomic.Int32
	resolver := dnsPeer(t, func(writer dnswire.ResponseWriter, input *dnswire.Msg) {
		calls.Add(1)
		data := []byte{1, 2, 3}
		if input.Question[0].Name == "oversized.invalid." {
			data = append(data, 4)
		}
		echDNSAnswer(writer, input, data)
	})
	own := bindFixture(t, OptionsV1{Name: "ech-byte-bound", PresetName: "chrome-148", ResolverAddress: resolver, MaxECHEntries: 1, MaxECHConfigBytes: 3}, 1).client.owner
	for _, host := range []string{"healthy.invalid", "oversized.invalid", "healthy.invalid"} {
		data, err := own.echConfig(testContext(t), host)
		if host == "oversized.invalid" {
			if !errors.Is(err, ErrLimit) || data != nil {
				t.Fatal("one-over ECH bytes entered the source cache", data, err)
			}
		} else if err != nil || !bytes.Equal(data, []byte{1, 2, 3}) {
			t.Fatal("exact-bound ECH bytes or existing cache entry changed", data, err)
		}
	}
	own.mu.Lock()
	defer own.mu.Unlock()
	if calls.Load() != 2 || len(own.ech) != 1 || own.ech["healthy.invalid"].config == nil || own.dnsActive != 0 || own.connections != 0 {
		t.Fatal("rejected ECH result evicted healthy cache or retained actual resources", calls.Load(), len(own.ech), own.dnsActive, own.connections)
	}
}

func TestDNSNativeCompressedNameStorageHasSeparateBudget(t *testing.T) {
	wire := make([]byte, 12)
	binary.BigEndian.PutUint16(wire[2:], 0x8000)
	binary.BigEndian.PutUint16(wire[4:], 1)
	binary.BigEndian.PutUint16(wire[6:], 1)
	for _, length := range []int{61, 63, 63, 63} {
		wire = append(wire, byte(length))
		wire = append(wire, bytes.Repeat([]byte{1}, length)...)
	}
	wire = append(wire, 0)
	wire = binary.BigEndian.AppendUint16(wire, dnswire.TypeA)
	wire = binary.BigEndian.AppendUint16(wire, dnswire.ClassINET)
	wire = append(wire, 0xc0, 12)
	wire = binary.BigEndian.AppendUint16(wire, dnswire.TypeHIP)
	wire = binary.BigEndian.AppendUint16(wire, dnswire.ClassINET)
	wire = binary.BigEndian.AppendUint32(wire, 60)
	const names = 64
	wire = binary.BigEndian.AppendUint16(wire, 4+2*names)
	wire = append(wire, 0, 0, 0, 0)
	for range names {
		wire = append(wire, 0xc0, 12)
	}
	var message dnswire.Msg
	if err := message.Unpack(wire); err != nil {
		t.Fatal(err)
	}
	record := message.Answer[0].(*dnswire.HIP)
	if len(record.RendezvousServers) != names || len(record.RendezvousServers[0]) != 1004 {
		t.Fatal("selected DNS parser expansion changed")
	}
	prepared, err := PrepareV1(OptionsV1{Name: "dns-budget", PresetName: "chrome-148"})
	if err != nil {
		t.Fatal(err)
	}
	metadata := prepared.Metadata()
	if metadata.DNSMessageBytes != dnswire.MaxMsgSize || metadata.DNSWorkBytes < int64(metadata.DNSMessageBytes)*int64(len(record.RendezvousServers[0])/2) {
		t.Fatal("DNS wire bytes were mistaken for decoded parser residence", metadata.DNSMessageBytes, metadata.DNSWorkBytes)
	}
	t.Logf("safe selected-parser witness: wire=%d, repeated-name strings=%d; per-slot declaration=%d", len(wire), names*len(record.RendezvousServers[0]), metadata.DNSWorkBytes)
}

func TestSourceCloseCancelsActualTCPDNSWork(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		var length [2]byte
		if _, err := io.ReadFull(conn, length[:]); err != nil {
			return
		}
		query := make([]byte, int(binary.BigEndian.Uint16(length[:])))
		if _, err := io.ReadFull(conn, query); err != nil {
			return
		}
		close(entered)
		_, _ = io.Copy(io.Discard, conn)
	}()
	t.Cleanup(func() { _ = listener.Close(); <-closed })
	fixture := bindFixture(t, OptionsV1{Name: "tcp-dns-close", PresetName: "chrome-148", ResolverAddress: listener.Addr().String(), ResolverNetwork: "tcp"}, 1)
	returned := make(chan error, 1)
	go func() { _, err := fixture.client.owner.echConfig(testContext(t), "closing.invalid"); returned <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("TCP DNS query did not reach independent peer")
	}
	if err := fixture.assembly.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-returned:
		if err == nil {
			t.Fatal("closed resolver unexpectedly returned discovery data")
		}
	case <-time.After(time.Second):
		t.Fatal("source release did not join DNS work")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("source release did not close actual DNS socket")
	}
	fixture.client.owner.mu.Lock()
	defer fixture.client.owner.mu.Unlock()
	if fixture.client.owner.dnsActive != 0 || fixture.client.owner.connections != 0 {
		t.Fatal("source released before owned DNS counts returned")
	}
}
