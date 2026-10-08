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

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/enetx/g"
	nativehttp "github.com/enetx/http"
	"github.com/enetx/surf/profiles/chrome"
	p "github.com/frost-leo/fathomry/adapters/httpclient/surf/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/quicvarint"
	utls "github.com/refraction-networking/utls"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	for _, mode := range []p.ProtocolMode{p.HTTP1Only, p.Negotiated, p.H2C, p.PreferHTTP3} {
		if err := run(ctx, mode); err != nil {
			fmt.Fprintln(os.Stderr, mode, err)
			os.Exit(1)
		}
	}
	if err := dynamicDirect(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("surf direct public consumer passed")
}
func ptr[T any](value T) *T { return &value }

type input struct {
	prefix, tail    *strings.Reader
	proceed, closed chan struct{}
	once            sync.Once
	generated       atomic.Bool
	closes          atomic.Int64
}

func (value *input) Read(data []byte) (int, error) {
	if value.prefix.Len() > 0 {
		return value.prefix.Read(data)
	}
	select {
	case <-value.proceed:
		value.generated.Store(true)
		return value.tail.Read(data)
	case <-value.closed:
		return 0, context.Canceled
	}
}
func (value *input) Close() error {
	value.once.Do(func() { value.closes.Add(1); close(value.closed) })
	return nil
}
func newRequest(endpoint string) *nativehttp.Request {
	value, err := nativehttp.NewRequest("POST", endpoint, nil)
	if err != nil {
		panic(err)
	}
	return value
}
func run(ctx context.Context, mode p.ProtocolMode) error {
	reader := &input{prefix: strings.NewReader("prefix"), tail: strings.NewReader("tail"), proceed: make(chan struct{}), closed: make(chan struct{})}
	defer reader.Close()
	proceed := sync.OnceFunc(func() { close(reader.proceed) })
	defer proceed()
	var received, boundaries atomic.Int64
	var peerErr atomic.Pointer[error]
	peer := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		received.Add(1)
		if request.URL.Path == "/multipart" {
			parts, err := request.MultipartReader()
			if err != nil {
				peerErr.Store(&err)
				return
			}
			field, err := parts.NextPart()
			if err != nil {
				peerErr.Store(&err)
				return
			}
			data, err := io.ReadAll(field)
			if err != nil || field.FormName() != "field" || string(data) != "value" {
				err = errors.New("field changed")
				peerErr.Store(&err)
				return
			}
			file, err := parts.NextPart()
			if err != nil {
				peerErr.Store(&err)
				return
			}
			prefix := make([]byte, 6)
			_, err = io.ReadFull(file, prefix)
			if err != nil || string(prefix) != "prefix" || reader.generated.Load() {
				err = errors.New("input was materialized before peer prefix")
				peerErr.Store(&err)
				return
			}
			proceed()
			tail, err := io.ReadAll(file)
			if err != nil || string(tail) != "tail" {
				err = errors.New("tail changed")
				peerErr.Store(&err)
				return
			}
			if _, err := parts.NextPart(); err != io.EOF {
				err = errors.New("unexpected multipart part")
				peerErr.Store(&err)
				return
			}
		}
		writer.Header().Set("Trailer", "X-End")
		writer.WriteHeader(418)
		_, _ = io.WriteString(writer, request.Proto)
		writer.Header().Set("X-End", "done")
	}))
	profile := chrome.Desktop
	profile.HelloSpec = nil
	profile.HelloID = utls.ClientHelloID{}
	profile.ShuffleExtensions = false
	profile.Boundary = func() g.String { boundaries.Add(1); return "public-native-boundary" }
	native := p.NativeOptions{Profile: &profile}
	if mode == p.H2C {
		protocols := new(http.Protocols)
		protocols.SetUnencryptedHTTP2(true)
		peer.Config.Protocols = protocols
		peer.Start()
	} else {
		peer.EnableHTTP2 = mode == p.Negotiated
		peer.StartTLS()
		pool := x509.NewCertPool()
		pool.AddCert(peer.Certificate())
		native.TLSConfig = &tls.Config{RootCAs: pool}
	}
	defer peer.Close()
	if mode == p.PreferHTTP3 {
		packet, err := net.ListenPacket("udp", peer.Listener.Addr().String())
		if err != nil {
			return err
		}
		server := &http3.Server{TLSConfig: &tls.Config{Certificates: peer.TLS.Certificates}, Handler: peer.Config.Handler, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
		done := make(chan struct{})
		go func() { defer close(done); _ = server.Serve(packet) }()
		defer func() { _ = server.Close(); _ = packet.Close(); <-done }()
	}
	prepared, err := p.Prepare(p.Settings{Name: string(mode), Mode: &mode, MaxActive: ptr(1)}, native)
	if err != nil {
		return err
	}
	policy, err := prepared.Policy()
	if err != nil {
		return err
	}
	runtime, err := adapters.New(ctx, policy.Runtime)
	if err != nil {
		return err
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[p.Result](policy.Evidence)
	if err != nil {
		return err
	}
	owner, err := prepared.Open(ctx, p.Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		return err
	}
	defer owner.Close(context.Background())
	client, err := owner.Client().WithID("independent")
	if err != nil {
		return err
	}
	options := p.RequestOptions{Multipart: &p.Multipart{Fields: []p.Field{{Name: "field", Value: "value"}}, Parts: []p.Part{{Name: "file", FileName: "payload.bin", Input: reader}}}}
	request := newRequest(peer.URL + "/multipart")
	canceled, stop := context.WithCancel(context.Background())
	stop()
	request = request.WithContext(canceled)
	receipt, err := client.Do(ctx, ctx, request, options)
	if err != nil {
		return err
	}
	observed, err := receipt.WaitReleased(ctx)
	if err != nil || observed.Err() != nil {
		return errors.Join(err, observed.Err())
	}
	value, ok := observed.ValueCopy()
	want := "HTTP/2.0"
	if mode == p.PreferHTTP3 {
		want = "HTTP/3.0"
	}
	if mode == p.HTTP1Only {
		want = "HTTP/1.1"
	}
	if problem := peerErr.Load(); problem != nil {
		return *problem
	}
	if !ok || !value.Complete() || value.Metadata().Protocol() != want || value.Metadata().StatusCode() != 418 || value.TrailersCopy().Get("X-End") != "done" || value.Source().Name != string(mode) || value.Attribution().ID != "independent" || reader.closes.Load() != 1 || boundaries.Load() != 1 || value.Attempts().Exact {
		return errors.New("native result, producer custody or attribution changed")
	}
	before := received.Load()
	if rejected, err := client.Do(ctx, ctx, newRequest(peer.URL+"/finite")); rejected != nil || !errors.Is(err, adapters.ErrEvidence) || received.Load() != before {
		return errors.New("evidence saturation dispatched HTTP")
	}
	if err := receive(ctx, inbox, receipt); err != nil {
		return err
	}
	stream, receipt, err := client.Open(ctx, newRequest(peer.URL+"/stream"))
	if err != nil {
		return err
	}
	data, err := io.ReadAll(stream)
	if err != nil || string(data) != want {
		return errors.New("stream bytes/protocol changed")
	}
	wait, stopWait := context.WithCancel(ctx)
	stopWait()
	if _, err := receipt.WaitReleased(wait); err == nil {
		return errors.New("EOF released retained stream")
	}
	if err := stream.Close(ctx); err != nil {
		return err
	}
	if err := receive(ctx, inbox, receipt); err != nil {
		return err
	}
	if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
		return errors.Join(err, errors.New("source not released"))
	}
	record, err := inbox.NextReleased(ctx)
	if err != nil {
		return err
	}
	if err := record.Ack(); err != nil {
		return err
	}
	state, _ := runtime.Inspect()
	evidence, _ := inbox.Inspect()
	if state.Active != 0 || state.WorkBytes != 0 || evidence.Outstanding != 0 {
		return errors.New("source/root/evidence reservations leaked")
	}
	build, err := p.Build()
	if err != nil {
		return err
	}
	found := false
	for _, module := range build.SDKs {
		if module.Path.Value == "github.com/enetx/surf" {
			found = module.Version.Value == "v1.0.206" && module.ReplacementKind != "none"
		}
	}
	if !found {
		return errors.New("selected corrected SDK graph missing")
	}
	return nil
}
func receive(ctx context.Context, inbox *adapters.Inbox[p.Result], receipt *adapters.Receipt[p.Result]) error {
	direct, err := receipt.WaitReleased(ctx)
	if err != nil {
		return err
	}
	delivery, err := inbox.NextReleased(ctx)
	if err != nil {
		return err
	}
	independent, err := delivery.Receipt()
	if err != nil {
		return err
	}
	snapshot, err := independent.WaitReleased(ctx)
	if err != nil || snapshot.Info() != direct.Info() {
		return errors.New("independent attribution changed")
	}
	left, _ := direct.ValueCopy()
	right, _ := snapshot.ValueCopy()
	if left.Complete() != right.Complete() || left.Attribution() != right.Attribution() || !bytes.Equal(left.DataCopy(), right.DataCopy()) {
		return errors.New("independent data changed")
	}
	if err := delivery.Retry(); err != nil {
		return err
	}
	delivery, err = inbox.NextReleased(ctx)
	if err != nil {
		return err
	}
	return delivery.Ack()
}

// dynamicQPACKPeer only uses standard library and the selected QUIC dependency.
// Clients must advertise capacity >=64 and at least one blocked stream. It sends
// one dynamic insertion per connection and references it in every response.
type dynamicQPACKPeer struct {
	URL                    string
	Roots                  *x509.CertPool
	Requests, Acknowledged atomic.Int64
	connections            atomic.Int64
	hold                   <-chan struct{}
	ctx                    context.Context
	cancel                 context.CancelFunc
	listener               *quic.Listener
	packet                 net.PacketConn
	workers                sync.WaitGroup
	once                   sync.Once
	mu                     sync.Mutex
	err                    error
	changed                chan struct{}
}

func newDynamicQPACKPeer(parent context.Context, held ...<-chan struct{}) (*dynamicQPACKPeer, error) {
	certificate := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	config := &tls.Config{Certificates: certificate.TLS.Certificates, NextProtos: []string{"h3"}}
	roots := x509.NewCertPool()
	roots.AddCert(certificate.Certificate())
	certificate.Close()
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	listener, err := quic.Listen(packet, config, &quic.Config{})
	if err != nil {
		_ = packet.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	peer := &dynamicQPACKPeer{URL: "https://" + packet.LocalAddr().String(), Roots: roots, ctx: ctx, cancel: cancel, listener: listener, packet: packet, changed: make(chan struct{}, 1)}
	if len(held) > 0 {
		peer.hold = held[0]
	}
	peer.workers.Go(func() {
		for {
			connection, err := listener.Accept(ctx)
			if err != nil {
				return
			}
			peer.workers.Go(func() { peer.serve(connection) })
		}
	})
	return peer, nil
}

func (peer *dynamicQPACKPeer) fail(err error) {
	peer.mu.Lock()
	if peer.err == nil {
		peer.err = err
	}
	peer.mu.Unlock()
	select {
	case peer.changed <- struct{}{}:
	default:
	}
}

func (peer *dynamicQPACKPeer) Close() error {
	peer.once.Do(func() { peer.cancel(); _ = peer.listener.Close(); _ = peer.packet.Close(); peer.workers.Wait() })
	peer.mu.Lock()
	defer peer.mu.Unlock()
	return peer.err
}

func (peer *dynamicQPACKPeer) WaitAcknowledged(ctx context.Context, count int64) error {
	for peer.Acknowledged.Load() < count {
		peer.mu.Lock()
		err := peer.err
		peer.mu.Unlock()
		if err != nil {
			return err
		}
		select {
		case <-peer.changed:
		case <-ctx.Done():
			return ctx.Err()
		case <-peer.ctx.Done():
			return peer.ctx.Err()
		}
	}
	return nil
}

func (peer *dynamicQPACKPeer) serve(connection *quic.Conn) {
	value := fmt.Sprintf("dynamic-%d", peer.connections.Add(1))
	stop := context.AfterFunc(peer.ctx, func() { _ = connection.CloseWithError(0, "") })
	defer stop()
	defer connection.CloseWithError(0, "")
	control, err := connection.OpenUniStreamSync(peer.ctx)
	if err != nil {
		return
	}
	if _, err := control.Write([]byte{0, 4, 0}); err != nil {
		return
	}
	settings := make(chan map[uint64]uint64, 1)
	peer.workers.Go(func() {
		for {
			stream, err := connection.AcceptUniStream(peer.ctx)
			if err != nil {
				return
			}
			peer.workers.Go(func() { peer.readUni(stream, settings) })
		}
	})
	select {
	case values := <-settings:
		if values[1] < 64 || values[7] < 1 {
			peer.fail(errors.New("dynamic peer: required QPACK settings absent"))
			return
		}
	case <-peer.ctx.Done():
		return
	case <-connection.Context().Done():
		return
	}
	encoder, err := connection.OpenUniStreamSync(peer.ctx)
	if err != nil {
		return
	}
	if _, err := encoder.Write(append([]byte{2, 0x3f, 0x21, 0x47}, append([]byte("x-qpack"), append([]byte{byte(len(value))}, []byte(value)...)...)...)); err != nil {
		return
	}
	for {
		stream, err := connection.AcceptStream(peer.ctx)
		if err != nil {
			return
		}
		peer.workers.Go(func() {
			reader := quicvarint.NewReader(stream)
			kind, err := quicvarint.Read(reader)
			if err != nil || kind != 1 {
				peer.fail(errors.New("dynamic peer: request HEADERS absent"))
				return
			}
			length, err := quicvarint.Read(reader)
			if err != nil || length > 64<<10 {
				peer.fail(errors.New("dynamic peer: request HEADERS exceeds bound"))
				return
			}
			if _, err := io.CopyN(io.Discard, reader, int64(length)); err != nil {
				return
			}
			sequence := peer.Requests.Add(1)
			if err := dynamicPeerFrame(stream, 1, []byte{2, 0, 0xd9, 0x80}); err != nil {
				return
			}
			if err := dynamicPeerFrame(stream, 0, []byte("dynamic")); err != nil {
				return
			}
			if sequence == 1 && peer.hold != nil {
				select {
				case <-peer.hold:
				case <-connection.Context().Done():
					return
				case <-peer.ctx.Done():
					return
				}
			}
			if err := dynamicPeerFrame(stream, 1, []byte{2, 0, 0x80}); err != nil {
				return
			}
			_ = stream.Close()
			_, _ = io.Copy(io.Discard, io.LimitReader(reader, 1<<20))
		})
	}
}

func (peer *dynamicQPACKPeer) readUni(stream *quic.ReceiveStream, settings chan<- map[uint64]uint64) {
	reader := bufio.NewReader(stream)
	kind, err := quicvarint.Read(reader)
	if err != nil {
		return
	}
	if kind == 0 {
		frame, err := quicvarint.Read(reader)
		if err != nil || frame != 4 {
			peer.fail(errors.New("dynamic peer: SETTINGS absent"))
			return
		}
		length, err := quicvarint.Read(reader)
		if err != nil || length > 4096 {
			peer.fail(errors.New("dynamic peer: SETTINGS exceeds bound"))
			return
		}
		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			return
		}
		input := bytes.NewReader(data)
		values := make(map[uint64]uint64)
		for input.Len() > 0 {
			key, err := quicvarint.Read(input)
			if err != nil {
				peer.fail(err)
				return
			}
			value, err := quicvarint.Read(input)
			if err != nil {
				peer.fail(err)
				return
			}
			values[key] = value
		}
		select {
		case settings <- values:
		case <-peer.ctx.Done():
			return
		}
		_, _ = io.Copy(io.Discard, reader)
		return
	}
	if kind != 3 {
		_, _ = io.Copy(io.Discard, reader)
		return
	}
	for {
		first, err := reader.ReadByte()
		if err != nil {
			return
		}
		prefix := uint(6)
		if first&0x80 != 0 {
			prefix = 7
		}
		if _, err := dynamicPeerInteger(reader, first, prefix); err != nil {
			peer.fail(err)
			return
		}
		if first&0x80 != 0 {
			peer.Acknowledged.Add(1)
			select {
			case peer.changed <- struct{}{}:
			default:
			}
		}
	}
}

func dynamicPeerInteger(reader io.ByteReader, first byte, bits uint) (uint64, error) {
	maximum := uint64(1<<bits) - 1
	value := uint64(first) & maximum
	if value != maximum {
		return value, nil
	}
	for shift := uint(0); shift < 63; shift += 7 {
		next, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		value += uint64(next&127) << shift
		if next&128 == 0 {
			return value, nil
		}
	}
	return 0, errors.New("dynamic peer: feedback integer exceeds bound")
}

func dynamicPeerFrame(writer io.Writer, kind uint64, data []byte) error {
	header := quicvarint.Append(nil, kind)
	header = quicvarint.Append(header, uint64(len(data)))
	_, err := writer.Write(append(header, data...))
	return err
}

func dynamicDirect(ctx context.Context) error {
	peer, err := newDynamicQPACKPeer(ctx)
	if err != nil {
		return err
	}
	defer peer.Close()
	mode := p.PreferHTTP3
	profile := chrome.Desktop
	prepared, err := p.Prepare(p.Settings{Name: "dynamic-qpack", Mode: &mode}, p.NativeOptions{Profile: &profile, TLSConfig: &tls.Config{RootCAs: peer.Roots}})
	if err != nil {
		return err
	}
	policy, err := prepared.Policy()
	if err != nil {
		return err
	}
	runtime, err := adapters.New(ctx, policy.Runtime)
	if err != nil {
		return err
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[p.Result](policy.Evidence)
	if err != nil {
		return err
	}
	owner, err := prepared.Open(ctx, p.Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		return err
	}
	defer owner.Close(context.Background())
	for index := int64(1); index <= 2; index++ {
		input, _ := nativehttp.NewRequest("GET", peer.URL, nil)
		receipt, err := owner.Client().Do(ctx, ctx, input)
		if err != nil {
			return err
		}
		snapshot, err := receipt.WaitReleased(ctx)
		if err != nil || snapshot.Err() != nil {
			return errors.Join(err, snapshot.Err())
		}
		result, present := snapshot.ValueCopy()
		if !present || !result.Complete() || result.Metadata().Protocol() != "HTTP/3.0" ||
			result.Metadata().HeadersCopy().Get("X-Qpack") != "dynamic-1" || result.TrailersCopy().Get("X-Qpack") != "dynamic-1" || string(result.DataCopy()) != "dynamic" {
			return errors.New("public dynamic QPACK headers/trailers changed")
		}
		if err := receive(ctx, inbox, receipt); err != nil {
			return err
		}
		if err := peer.WaitAcknowledged(ctx, 2*index); err != nil {
			return err
		}
	}
	if peer.Requests.Load() != 2 {
		return errors.New("dynamic requests were replayed")
	}
	if err := owner.Close(ctx); err != nil {
		return err
	}
	delivery, err := inbox.NextReleased(ctx)
	if err != nil {
		return err
	}
	if err := delivery.Ack(); err != nil {
		return err
	}
	state, _ := runtime.Inspect()
	if state.Active != 0 || state.WorkBytes != 0 {
		return errors.New("dynamic source work retained")
	}
	return peer.Close()
}
