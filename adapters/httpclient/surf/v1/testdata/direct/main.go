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
	"github.com/quic-go/quic-go/http3"
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
