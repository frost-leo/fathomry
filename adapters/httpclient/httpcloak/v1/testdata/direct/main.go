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
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"time"

	p "github.com/frost-leo/fathomry/adapters/httpclient/httpcloak/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	officialh3 "github.com/quic-go/quic-go/http3"
	nativehttp "github.com/sardanioss/http"
	"github.com/sardanioss/httpcloak/fingerprint"
	"github.com/sardanioss/httpcloak/transport"
)

func ptr[T any](value T) *T { return &value }
func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	for _, mode := range []p.ProtocolMode{p.HTTP1, p.HTTP2, p.HTTP3} {
		if err := run(ctx, mode); err != nil {
			fmt.Fprintln(os.Stderr, mode, err)
			os.Exit(1)
		}
	}
	fmt.Println("httpcloak direct public consumer passed")
}

func run(ctx context.Context, mode p.ProtocolMode) error {
	var effects atomic.Int64
	wantedProtocol := map[p.ProtocolMode]string{p.HTTP1: "HTTP/1.1", p.HTTP2: "HTTP/2.0", p.HTTP3: "HTTP/3.0"}[mode]
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		effects.Add(1)
		if r.Proto != wantedProtocol {
			http.Error(w, "wrong actual protocol", http.StatusBadRequest)
			return
		}
		if strings.Join(r.Header.Values("X-Exact"), ",") != "one,two" || r.Header.Get("X-Discard") != "" {
			http.Error(w, "native fields changed", http.StatusBadRequest)
			return
		}
		w.Header().Set("Trailer", "X-Final")
		w.WriteHeader(418)
		_, _ = io.WriteString(w, "body")
		w.Header().Set("X-Final", "done")
	})
	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = mode == p.HTTP2
	server.StartTLS()
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	target := server.URL
	if mode == p.HTTP3 {
		packet, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			return err
		}
		peer := &officialh3.Server{TLSConfig: &tls.Config{Certificates: server.TLS.Certificates}, Handler: handler}
		exited := make(chan error, 1)
		go func() { exited <- peer.Serve(packet) }()
		defer func() { _ = peer.Close(); _ = packet.Close(); <-exited }()
		target = "https://" + packet.LocalAddr().String()
	}
	settings := p.Settings{Name: string(mode), PresetName: "chrome-148", Protocol: &mode, DisableECH: ptr(true), MaxActive: ptr(1), MaxBindings: ptr(1), MaxConnections: ptr(4)}
	native := p.NativeOptions{Verify: &transport.TLSVerify{RootCAs: roots}}
	prepared, err := p.Prepare(settings, native)
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
	dependencies := p.Dependencies{Runtime: runtime, Evidence: inbox}
	insufficient := policy.Runtime
	insufficient.MaxWorkBytes--
	rejectedRuntime, err := adapters.New(ctx, insufficient)
	if err != nil {
		return err
	}
	rejected, err := prepared.Open(ctx, p.Dependencies{Runtime: rejectedRuntime, Evidence: inbox})
	if rejected != nil || !errors.Is(err, p.ErrLimit) {
		return errors.New("insufficient source runtime acquired native state")
	}
	if err := rejectedRuntime.Close(ctx); err != nil {
		return err
	}
	owner, err := prepared.Open(ctx, dependencies)
	if err != nil {
		return err
	}
	defer owner.Close(context.Background())
	client, err := owner.Client().WithID("public-direct")
	if err != nil {
		return err
	}
	for _, streamed := range []bool{false, true} {
		input, _ := nativehttp.NewRequest("GET", target, nil)
		input.Header.Set("X-Discard", "no")
		options := p.RequestOptions{ExactHeaders: []fingerprint.HeaderPair{{Key: "X-Exact", Value: "one"}, {Key: "X-Exact", Value: "two"}}}
		var receipt *adapters.Receipt[p.Result]
		if streamed {
			stream, accepted, err := client.Open(ctx, input, options)
			if err != nil {
				return err
			}
			body, err := io.ReadAll(stream)
			if err != nil || string(body) != "body" {
				return errors.Join(errors.New("stream bytes changed"), err)
			}
			if err := stream.Close(ctx); err != nil {
				return err
			}
			receipt = accepted
		} else {
			receipt, err = client.Do(ctx, ctx, input, options)
			if err != nil {
				return err
			}
		}
		snapshot, err := receipt.WaitReleased(ctx)
		if err != nil || snapshot.Err() != nil {
			return errors.Join(err, snapshot.Err())
		}
		result, present := snapshot.ValueCopy()
		if result.Metadata().Protocol() != wantedProtocol {
			return errors.New("selected protocol silently changed")
		}
		if !present || !result.HasData() || !result.Complete() || !result.InputComplete() || result.Metadata().StatusCode() != 418 || result.TrailersCopy().Get("X-Final") != "done" || result.BytesRead() != 4 || result.WireBytesRead() != 4 || result.Attribution().ID != "public-direct" {
			return errors.New("native facts/attribution lost")
		}
		if !streamed && string(result.DataCopy()) != "body" || streamed && result.DataCopy() != nil {
			return errors.New("finite and stream storage confused")
		}
		delivery, err := inbox.NextReleased(ctx)
		if err != nil {
			return err
		}
		independent, err := delivery.Receipt()
		if err != nil {
			return err
		}
		observed, err := independent.WaitReleased(ctx)
		if err != nil || observed.Info() != snapshot.Info() {
			return errors.New("independent evidence changed")
		}
		beforeRetry := effects.Load()
		if err := delivery.Retry(); err != nil {
			return err
		}
		delivery, err = inbox.NextReleased(ctx)
		if err != nil {
			return err
		}
		repeated, err := delivery.Receipt()
		if err != nil {
			return err
		}
		repeatedSnapshot, err := repeated.WaitReleased(ctx)
		if err != nil || repeatedSnapshot.Info() != snapshot.Info() {
			return errors.New("evidence retry changed identity")
		}
		if effects.Load() != beforeRetry {
			return errors.New("evidence retry dispatched another HTTP request")
		}
		if err := delivery.Ack(); err != nil {
			return err
		}
	}
	if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
		return errors.Join(err, errors.New("source did not retire"))
	}
	delivery, err := inbox.NextReleased(ctx)
	if err != nil {
		return err
	}
	if err := delivery.Ack(); err != nil {
		return err
	}
	state, err := runtime.Inspect()
	if err != nil || state.Active != 0 || state.WorkBytes != 0 {
		return errors.New("runtime retained source/root reservation")
	}
	evidence, err := inbox.Inspect()
	if err != nil || evidence.Outstanding != 0 {
		return errors.New("evidence remained")
	}
	build, err := p.Build()
	if err != nil || !build.Framework.Present || len(build.SDKs) < 6 {
		return errors.New("actual module metadata absent")
	}
	return nil
}
