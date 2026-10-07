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
	"sync/atomic"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	"github.com/bogdanfinn/fhttp/cookiejar"
	sdk "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
	p "github.com/frost-leo/fathomry/adapters/httpclient/tlsclient/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/quic-go/quic-go/http3"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	for _, mode := range []string{"h1", "h2", "race-h3", "race-tcp"} {
		if err := run(ctx, mode); err != nil {
			fmt.Fprintln(os.Stderr, mode, err)
			os.Exit(1)
		}
	}
	fmt.Println("tlsclient direct public consumer passed")
}
func ptr[T any](value T) *T { return &value }
func run(ctx context.Context, mode string) error {
	var requests, hooks atomic.Int64
	handler := func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if mode == "race-tcp" && r.ProtoMajor == 3 {
			<-r.Context().Done()
			return
		}
		if mode == "race-h3" && r.ProtoMajor != 3 {
			<-r.Context().Done()
			return
		}
		if r.Header.Get("X-Hook") != "yes" {
			http.Error(w, "missing hook", 400)
			return
		}
		if r.URL.Path == "/redirect" {
			http.SetCookie(w, &http.Cookie{Name: "synthetic", Value: "jar", Path: "/"})
			http.Redirect(w, r, "/echo", 307)
			return
		}
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Cookie", r.Header.Get("Cookie"))
		w.Header().Set("Trailer", "X-End")
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write(body)
		w.Header().Set("X-End", "complete")
	}
	peer := httptest.NewUnstartedServer(http.HandlerFunc(handler))
	peer.EnableHTTP2 = mode != "h1"
	peer.StartTLS()
	defer peer.Close()
	roots := x509.NewCertPool()
	roots.AddCert(peer.Certificate())
	if strings.HasPrefix(mode, "race") {
		packet, err := net.ListenPacket("udp", peer.Listener.Addr().String())
		if err != nil {
			return err
		}
		server := &http3.Server{TLSConfig: &tls.Config{Certificates: peer.TLS.Certificates}, Handler: http.HandlerFunc(handler), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
		stopped := make(chan struct{})
		go func() { defer close(stopped); _ = server.Serve(packet) }()
		defer func() { _ = server.Close(); _ = packet.Close(); <-stopped }()
	}
	jar, _ := cookiejar.New(nil)
	profile := profiles.Chrome_144
	native := p.NativeOptions{Profile: &profile, Transport: &sdk.TransportOptions{RootCAs: roots}, Jar: jar}
	notice := errors.New("synthetic private post notice")
	native.PreHooks = []sdk.PreRequestHookFunc{func(request *fhttp.Request) error {
		hooks.Add(1)
		if request.Body != nil || request.GetBody != nil || request.TLS != nil {
			return errors.New("owning hook escaped")
		}
		request.Header.Set("X-Hook", "yes")
		return nil
	}}
	native.PostHooks = []sdk.PostResponseHookFunc{func(response *sdk.PostResponseContext) error {
		if response.Response != nil && (response.Response.Body != nil || response.Response.TLS != nil) {
			return errors.New("owning response escaped")
		}
		return notice
	}}
	protocol := p.Negotiated
	if mode == "h1" {
		protocol = p.HTTP1Only
	}
	if strings.HasPrefix(mode, "race") {
		protocol = p.HTTP3Racing
	}
	prepared, err := p.Prepare(p.Settings{Name: mode, Mode: &protocol, MaxActive: ptr(1), Bandwidth: ptr(true)}, native)
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
	client, err := owner.Client().WithID("synthetic-operation")
	if err != nil {
		return err
	}
	inspection, err := client.Profile(ctx)
	if err != nil {
		return err
	}
	corrected := false
	for _, option := range inspection.Options {
		if option.Name == "local-compatibility-revision" {
			corrected = option.Value == "v3"
		}
	}
	if !corrected {
		return errors.New("executing SDK correction not selected")
	}
	build, err := p.Build()
	if err != nil || len(build.SDKs) != 4 {
		return errors.New("actual selected SDK graph missing")
	}
	for _, module := range build.SDKs {
		if module.Path.Value == "github.com/bogdanfinn/tls-client" && module.Version.Value != "v1.16.0" {
			return errors.New("selected SDK upgraded")
		}
	}
	path := "/redirect"
	if strings.HasPrefix(mode, "race") {
		path = "/echo"
	}
	request, _ := fhttp.NewRequest("POST", peer.URL+path, strings.NewReader("replayed"))
	canceled, stop := context.WithCancel(context.Background())
	stop()
	request = request.WithContext(canceled)
	receipt, err := client.Do(ctx, ctx, request)
	if err != nil {
		return err
	}
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil || snapshot.Err() != nil {
		return errors.Join(err, snapshot.Err())
	}
	value, present := snapshot.ValueCopy()
	want := "HTTP/2.0"
	if mode == "h1" {
		want = "HTTP/1.1"
	}
	if mode == "race-h3" {
		want = "HTTP/3.0"
	}
	if !present || !value.Complete() || string(value.DataCopy()) != "replayed" || value.Metadata().Protocol() != want || value.Metadata().StatusCode() != 418 ||
		value.TrailersCopy().Get("X-End") != "complete" || value.Attribution().ID != "synthetic-operation" || value.Attempts().Exact || value.Source().Name != mode {
		return errors.New("native result or attribution changed")
	}
	if !strings.HasPrefix(mode, "race") && (!strings.Contains(value.Metadata().HeadersCopy().Get("X-Cookie"), "synthetic=jar") || value.Exchanges() != 2) {
		return errors.New("redirect/jar association changed")
	}
	if len(value.HookErrorsCopy()) < 1 || !errors.Is(value.HookErrorsCopy()[0], notice) {
		return errors.New("native notices were lost or promoted to failure")
	}
	before := requests.Load()
	if rejected, err := client.Do(ctx, ctx, request); rejected != nil || !errors.Is(err, adapters.ErrEvidence) || requests.Load() != before {
		return errors.New("unacknowledged evidence did not bound dispatch")
	}
	if err := take(ctx, inbox, receipt, false); err != nil {
		return err
	}
	request, _ = fhttp.NewRequest("POST", peer.URL+path, strings.NewReader("replayed"))
	stream, receipt, err := client.Open(ctx, request)
	if err != nil {
		return err
	}
	content, err := io.ReadAll(stream)
	if err != nil || string(content) != "replayed" {
		return errors.New("native stream changed")
	}
	stopped, stopWait := context.WithCancel(ctx)
	stopWait()
	if _, err := receipt.WaitReleased(stopped); err == nil {
		return errors.New("EOF released retained stream")
	}
	if err := stream.Close(ctx); err != nil {
		return err
	}
	if err := take(ctx, inbox, receipt, true); err != nil {
		return err
	}
	measured, err := client.Bandwidth(ctx)
	if err != nil || !measured.Enabled() || measured.Scope() != "origin-tls-over-tcp" || measured.Source().Name != mode {
		return errors.New("native bandwidth attribution missing")
	}
	read, available := measured.ReadBytes()
	if !available || (!strings.HasPrefix(mode, "race") && read <= 16) {
		return errors.New("measured TLS traffic was unavailable or replaced by body count")
	}
	if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
		return errors.Join(errors.New("native source not released"), err)
	}
	record, err := inbox.NextReleased(ctx)
	if err != nil {
		return err
	}
	if err := record.Ack(); err != nil {
		return err
	}
	status, _ := runtime.Inspect()
	custody, _ := inbox.Inspect()
	if status.Active != 0 || status.WorkBytes != 0 || custody.Outstanding != 0 || hooks.Load() < 2 {
		return errors.New("work/evidence did not drain")
	}
	return nil
}
func take(ctx context.Context, inbox *adapters.Inbox[p.Result], receipt *adapters.Receipt[p.Result], stream bool) error {
	direct, err := receipt.WaitReleased(ctx)
	if err != nil || direct.Err() != nil {
		return errors.Join(err, direct.Err())
	}
	record, err := inbox.NextReleased(ctx)
	if err != nil {
		return err
	}
	independent, err := record.Receipt()
	if err != nil {
		return err
	}
	observed, err := independent.WaitReleased(ctx)
	if err != nil || observed.Info() != direct.Info() {
		return errors.New("independent evidence mismatched identity")
	}
	left, ok := direct.ValueCopy()
	right, present := observed.ValueCopy()
	if !ok || !present || !left.Complete() || !right.Complete() || !bytes.Equal(left.DataCopy(), right.DataCopy()) || left.Attribution() != right.Attribution() || (stream && left.DataCopy() != nil) {
		return errors.New("independent result differed or stream became materialized")
	}
	if err := record.Retry(); err != nil {
		return err
	}
	retry, err := inbox.NextReleased(ctx)
	if err != nil {
		return err
	}
	again, err := retry.Receipt()
	if err != nil {
		return err
	}
	snapshot, _ := again.Snapshot()
	if snapshot.Info() != observed.Info() {
		return errors.New("evidence retry changed actual operation")
	}
	return retry.Ack()
}
