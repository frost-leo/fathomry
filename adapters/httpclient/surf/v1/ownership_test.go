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

package surf_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	nativehttp "github.com/enetx/http"
	"github.com/enetx/http/cookiejar"
	sdk "github.com/enetx/surf"
	"github.com/enetx/surf/profiles"
	p "github.com/frost-leo/fathomry/adapters/httpclient/surf/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	utls "github.com/refraction-networking/utls"
)

func reviewContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func reviewMechanisms(t *testing.T, prepared p.Prepared) p.Dependencies {
	t.Helper()
	policy, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[p.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(reviewContext(t)); err != nil {
			t.Error(err)
		}
	})
	return p.Dependencies{Runtime: runtime, Evidence: inbox}
}

func reviewDrain(t *testing.T, inbox *adapters.Inbox[p.Result]) {
	t.Helper()
	for {
		status, err := inbox.Inspect()
		if err != nil {
			t.Fatal(err)
		}
		if status.Outstanding == 0 {
			return
		}
		record, err := inbox.NextReleased(reviewContext(t))
		if err != nil {
			t.Fatal(err)
		}
		if err := record.Ack(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPublicPreparedCopySourceAndNativeAuthority(t *testing.T) {
	var calls, dials atomic.Int64
	peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("X-Prepared") != "before" {
			t.Error("native header container was not frozen")
		}
		if calls.Add(1) == 1 {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(writer, request.Proto)
	}))
	defer peer.Close()
	mode, retries := p.HTTP1Only, 1
	codes := []int{http.StatusServiceUnavailable}
	headers := nativehttp.Header{"X-Prepared": {"before"}}
	native := p.NativeOptions{Headers: headers, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}}
	prepared, err := p.Prepare(p.Settings{Name: "frozen-public", Mode: &mode, NativeRetries: &retries, RetryCodes: codes}, native)
	if err != nil {
		t.Fatal(err)
	}
	before, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	mode, retries, codes[0], headers["X-Prepared"][0] = p.H2C, 0, http.StatusBadGateway, "after"
	after, err := prepared.Policy()
	if err != nil || !reflect.DeepEqual(before, after) || dials.Load() != 0 {
		t.Fatal("preparation changed or performed network work", err)
	}
	deps := reviewMechanisms(t, prepared)
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, extra := range map[string]p.NativeOptions{
		"profile": {Profile: &profiles.Variant{}},
		"os":      {OS: profiles.OSKey(1)},
		"hello": {HelloSpecFactory: func(context.Context) (utls.ClientHelloSpec, error) {
			t.Fatal("extra factory called")
			return utls.ClientHelloSpec{}, nil
		}},
		"tls": {TLSConfig: &tls.Config{}}, "ja": {JAConfig: &utls.Config{}}, "proxy-tls": {ProxyTLSConfig: &tls.Config{}},
		"headers": {Headers: nativehttp.Header{}},
		"jar":     {Jar: jar},
		"dial":    {DialContext: native.DialContext},
		"packet": {ListenPacket: func(context.Context, string, string) (net.PacketConn, error) {
			t.Fatal("extra packet callback called")
			return nil, nil
		}},
		"resolver":       {Resolver: &net.Resolver{}},
		"request-hooks":  {RequestMiddleware: []func(*sdk.Request) error{}},
		"response-hooks": {ResponseMiddleware: []func(*sdk.Response) error{}},
		"redirect":       {CheckRedirect: func(*nativehttp.Request, []*nativehttp.Request) error { t.Fatal("extra redirect called"); return nil }},
	} {
		t.Run(name, func(t *testing.T) {
			unexpected := deps
			unexpected.Native = extra
			if owner, err := prepared.Open(reviewContext(t), unexpected); owner != nil || !errors.Is(err, p.ErrInput) {
				t.Fatal("Prepared.Open accepted another native selection", err)
			}
			if client, err := p.Using(reviewContext(t), resource.Ref[p.Handle]{}, before.Budget, unexpected); client != nil || !errors.Is(err, p.ErrInput) {
				t.Fatal("Using granted new native authority", err)
			}
		})
	}
	status, _ := deps.Runtime.Inspect()
	if status.Accepted != 0 || dials.Load() != 0 {
		t.Fatal("rejected native authority retained work")
	}
	owner, err := prepared.Open(reviewContext(t), deps)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(context.Background())
	profile, err := owner.Client().Profile(reviewContext(t))
	if err != nil {
		t.Fatal(err)
	}
	choices := map[string]string{}
	for _, option := range profile.Options {
		choices[option.Name] = option.Value
	}
	if choices["mode"] != "http1" || choices["native-retries"] != "1" || choices["native-retry-codes-0"] != "503" {
		t.Fatal("effective profile changed after preparation", choices)
	}
	identity := owner.Info()
	if identity.Name != "frozen-public" || identity.Provider != p.ProviderID || identity.FormatVersion != 1 || len(identity.Provenance) == 0 || len(identity.Provenance[0].Fields) == 0 {
		t.Fatal("prepared source identity or provenance omitted")
	}
	identity.Provenance[0].Fields[0] = "mutated"
	profile.Options[0].Value = "mutated"
	again, err := owner.Client().Profile(reviewContext(t))
	if err != nil || owner.Info().Provenance[0].Fields[0] == "mutated" || again.Options[0].Value == "mutated" {
		t.Fatal("inspection returned mutable source aliases", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	request, _ := nativehttp.NewRequestWithContext(canceled, "GET", peer.URL, nil)
	receipt, err := owner.Client().Do(reviewContext(t), reviewContext(t), request)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(reviewContext(t))
	if err != nil || snapshot.Err() != nil {
		t.Fatal(err, snapshot.Err())
	}
	value, present := snapshot.ValueCopy()
	if !present || !value.Complete() || string(value.DataCopy()) != "HTTP/1.1" || calls.Load() != 2 || !reflect.DeepEqual(value.Source(), owner.Info()) {
		t.Fatal("copied retry/header/mode, method context or source facts changed", calls.Load())
	}
	resultIdentity := value.Source()
	resultIdentity.Provenance[0].Fields[0] = "mutated"
	if value.Source().Provenance[0].Fields[0] == "mutated" {
		t.Fatal("Result.Source returned a provenance alias")
	}
	if err := owner.Close(reviewContext(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("source release incomplete", err)
	}
	reviewDrain(t, deps.Evidence)
}

type reviewLateReader struct {
	closed atomic.Int64
	err    error
}

func (*reviewLateReader) Read([]byte) (int, error) { return 0, io.EOF }
func (input *reviewLateReader) Close() error       { input.closed.Add(1); return input.err }

func TestPublicLateMultipartNoticeAndActualSourceRelease(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) { _, _ = io.Copy(io.Discard, request.Body) }))
	defer func() { peer.CloseClientConnections(); peer.Close() }()
	mode := p.HTTP1Only
	prepared, err := p.Prepare(p.Settings{Name: "public-late-factory", Mode: &mode}, p.NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	deps := reviewMechanisms(t, prepared)
	owner, err := prepared.Open(reviewContext(t), deps)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(context.Background())
	entered, allowed := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(allowed) })
	defer unblock()
	factoryFailure := errors.New("public factory private canary")
	cleanupFailure := errors.New("public cleanup private canary")
	input := &reviewLateReader{err: cleanupFailure}
	ctx, cancel := context.WithCancel(reviewContext(t))
	defer cancel()
	done := make(chan *adapters.Receipt[p.Result], 1)
	go func() {
		request, _ := nativehttp.NewRequest("POST", peer.URL, nil)
		receipt, _ := owner.Client().Do(ctx, context.Background(), request, p.RequestOptions{Multipart: &p.Multipart{Parts: []p.Part{{Name: "late", FileName: "late.bin", Open: func(ctx context.Context) (io.ReadCloser, error) {
			close(entered)
			<-allowed
			if ctx.Err() == nil {
				return input, errors.New("factory context survived cancellation")
			}
			return input, factoryFailure
		}}}}})
		done <- receipt
	}()
	select {
	case <-entered:
	case <-reviewContext(t).Done():
		t.Fatal("factory was not entered")
	}
	cancel()
	var receipt *adapters.Receipt[p.Result]
	select {
	case receipt = <-done:
		if receipt == nil {
			t.Fatal("canceled admitted factory lost its receipt")
		}
	case <-reviewContext(t).Done():
		t.Fatal("canceled caller could not stop waiting")
	}
	short, stop := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer stop()
	if err := owner.Close(short); !errors.Is(err, context.DeadlineExceeded) || owner.ShutdownComplete() || input.closed.Load() != 0 {
		t.Fatal("source released before late factory returned", err)
	}
	snapshot, resolved := receipt.Snapshot()
	state, _ := deps.Runtime.Inspect()
	if resolved || snapshot.Info().Released || state.Active != 2 || state.WorkBytes == 0 {
		t.Fatal("late native work lost public owner/root reservations")
	}
	unblock()
	snapshot, err = receipt.WaitReleased(reviewContext(t))
	if err != nil || !errors.Is(snapshot.Primary(), context.Canceled) || !errors.Is(snapshot.Cleanup(), cleanupFailure) || input.closed.Load() != 1 {
		t.Fatal("late cleanup or primary ownership changed", err, snapshot.Err())
	}
	value, present := snapshot.ValueCopy()
	if !present || value.Complete() || value.Source().Name != "public-late-factory" {
		t.Fatal("partial native result was omitted or became complete")
	}
	notices := value.InputErrorsCopy()
	found := false
	for _, notice := range notices {
		if errors.Is(notice, factoryFailure) {
			found = true
		}
		if strings.Contains(fmt.Sprint(notice), "private canary") {
			t.Fatal("input notice leaked private cause")
		}
	}
	if !found {
		t.Fatal("late factory error disappeared from public input notices")
	}
	for index := range notices {
		notices[index] = nil
	}
	found = false
	for _, notice := range value.InputErrorsCopy() {
		found = found || errors.Is(notice, factoryFailure)
	}
	if !found {
		t.Fatal("input notice copy mutated retained result")
	}
	if err := owner.Close(reviewContext(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("source cleanup did not continue after timed-out Close", err)
	}
	reviewDrain(t, deps.Evidence)
	state, _ = deps.Runtime.Inspect()
	if state.Active != 0 || state.WorkBytes != 0 {
		t.Fatal("actual cleanup retained public work")
	}
}
