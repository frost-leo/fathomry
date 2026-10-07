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
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	nethttp "github.com/frost-leo/fathomry/adapters/httpclient/nethttp/v1"
	"github.com/frost-leo/fathomry/adapters/httpclient/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := direct(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("nethttp direct public consumer passed")
}

type socketTracker struct {
	dials  atomic.Int64
	closed atomic.Int64
}

func (tracker *socketTracker) dial(ctx context.Context, network, address string) (net.Conn, error) {
	connection, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	tracker.dials.Add(1)
	return &trackedConnection{Conn: connection, tracker: tracker}, nil
}

func (tracker *socketTracker) active() int64 { return tracker.dials.Load() - tracker.closed.Load() }

type trackedConnection struct {
	net.Conn
	tracker *socketTracker
	once    sync.Once
}

func (connection *trackedConnection) Close() error {
	err := connection.Conn.Close()
	if err == nil {
		connection.once.Do(func() { connection.tracker.closed.Add(1) })
	}
	return err
}

type heldExchange struct {
	proceed chan struct{}
	done    chan struct{}
	once    sync.Once
}

func (exchange *heldExchange) release() { exchange.once.Do(func() { close(exchange.proceed) }) }

type protocolPeer struct {
	server   *httptest.Server
	requests atomic.Int64
	mu       sync.Mutex
	held     map[string]*heldExchange
}

func newPeer() *protocolPeer {
	peer := &protocolPeer{held: make(map[string]*heldExchange)}
	peer.server = httptest.NewServer(http.HandlerFunc(peer.serveHTTP))
	return peer
}

func (peer *protocolPeer) hold(token string) *heldExchange {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	value := &heldExchange{proceed: make(chan struct{}), done: make(chan struct{})}
	peer.held[token] = value
	return value
}

func (peer *protocolPeer) close() {
	peer.mu.Lock()
	for _, exchange := range peer.held {
		exchange.release()
	}
	peer.mu.Unlock()
	peer.server.CloseClientConnections()
	peer.server.Close()
}

func (peer *protocolPeer) serveHTTP(writer http.ResponseWriter, request *http.Request) {
	peer.requests.Add(1)
	if request.URL.Path == "/held" {
		peer.mu.Lock()
		exchange := peer.held[request.URL.Query().Get("token")]
		peer.mu.Unlock()
		if exchange == nil {
			http.Error(writer, "unknown fixture token", http.StatusBadRequest)
			return
		}
		defer close(exchange.done)
		writer.Header().Set("X-Fixture", "held")
		_, _ = io.WriteString(writer, "prefix:")
		writer.(http.Flusher).Flush()
		select {
		case <-exchange.proceed:
			_, _ = io.WriteString(writer, "tail")
		case <-request.Context().Done():
		}
		return
	}
	content, err := io.ReadAll(io.LimitReader(request.Body, 4097))
	if err != nil || len(content) > 4096 {
		http.Error(writer, "invalid fixture input", http.StatusBadRequest)
		return
	}
	writer.Header().Set("X-Seen-Method", request.Method)
	writer.Header().Set("X-Seen-Host", request.Host)
	writer.Header().Set("X-Seen-Fixture", request.Header.Get("X-Fixture"))
	writer.Header().Set("X-Seen-Trailer", request.Trailer.Get("X-Request-Trailer"))
	writer.Header().Set("Trailer", "X-Response-Trailer")
	writer.WriteHeader(http.StatusTeapot)
	_, _ = writer.Write(content)
	writer.Header().Set("X-Response-Trailer", "finished")
}

func (peer *protocolPeer) request(path, body string) *http.Request {
	request, err := http.NewRequest(http.MethodPost, peer.server.URL+path, strings.NewReader(body))
	if err != nil {
		panic(err)
	}
	return request
}

func waitSignal(ctx context.Context, signal <-chan struct{}, description string) error {
	select {
	case <-signal:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%s: %w", description, ctx.Err())
	}
}

func cleanup(close func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = close(ctx)
}

type evidenceLedger struct {
	inbox    *adapters.Inbox[nethttp.Result]
	expected map[uint64]*adapters.Receipt[nethttp.Result]
	seen     map[uint64]adapters.Snapshot[nethttp.Result]
	owners   int
}

func newLedger(inbox *adapters.Inbox[nethttp.Result]) *evidenceLedger {
	return &evidenceLedger{inbox: inbox, expected: make(map[uint64]*adapters.Receipt[nethttp.Result]), seen: make(map[uint64]adapters.Snapshot[nethttp.Result])}
}

func (ledger *evidenceLedger) expect(receipt *adapters.Receipt[nethttp.Result]) error {
	if receipt == nil {
		return errors.New("accepted operation has no public receipt")
	}
	value, _ := receipt.Snapshot()
	if value.Info().Sequence == 0 {
		return errors.New("accepted operation has no public sequence")
	}
	ledger.expected[value.Info().Sequence] = receipt
	return nil
}

func (ledger *evidenceLedger) take(ctx context.Context, retry bool, peer *protocolPeer) error {
	delivery, err := ledger.inbox.NextReleased(ctx)
	if err != nil {
		return err
	}
	receipt, err := delivery.Receipt()
	if err != nil {
		return err
	}
	value, err := receipt.WaitReleased(ctx)
	if err != nil {
		return err
	}
	sequence := value.Info().Sequence
	if _, exists := ledger.seen[sequence]; exists {
		return fmt.Errorf("duplicate evidence sequence %d", sequence)
	}
	if expected := ledger.expected[sequence]; expected != nil {
		direct, err := expected.WaitReleased(ctx)
		if err != nil || direct.Info() != value.Info() || !value.Info().Released {
			return fmt.Errorf("direct/independent attribution differs for sequence %d", sequence)
		}
		actualResult, actualPresent := value.ValueCopy()
		directResult, directPresent := direct.ValueCopy()
		if actualPresent != directPresent || actualResult.Attribution() != directResult.Attribution() || actualResult.Complete() != directResult.Complete() ||
			actualResult.Connected() != directResult.Connected() || !bytes.Equal(actualResult.DataCopy(), directResult.DataCopy()) {
			return fmt.Errorf("direct/independent results differ for sequence %d", sequence)
		}
	} else if strings.HasSuffix(value.Info().Operation, ".open") {
		ledger.owners++
	} else {
		return fmt.Errorf("unregistered operation evidence %s", value.Info().Operation)
	}
	if retry {
		if peer == nil {
			return errors.New("retry control has no protocol peer")
		}
		before := peer.requests.Load()
		if err := delivery.Retry(); err != nil {
			return err
		}
		again, err := ledger.inbox.NextReleased(ctx)
		if err != nil {
			return err
		}
		repeated, err := again.Receipt()
		if err != nil {
			return err
		}
		observation, err := repeated.WaitReleased(ctx)
		if err != nil || observation.Info().Sequence != sequence || peer.requests.Load() != before {
			return errors.New("evidence retry changed identity or dispatched HTTP again")
		}
		delivery = again
	}
	ledger.seen[sequence] = value
	return delivery.Ack()
}

func (ledger *evidenceLedger) result(ctx context.Context, receipt *adapters.Receipt[nethttp.Result], directErr error) (nethttp.Result, error) {
	if directErr != nil {
		return nethttp.Result{}, directErr
	}
	if err := ledger.expect(receipt); err != nil {
		return nethttp.Result{}, err
	}
	value, err := receipt.WaitReleased(ctx)
	if err != nil || value.Err() != nil {
		return nethttp.Result{}, errors.Join(err, value.Err())
	}
	for {
		if _, exists := ledger.seen[value.Info().Sequence]; exists {
			break
		}
		if err := ledger.take(ctx, false, nil); err != nil {
			return nethttp.Result{}, err
		}
	}
	result, present := value.ValueCopy()
	if !present {
		return nethttp.Result{}, errors.New("accepted HTTP operation lost its result")
	}
	return result, nil
}

func (ledger *evidenceLedger) drain(ctx context.Context) error {
	for {
		status, err := ledger.inbox.Inspect()
		if err != nil {
			return err
		}
		if status.Outstanding == 0 {
			break
		}
		if err := ledger.take(ctx, false, nil); err != nil {
			return err
		}
	}
	for sequence := range ledger.expected {
		if _, exists := ledger.seen[sequence]; !exists {
			return fmt.Errorf("sequence %d never reached the independent receiver", sequence)
		}
	}
	return nil
}

func direct(ctx context.Context) error {
	peer := newPeer()
	defer peer.close()
	tracker := &socketTracker{}
	prepared, err := nethttp.Prepare(nethttp.Settings{Name: "direct"}, nethttp.NativeOptions{DialContext: tracker.dial})
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
	defer cleanup(runtime.Close)
	inbox, err := adapters.NewInbox[nethttp.Result](policy.Evidence)
	if err != nil {
		return err
	}
	owner, err := prepared.Open(ctx, nethttp.Dependencies{Runtime: runtime, Evidence: inbox})
	if owner != nil {
		defer cleanup(owner.Close)
	}
	if err != nil || owner == nil {
		return fmt.Errorf("source open: %w", err)
	}
	if tracker.dials.Load() != 0 {
		return errors.New("preparation or construction performed unexpected network work")
	}
	client, err := owner.Client().WithID("direct-fixture")
	if err != nil {
		return err
	}
	ledger := newLedger(inbox)
	requestContext, stopRequest := context.WithCancel(context.Background())
	stopRequest()
	request := peer.request("/finite", "payload").WithContext(requestContext)
	request.Method, request.Host = "PATCH", "fixture.invalid"
	request.Header.Set("X-Fixture", "request-header")
	request.ContentLength = -1
	request.TransferEncoding = []string{"chunked"}
	request.Trailer = http.Header{"X-Request-Trailer": {"fixed"}}
	receipt, err := client.Do(ctx, ctx, request)
	if err != nil {
		return err
	}
	if err := ledger.expect(receipt); err != nil {
		return err
	}
	first, err := receipt.WaitReleased(ctx)
	if err != nil || first.Err() != nil {
		return errors.Join(err, first.Err())
	}
	value, present := first.ValueCopy()
	metadata := value.Metadata()
	if !present || !value.Complete() || string(value.DataCopy()) != "payload" || metadata.StatusCode() != http.StatusTeapot ||
		metadata.HeadersCopy().Get("X-Seen-Method") != "PATCH" || metadata.HeadersCopy().Get("X-Seen-Host") != "fixture.invalid" ||
		metadata.HeadersCopy().Get("X-Seen-Fixture") != "request-header" || metadata.HeadersCopy().Get("X-Seen-Trailer") != "fixed" ||
		value.TrailersCopy().Get("X-Response-Trailer") != "finished" || value.Attribution().ID != "direct-fixture" ||
		value.Source().Name != "direct" || value.Attribution().Source.Generation != 0 {
		return errors.New("finite request, context authority, response or attribution changed")
	}
	content := value.DataCopy()
	content[0] = '!'
	headers := metadata.HeadersCopy()
	headers.Set("X-Seen-Fixture", "changed")
	if string(value.DataCopy()) != "payload" || metadata.HeadersCopy().Get("X-Seen-Fixture") != "request-header" {
		return errors.New("public result storage aliases caller copies")
	}
	if err := ledger.take(ctx, true, peer); err != nil {
		return err
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	unborrowed := &observedBody{Reader: strings.NewReader("untouched")}
	unadmitted := peer.request("/finite", "")
	unadmitted.Body, unadmitted.ContentLength = unborrowed, 9
	before := peer.requests.Load()
	refused, err := client.Do(canceled, ctx, unadmitted)
	if refused != nil || !errors.Is(err, context.Canceled) || unborrowed.reads.Load() != 0 || unborrowed.closes.Load() != 0 || peer.requests.Load() != before {
		return errors.New("pre-admission cancellation borrowed input or sent HTTP")
	}
	stream, receipt, err := client.Open(ctx, peer.request("/finite", "streamed"))
	if err != nil || stream == nil {
		return fmt.Errorf("stream open: %w", err)
	}
	read, err := io.ReadAll(stream)
	if err != nil || string(read) != "streamed" {
		return fmt.Errorf("incremental stream read: %w", err)
	}
	if err := stream.Close(ctx); err != nil {
		return err
	}
	value, err = ledger.result(ctx, receipt, nil)
	if err != nil || !value.Complete() || value.DataCopy() != nil {
		return errors.New("stream EOF or unretained-body semantics changed")
	}
	early := peer.hold("early")
	stream, receipt, err = client.Open(ctx, peer.request("/held?token=early", ""))
	if err != nil || stream == nil {
		return fmt.Errorf("early stream open: %w", err)
	}
	if err := readPrefix(stream); err != nil {
		return err
	}
	if err := stream.Close(ctx); err != nil {
		return err
	}
	value, err = ledger.result(ctx, receipt, nil)
	if err != nil || value.Complete() || value.DataCopy() != nil {
		return errors.New("abandoned response falsely completed or retained a body")
	}
	if err := waitSignal(ctx, early.done, "early response peer cleanup"); err != nil {
		return err
	}
	address, _ := url.Parse(peer.server.URL)
	connection, root, err := client.Connect(ctx, address.Scheme, address.Host)
	if err != nil || connection == nil {
		return fmt.Errorf("direct connection: %w", err)
	}
	if err := ledger.expect(root); err != nil {
		return err
	}
	child, childErr := connection.Do(ctx, ctx, peer.request("/finite", "child"))
	value, err = ledger.result(ctx, child, childErr)
	rootSnapshot, _ := root.Snapshot()
	if err != nil || string(value.DataCopy()) != "child" || value.Attribution().Parent != rootSnapshot.Info().Sequence || rootSnapshot.Info().Released {
		return errors.New("connection child lost parent attribution or released its parent")
	}
	if err := connection.Close(ctx); err != nil {
		return err
	}
	value, err = ledger.result(ctx, root, nil)
	if err != nil || !value.Connected() || !value.Complete() {
		return errors.New("connection terminal facts were lost")
	}
	if err := saturatedCleanup(ctx, peer, client, owner, ledger, policy); err != nil {
		return err
	}
	if tracker.dials.Load() == 0 || tracker.active() != 0 || !owner.ShutdownComplete() {
		return fmt.Errorf("source release left managed sockets: dials=%d active=%d", tracker.dials.Load(), tracker.active())
	}
	if err := ledger.drain(ctx); err != nil {
		return err
	}
	if ledger.owners != 1 {
		return errors.New("direct source lifetime evidence was not received exactly once")
	}
	if err := runtime.Close(ctx); err != nil {
		return err
	}
	usage, err := runtime.Inspect()
	if err != nil || usage.Active != 0 || usage.WorkBytes != 0 {
		return errors.New("direct shutdown retained operation work")
	}
	return nil
}

type observedBody struct {
	*strings.Reader
	reads  atomic.Int64
	closes atomic.Int64
}

func (body *observedBody) Read(output []byte) (int, error) {
	body.reads.Add(1)
	return body.Reader.Read(output)
}

func (body *observedBody) Close() error { body.closes.Add(1); return nil }

func readPrefix(reader io.Reader) error {
	content := make([]byte, len("prefix:"))
	if _, err := io.ReadFull(reader, content); err != nil {
		return err
	}
	if string(content) != "prefix:" {
		return errors.New("held response prefix changed")
	}
	return nil
}

func saturatedCleanup(ctx context.Context, peer *protocolPeer, client *nethttp.Client, owner *nethttp.Owner, ledger *evidenceLedger, policy httpclient.Policy) error {
	address, _ := url.Parse(peer.server.URL)
	connection, root, err := client.Connect(ctx, address.Scheme, address.Host)
	if err != nil || connection == nil {
		return fmt.Errorf("saturation connection: %w", err)
	}
	if err := ledger.expect(root); err != nil {
		return err
	}
	held := peer.hold("saturated")
	stream, child, err := connection.Open(ctx, peer.request("/held?token=saturated", ""))
	if err != nil || stream == nil {
		return fmt.Errorf("saturation child: %w", err)
	}
	if err := ledger.expect(child); err != nil {
		return err
	}
	if err := readPrefix(stream); err != nil {
		return err
	}
	refused := false
	for index := 0; index < policy.Evidence.Capacity+2; index++ {
		before := peer.requests.Load()
		receipt, err := client.Do(ctx, ctx, peer.request("/finite", "saturation"))
		if receipt == nil {
			if !errors.Is(err, adapters.ErrEvidence) || peer.requests.Load() != before {
				return fmt.Errorf("evidence refusal changed dispatch: %w", err)
			}
			refused = true
			break
		}
		if err != nil {
			return err
		}
		if err := ledger.expect(receipt); err != nil {
			return err
		}
		value, err := receipt.WaitReleased(ctx)
		if err != nil || value.Err() != nil {
			return errors.Join(err, value.Err())
		}
	}
	status, err := ledger.inbox.Inspect()
	if err != nil || !refused || status.Outstanding != policy.Evidence.Capacity {
		return errors.New("evidence saturation was not exercised")
	}
	before := peer.requests.Load()
	receipt, err := connection.Do(ctx, ctx, peer.request("/finite", "forbidden"))
	if receipt != nil || !errors.Is(err, adapters.ErrEvidence) || peer.requests.Load() != before {
		return errors.New("saturated child acquired work or bypassed evidence admission")
	}
	if err := owner.Close(ctx); err != nil {
		return fmt.Errorf("saturated source cleanup with live descendants: %w", err)
	}
	if err := connection.Close(ctx); err != nil {
		return fmt.Errorf("repeat connection cleanup: %w", err)
	}
	if err := stream.Close(ctx); err != nil {
		return fmt.Errorf("repeat child cleanup: %w", err)
	}
	if err := waitSignal(ctx, held.done, "saturated child peer cleanup"); err != nil {
		return err
	}
	rootValue, err := root.WaitReleased(ctx)
	if err != nil || rootValue.Err() != nil {
		return errors.Join(err, rootValue.Err())
	}
	childValue, err := child.WaitReleased(ctx)
	if err != nil || childValue.Err() != nil {
		return errors.Join(err, childValue.Err())
	}
	result, _ := childValue.ValueCopy()
	if result.Complete() {
		return errors.New("parent cleanup claimed child body EOF")
	}
	return nil
}
