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
	"encoding/json"
	"errors"
	"fmt"
	nativehttp "github.com/enetx/http"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/enetx/surf/profiles/chrome"
	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	surf "github.com/frost-leo/fathomry/adapters/httpclient/surf/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/quicvarint"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := fixedFollow(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := dynamicFollow(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("surf framework public consumer passed")
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

func (peer *protocolPeer) request(path, body string) *nativehttp.Request {
	request, err := nativehttp.NewRequest(http.MethodPost, peer.server.URL+path, strings.NewReader(body))
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
	inbox    *adapters.Inbox[surf.Result]
	expected map[uint64]*adapters.Receipt[surf.Result]
	seen     map[uint64]adapters.Snapshot[surf.Result]
	owners   int
}

func newLedger(inbox *adapters.Inbox[surf.Result]) *evidenceLedger {
	return &evidenceLedger{inbox: inbox, expected: make(map[uint64]*adapters.Receipt[surf.Result]), seen: make(map[uint64]adapters.Snapshot[surf.Result])}
}

func (ledger *evidenceLedger) expect(receipt *adapters.Receipt[surf.Result]) error {
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
			!bytes.Equal(actualResult.DataCopy(), directResult.DataCopy()) {
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

func (ledger *evidenceLedger) result(ctx context.Context, receipt *adapters.Receipt[surf.Result], directErr error) (surf.Result, error) {
	if directErr != nil {
		return surf.Result{}, directErr
	}
	if err := ledger.expect(receipt); err != nil {
		return surf.Result{}, err
	}
	value, err := receipt.WaitReleased(ctx)
	if err != nil || value.Err() != nil {
		return surf.Result{}, errors.Join(err, value.Err())
	}
	for {
		if _, exists := ledger.seen[value.Info().Sequence]; exists {
			break
		}
		if err := ledger.take(ctx, false, nil); err != nil {
			return surf.Result{}, err
		}
	}
	result, present := value.ValueCopy()
	if !present {
		return surf.Result{}, errors.New("accepted HTTP operation lost its result")
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

type constructedSource struct {
	binding string
	name    string
	owner   *surf.Owner
	tracker *socketTracker
}

func cloneSettings(value surf.Settings) surf.Settings {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	var copy surf.Settings
	if err := json.Unmarshal(data, &copy); err != nil {
		panic(err)
	}
	return copy
}

func fixedFollow(ctx context.Context) error {
	locked := false
	original := surf.Settings{Name: "copy-control", RoutingLocked: &locked}
	copied := cloneSettings(original)
	if !reflect.DeepEqual(original, copied) {
		return errors.New("Settings clone changed values")
	}
	*copied.RoutingLocked = true
	if *original.RoutingLocked {
		return errors.New("Settings clone retained pointer alias")
	}
	peer := newPeer()
	defer peer.close()
	first := surf.Settings{Name: "first"}
	prepared, err := surf.Prepare(first, nativeOptions(&socketTracker{}))
	if err != nil {
		return err
	}
	policy, err := surf.Compose(prepared, prepared, prepared, prepared, prepared, prepared)
	if err != nil {
		return err
	}
	runtime, err := framework.New(ctx, framework.Options{Operations: policy.Runtime})
	if err != nil {
		return err
	}
	defer cleanup(runtime.Close)
	inbox, err := adapters.NewInbox[surf.Result](policy.Evidence)
	if err != nil {
		return err
	}
	dependencies := surf.Dependencies{Runtime: runtime.Operations(), Evidence: inbox}
	ledger := newLedger(inbox)
	constructed := make(chan constructedSource, 16)
	released := make(chan *surf.Owner, 16)
	refused := errors.New("fixture candidate refused")
	obsoleteGate := make(chan struct{})
	releaseObsolete := sync.OnceFunc(func() { close(obsoleteGate) })
	defer releaseObsolete()
	var ownersMu sync.Mutex
	var owners []constructedSource
	bind := func(name string, mode resource.Policy) (resource.Ref[surf.Handle], error) {
		return resource.Bind(runtime.Resources(), resource.Binding[surf.Settings, surf.Handle]{
			Name: name, Policy: mode,
			Select: func(view settings.View) (surf.Settings, error) {
				snapshot, err := settings.As[surf.Settings](view)
				if err != nil {
					return surf.Settings{}, err
				}
				return snapshot.ValueCopy()
			},
			Clone: cloneSettings,
			Equal: func(left, right surf.Settings) bool { return reflect.DeepEqual(left, right) },
			Build: func(ctx context.Context, value surf.Settings) (*resource.Instance[surf.Handle], error) {
				tracker := &socketTracker{}
				deps := dependencies
				deps.Native = nativeOptions(tracker)
				owner, err := surf.Open(ctx, value, deps)
				if owner == nil {
					return nil, err
				}
				entry := constructedSource{binding: name, name: value.Name, owner: owner, tracker: tracker}
				ownersMu.Lock()
				owners = append(owners, entry)
				ownersMu.Unlock()
				constructed <- entry
				if value.Name == "obsolete" {
					<-obsoleteGate
				}
				var notified sync.Once
				instance := &resource.Instance[surf.Handle]{Value: owner.Handle(), Release: func(ctx context.Context) resource.ReleaseResult {
					result := owner.Release(ctx)
					if result.Complete {
						notified.Do(func() { released <- owner })
					}
					return result
				}}
				if value.Name == "refused" {
					return instance, errors.Join(err, refused)
				}
				return instance, err
			},
		})
	}
	fixedRef, err := bind("fixed", resource.Fixed)
	if err != nil {
		return err
	}
	followRef, err := bind("follow", resource.Follow)
	if err != nil {
		return err
	}
	beginApply := func(value surf.Settings) (*resource.Update, error) {
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		schema := configsource.Schema[surf.Settings]{Version: 1, Defaults: first,
			Validate: func(_ context.Context, value surf.Settings) error {
				return surf.Validate(value)
			}}
		prepared, err := configsource.Prepare(ctx, schema, []configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: raw}})
		if err != nil {
			return nil, err
		}
		snapshot, err := prepared.Snapshot()
		if err != nil {
			return nil, err
		}
		return runtime.Resources().Apply(ctx, snapshot.View())
	}
	apply := func(value surf.Settings) error {
		update, err := beginApply(value)
		if err != nil {
			return err
		}
		return update.Wait(ctx)
	}
	nextConstructed := func() (constructedSource, error) {
		select {
		case source := <-constructed:
			return source, nil
		case <-ctx.Done():
			return constructedSource{}, errors.New("candidate construction missing")
		}
	}
	awaitReleased := func(wanted ...*surf.Owner) error {
		pending := make(map[*surf.Owner]bool, len(wanted))
		for _, owner := range wanted {
			pending[owner] = true
		}
		for len(pending) != 0 {
			select {
			case owner := <-released:
				if !pending[owner] || !owner.ShutdownComplete() {
					return errors.New("wrong source finalized or release was unconfirmed")
				}
				delete(pending, owner)
			case <-ctx.Done():
				return errors.New("retired or failed source was not released")
			}
		}
		return nil
	}
	if err := apply(first); err != nil {
		return err
	}
	var old, fixedOwner constructedSource
	for range 2 {
		source, err := nextConstructed()
		if err != nil {
			return err
		}
		if source.binding == "follow" {
			old = source
		} else {
			fixedOwner = source
		}
	}
	if old.owner == nil || fixedOwner.owner == nil {
		return errors.New("initial Fixed/Follow sources were not both constructed")
	}
	fixed, err := surf.Using(ctx, fixedRef, policy.Budget, dependencies)
	if err != nil {
		return err
	}
	follow, err := surf.Using(ctx, followRef, policy.Budget, dependencies)
	if err != nil {
		return err
	}
	fixedStatus, _ := fixedRef.Inspect()
	oldStatus, _ := followRef.Inspect()
	held := peer.hold("old-generation")
	stream, child, err := follow.Open(ctx, peer.request("/held?token=old-generation", ""))
	if err != nil || stream == nil {
		return fmt.Errorf("retained root: %w", err)
	}
	if err := ledger.expect(child); err != nil {
		return err
	}
	if err := readPrefix(stream); err != nil {
		return err
	}
	if old.tracker.active() < 1 {
		return errors.New("old source never acquired a tracked socket")
	}
	second := surf.Settings{Name: "second"}
	if err := apply(second); err != nil {
		return err
	}
	current, err := nextConstructed()
	if err != nil {
		return err
	}
	currentStatus, _ := followRef.Inspect()
	if current.binding != "follow" || current.name != "second" || currentStatus.Generation == oldStatus.Generation || currentStatus.Retiring != 1 || old.owner.ShutdownComplete() {
		return errors.New("Follow replacement revoked or lost its retained generation")
	}
	if status, _ := fixedRef.Inspect(); status.Generation != fixedStatus.Generation {
		return errors.New("Fixed source changed generation")
	}
	requestResult := func(client *surf.Client, body, source string, generation uint64) error {
		receipt, err := client.Do(ctx, ctx, peer.request("/finite", body))
		result, err := ledger.result(ctx, receipt, err)
		if err != nil {
			return err
		}
		if string(result.DataCopy()) != body || result.Source().Name != source || result.Attribution().Source.Generation != generation {
			return errors.New("root response or actual generation attribution changed")
		}
		return nil
	}
	if err := requestResult(follow, "new", "second", currentStatus.Generation); err != nil {
		return err
	}
	if err := requestResult(fixed, "fixed", "first", fixedStatus.Generation); err != nil {
		return err
	}
	held.release()
	remaining, err := io.ReadAll(stream)
	if err != nil || string(remaining) != "tail" {
		return fmt.Errorf("old-generation response continuation: %w", err)
	}
	if err := stream.Close(ctx); err != nil {
		return err
	}
	value, err := ledger.result(ctx, child, nil)
	if err != nil || !value.Complete() || value.Source().Name != "first" || value.Attribution().Source.Generation != oldStatus.Generation {
		return errors.New("retained stream retargeted or lost completion evidence")
	}
	if err := awaitReleased(old.owner); err != nil {
		return err
	}
	if old.tracker.active() != 0 {
		return errors.New("retired generation retained a real socket after confirmed release")
	}
	if err := apply(surf.Settings{Name: "refused"}); !errors.Is(err, refused) {
		return fmt.Errorf("failed candidate lost its original cause: %w", err)
	}
	candidate, err := nextConstructed()
	if err != nil {
		return err
	}
	if candidate.name != "refused" {
		return errors.New("wrong failed candidate was constructed")
	}
	if err := awaitReleased(candidate.owner); err != nil {
		return err
	}
	if status, _ := followRef.Inspect(); status.Generation != currentStatus.Generation {
		return errors.New("failed candidate discarded the last good source")
	}
	if err := requestResult(follow, "last-good", "second", currentStatus.Generation); err != nil {
		return err
	}
	largerBytes := int64(16 << 20)
	if err := apply(surf.Settings{Name: "oversized", MaxResponseBytes: &largerBytes}); err != nil {
		return err
	}
	larger, err := nextConstructed()
	if err != nil {
		return err
	}
	if larger.name != "oversized" || larger.tracker.dials.Load() != 0 {
		return errors.New("larger generation did not remain inert before use")
	}
	before := peer.requests.Load()
	rejected, rejectedErr := follow.Do(ctx, ctx, peer.request("/finite", "must-not-send"))
	if rejectedErr != nil || rejected == nil {
		return fmt.Errorf("larger generation refusal lost admitted evidence: %w", rejectedErr)
	}
	if err := ledger.expect(rejected); err != nil {
		return err
	}
	rejection, err := rejected.WaitReleased(ctx)
	if err != nil || !errors.Is(rejection.Err(), surf.ErrLimit) || peer.requests.Load() != before || larger.tracker.dials.Load() != 0 {
		return errors.New("larger generation bypassed its frozen Using budget before native dispatch")
	}
	if _, present := rejection.ValueCopy(); present {
		return errors.New("undispatched larger generation invented a native result")
	}
	for {
		if _, seen := ledger.seen[rejection.Info().Sequence]; seen {
			break
		}
		if err := ledger.take(ctx, false, nil); err != nil {
			return err
		}
	}
	if err := awaitReleased(current.owner); err != nil {
		return err
	}
	current = larger
	currentStatus, _ = followRef.Inspect()
	obsoleteUpdate, err := beginApply(surf.Settings{Name: "obsolete"})
	if err != nil {
		return err
	}
	obsolete, err := nextConstructed()
	if err != nil {
		return err
	}
	if obsolete.name != "obsolete" || obsolete.owner.ShutdownComplete() {
		return errors.New("obsolete candidate gate was not reached with live ownership")
	}
	latestUpdate, err := beginApply(surf.Settings{Name: "latest"})
	if err != nil {
		return err
	}
	if err := obsoleteUpdate.Wait(ctx); !errors.Is(err, resource.ErrSuperseded) {
		return errors.New("new settings did not supersede the gated candidate")
	}
	if status, _ := followRef.Inspect(); status.Generation != currentStatus.Generation {
		return errors.New("obsolete candidate replaced last-good ownership before qualification")
	}
	releaseObsolete()
	if err := latestUpdate.Wait(ctx); err != nil {
		return err
	}
	latest, err := nextConstructed()
	if err != nil {
		return err
	}
	latestStatus, _ := followRef.Inspect()
	if latest.name != "latest" || latestStatus.Generation == currentStatus.Generation {
		return errors.New("latest source was not adopted")
	}
	if err := awaitReleased(obsolete.owner, current.owner); err != nil {
		return err
	}
	if err := requestResult(follow, "latest", "latest", latestStatus.Generation); err != nil {
		return err
	}
	if err := requestResult(fixed, "still-fixed", "first", fixedStatus.Generation); err != nil {
		return err
	}
	if err := runtime.Close(ctx); err != nil {
		return err
	}
	if err := awaitReleased(fixedOwner.owner, latest.owner); err != nil {
		return err
	}
	if err := ledger.drain(ctx); err != nil {
		return err
	}
	ownersMu.Lock()
	defer ownersMu.Unlock()
	if ledger.owners != len(owners) {
		return fmt.Errorf("source records differ from constructed owners: records=%d owners=%d", ledger.owners, len(owners))
	}
	for _, source := range owners {
		if !source.owner.ShutdownComplete() || source.tracker.active() != 0 {
			return fmt.Errorf("source %s/%s retained authority or sockets", source.binding, source.name)
		}
	}
	usage, err := runtime.Operations().Inspect()
	if err != nil || usage.Active != 0 || usage.WorkBytes != 0 {
		return errors.New("Framework cleanup retained public work reservations")
	}
	return nil
}

func nativeOptions(tracker *socketTracker) surf.NativeOptions {
	return surf.NativeOptions{DialContext: tracker.dial}
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

func dynamicFollow(ctx context.Context) error {
	hold := make(chan struct{})
	release := sync.OnceFunc(func() { close(hold) })
	defer release()
	peer, err := newDynamicQPACKPeer(ctx, hold)
	if err != nil {
		return err
	}
	defer peer.Close()
	mode := surf.PreferHTTP3
	profile := chrome.Desktop
	native := surf.NativeOptions{Profile: &profile, TLSConfig: &tls.Config{RootCAs: peer.Roots}}
	first := surf.Settings{Name: "qpack-first", Mode: &mode}
	prepared, err := surf.Prepare(first, native)
	if err != nil {
		return err
	}
	policy, err := surf.Compose(prepared, prepared, prepared)
	if err != nil {
		return err
	}
	runtime, err := framework.New(ctx, framework.Options{Operations: policy.Runtime})
	if err != nil {
		return err
	}
	defer cleanup(runtime.Close)
	inbox, err := adapters.NewInbox[surf.Result](policy.Evidence)
	if err != nil {
		return err
	}
	dependencies := surf.Dependencies{Runtime: runtime.Operations(), Evidence: inbox}
	ledger := newLedger(inbox)
	created := make(chan constructedSource, 4)
	bind := func(name string, mode resource.Policy) (resource.Ref[surf.Handle], error) {
		return resource.Bind(runtime.Resources(), resource.Binding[surf.Settings, surf.Handle]{
			Name: name, Policy: mode, Clone: cloneSettings, Equal: func(a, b surf.Settings) bool { return reflect.DeepEqual(a, b) },
			Select: func(view settings.View) (surf.Settings, error) {
				snapshot, err := settings.As[surf.Settings](view)
				if err != nil {
					return surf.Settings{}, err
				}
				return snapshot.ValueCopy()
			},
			Build: func(ctx context.Context, value surf.Settings) (*resource.Instance[surf.Handle], error) {
				deps := dependencies
				deps.Native = native
				owner, err := surf.Open(ctx, value, deps)
				if owner == nil {
					return nil, err
				}
				created <- constructedSource{binding: name, name: value.Name, owner: owner}
				return &resource.Instance[surf.Handle]{Value: owner.Handle(), Release: owner.Release}, err
			},
		})
	}
	fixedRef, err := bind("qpack-fixed", resource.Fixed)
	if err != nil {
		return err
	}
	followRef, err := bind("qpack-follow", resource.Follow)
	if err != nil {
		return err
	}
	apply := func(value surf.Settings) error {
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		prepared, err := configsource.Prepare(ctx, configsource.Schema[surf.Settings]{Version: 1, Defaults: first, Validate: func(_ context.Context, value surf.Settings) error { return surf.Validate(value) }}, []configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: raw}})
		if err != nil {
			return err
		}
		snapshot, err := prepared.Snapshot()
		if err != nil {
			return err
		}
		update, err := runtime.Resources().Apply(ctx, snapshot.View())
		if err != nil {
			return err
		}
		return update.Wait(ctx)
	}
	if err := apply(first); err != nil {
		return err
	}
	var old *surf.Owner
	for range 2 {
		select {
		case entry := <-created:
			if entry.binding == "qpack-follow" {
				old = entry.owner
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	fixed, err := surf.Using(ctx, fixedRef, policy.Budget, dependencies)
	if err != nil {
		return err
	}
	follow, err := surf.Using(ctx, followRef, policy.Budget, dependencies)
	if err != nil {
		return err
	}
	oldStatus, _ := followRef.Inspect()
	input, _ := nativehttp.NewRequest("GET", peer.URL, nil)
	stream, receipt, err := follow.Open(ctx, input)
	if err != nil {
		return err
	}
	defer stream.Close(context.Background())
	if err := ledger.expect(receipt); err != nil {
		return err
	}
	oldValue := stream.Metadata().HeadersCopy().Get("X-Qpack")
	if oldValue != "dynamic-1" {
		return errors.New("old dynamic table header missing")
	}
	prefix := make([]byte, 7)
	if _, err := io.ReadFull(stream, prefix); err != nil || string(prefix) != "dynamic" {
		return errors.Join(err, errors.New("dynamic old body prefix missing"))
	}
	second := cloneSettings(first)
	second.Name = "qpack-second"
	if err := apply(second); err != nil {
		return err
	}
	select {
	case <-created:
	case <-ctx.Done():
		return ctx.Err()
	}
	currentStatus, _ := followRef.Inspect()
	if old == nil || old.ShutdownComplete() || currentStatus.Generation == oldStatus.Generation || currentStatus.Retiring != 1 {
		return errors.New("dynamic old source was not retained")
	}
	for _, selection := range []struct {
		client *surf.Client
		name   string
	}{{follow, "qpack-second"}, {fixed, "qpack-first"}} {
		input, _ := nativehttp.NewRequest("GET", peer.URL, nil)
		got, err := selection.client.Do(ctx, ctx, input)
		result, err := ledger.result(ctx, got, err)
		if err != nil {
			return err
		}
		value := result.Metadata().HeadersCopy().Get("X-Qpack")
		if !result.Complete() || result.Source().Name != selection.name || value == "" || value == oldValue || result.TrailersCopy().Get("X-Qpack") != value {
			return errors.New("dynamic generation or table isolation changed")
		}
	}
	release()
	if data, err := io.ReadAll(stream); err != nil || len(data) != 0 {
		return errors.Join(err, errors.New("held dynamic trailer failed"))
	}
	if err := peer.WaitAcknowledged(ctx, 6); err != nil {
		return err
	}
	result, err := ledger.result(ctx, receipt, stream.Close(ctx))
	if err != nil {
		return err
	}
	if !result.Complete() || result.TrailersCopy().Get("X-Qpack") != oldValue || result.Attribution().Source.Generation != oldStatus.Generation {
		return errors.New("retired dynamic table or generation was lost")
	}
	if err := runtime.Close(ctx); err != nil {
		return err
	}
	if err := ledger.drain(ctx); err != nil {
		return err
	}
	status, _ := runtime.Operations().Inspect()
	if status.Active != 0 || status.WorkBytes != 0 || !old.ShutdownComplete() {
		return errors.New("dynamic Framework source was not released")
	}
	return peer.Close()
}
