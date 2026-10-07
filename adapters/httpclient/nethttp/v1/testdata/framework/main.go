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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	nethttp "github.com/frost-leo/fathomry/adapters/httpclient/nethttp/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := fixedFollow(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("nethttp framework public consumer passed")
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
	owner   *nethttp.Owner
	tracker *socketTracker
}

func cloneSettings(value nethttp.Settings) nethttp.Settings {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	var copy nethttp.Settings
	if err := json.Unmarshal(data, &copy); err != nil {
		panic(err)
	}
	return copy
}

func fixedFollow(ctx context.Context) error {
	enabled := true
	original := nethttp.Settings{Name: "copy-control", HTTP1: &enabled, ProxyConnectHeader: map[string][]string{"X-Copy": {"original"}}}
	copied := cloneSettings(original)
	if !reflect.DeepEqual(original, copied) {
		return errors.New("Settings clone changed field values")
	}
	*copied.HTTP1 = false
	copied.ProxyConnectHeader["X-Copy"][0] = "changed"
	if !*original.HTTP1 || original.ProxyConnectHeader["X-Copy"][0] != "original" {
		return errors.New("Settings clone retained mutable pointer/map aliases")
	}
	peer := newPeer()
	defer peer.close()
	first := nethttp.Settings{Name: "first"}
	prepared, err := nethttp.Prepare(first, nethttp.NativeOptions{DialContext: (&socketTracker{}).dial})
	if err != nil {
		return err
	}
	policy, err := nethttp.Compose(prepared, prepared, prepared, prepared, prepared, prepared)
	if err != nil {
		return err
	}
	runtime, err := framework.New(ctx, framework.Options{Operations: policy.Runtime})
	if err != nil {
		return err
	}
	defer cleanup(runtime.Close)
	inbox, err := adapters.NewInbox[nethttp.Result](policy.Evidence)
	if err != nil {
		return err
	}
	dependencies := nethttp.Dependencies{Runtime: runtime.Operations(), Evidence: inbox}
	ledger := newLedger(inbox)
	constructed := make(chan constructedSource, 16)
	released := make(chan *nethttp.Owner, 16)
	refused := errors.New("fixture candidate refused")
	obsoleteGate := make(chan struct{})
	releaseObsolete := sync.OnceFunc(func() { close(obsoleteGate) })
	defer releaseObsolete()
	var ownersMu sync.Mutex
	var owners []constructedSource
	bind := func(name string, mode resource.Policy) (resource.Ref[nethttp.Handle], error) {
		return resource.Bind(runtime.Resources(), resource.Binding[nethttp.Settings, nethttp.Handle]{
			Name: name, Policy: mode,
			Select: func(view settings.View) (nethttp.Settings, error) {
				snapshot, err := settings.As[nethttp.Settings](view)
				if err != nil {
					return nethttp.Settings{}, err
				}
				return snapshot.ValueCopy()
			},
			Clone: cloneSettings,
			Equal: func(left, right nethttp.Settings) bool { return reflect.DeepEqual(left, right) },
			Build: func(ctx context.Context, value nethttp.Settings) (*resource.Instance[nethttp.Handle], error) {
				tracker := &socketTracker{}
				deps := dependencies
				deps.Native = nethttp.NativeOptions{DialContext: tracker.dial}
				owner, err := nethttp.Open(ctx, value, deps)
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
				instance := &resource.Instance[nethttp.Handle]{Value: owner.Handle(), Release: func(ctx context.Context) resource.ReleaseResult {
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
	beginApply := func(value nethttp.Settings) (*resource.Update, error) {
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		schema := configsource.Schema[nethttp.Settings]{Version: 1, Defaults: first,
			Validate: func(_ context.Context, value nethttp.Settings) error {
				_, err := nethttp.Prepare(value, nethttp.NativeOptions{})
				return err
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
	apply := func(value nethttp.Settings) error {
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
	awaitReleased := func(wanted ...*nethttp.Owner) error {
		pending := make(map[*nethttp.Owner]bool, len(wanted))
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
	fixed, err := nethttp.Using(ctx, fixedRef, policy.Budget, dependencies)
	if err != nil {
		return err
	}
	follow, err := nethttp.Using(ctx, followRef, policy.Budget, dependencies)
	if err != nil {
		return err
	}
	fixedStatus, _ := fixedRef.Inspect()
	oldStatus, _ := followRef.Inspect()
	address, _ := url.Parse(peer.server.URL)
	connection, root, err := follow.Connect(ctx, address.Scheme, address.Host)
	if err != nil || connection == nil {
		return fmt.Errorf("retained connection: %w", err)
	}
	if err := ledger.expect(root); err != nil {
		return err
	}
	held := peer.hold("old-generation")
	stream, child, err := connection.Open(ctx, peer.request("/held?token=old-generation", ""))
	if err != nil || stream == nil {
		return fmt.Errorf("retained child: %w", err)
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
	second := nethttp.Settings{Name: "second"}
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
	requestResult := func(client *nethttp.Client, body, source string, generation uint64) error {
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
	if old.owner.ShutdownComplete() {
		return errors.New("finishing a child revoked its retained connection")
	}
	child, childErr := connection.Do(ctx, ctx, peer.request("/finite", "old-child"))
	value, err = ledger.result(ctx, child, childErr)
	rootSnapshot, _ := root.Snapshot()
	if err != nil || string(value.DataCopy()) != "old-child" || value.Source().Name != "first" ||
		value.Attribution().Source.Generation != oldStatus.Generation || value.Attribution().Parent != rootSnapshot.Info().Sequence {
		return errors.New("connection child reacquired the replacement generation")
	}
	if err := connection.Close(ctx); err != nil {
		return err
	}
	value, err = ledger.result(ctx, root, nil)
	if err != nil || !value.Connected() || !value.Complete() || value.Attribution().Source.Generation != oldStatus.Generation {
		return errors.New("old connection terminal evidence changed generations")
	}
	if err := awaitReleased(old.owner); err != nil {
		return err
	}
	if old.tracker.active() != 0 {
		return errors.New("retired generation retained a real socket after confirmed release")
	}
	if err := apply(nethttp.Settings{Name: "refused"}); !errors.Is(err, refused) {
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
	if err := apply(nethttp.Settings{Name: "oversized", MaxResponseBytes: &largerBytes}); err != nil {
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
	if err != nil || !errors.Is(rejection.Err(), nethttp.ErrLimit) || peer.requests.Load() != before || larger.tracker.dials.Load() != 0 {
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
	obsoleteUpdate, err := beginApply(nethttp.Settings{Name: "obsolete"})
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
	latestUpdate, err := beginApply(nethttp.Settings{Name: "latest"})
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
