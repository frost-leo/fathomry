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
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	p "github.com/frost-leo/fathomry/adapters/httpclient/httpcloak/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	nativehttp "github.com/sardanioss/http"
)

func ptr[T any](value T) *T { return &value }
func clone(value p.Settings) p.Settings {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	var result p.Settings
	if err := json.Unmarshal(raw, &result); err != nil {
		panic(err)
	}
	return result
}
func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("httpcloak framework public consumer passed")
}

type source struct {
	binding, name string
	owner         *p.Owner
}
type ledger struct {
	inbox    *adapters.Inbox[p.Result]
	expected map[uint64]*adapters.Receipt[p.Result]
	received map[uint64]bool
}

func (record *ledger) expect(receipt *adapters.Receipt[p.Result]) error {
	if receipt == nil {
		return errors.New("missing admitted receipt")
	}
	snapshot, _ := receipt.Snapshot()
	if snapshot.Info().Sequence == 0 {
		return errors.New("admitted receipt identity absent")
	}
	record.expected[snapshot.Info().Sequence] = receipt
	return nil
}
func (record *ledger) take(ctx context.Context) error {
	delivery, err := record.inbox.NextReleased(ctx)
	if err != nil {
		return err
	}
	receipt, err := delivery.Receipt()
	if err != nil {
		return err
	}
	observed, err := receipt.WaitReleased(ctx)
	if err != nil {
		return err
	}
	sequence := observed.Info().Sequence
	if record.received[sequence] {
		return errors.New("duplicate independent evidence")
	}
	if direct := record.expected[sequence]; direct != nil {
		expected, err := direct.WaitReleased(ctx)
		if err != nil || expected.Info() != observed.Info() {
			return errors.New("independent lineage changed")
		}
		left, leftPresent := expected.ValueCopy()
		right, rightPresent := observed.ValueCopy()
		if leftPresent != rightPresent || left.Attribution() != right.Attribution() || left.Complete() != right.Complete() || !bytes.Equal(left.DataCopy(), right.DataCopy()) {
			return errors.New("independent result changed")
		}
	} else if !strings.HasSuffix(observed.Info().Operation, ".open") {
		return errors.New("unexpected operation evidence")
	}
	record.received[sequence] = true
	return delivery.Ack()
}
func (record *ledger) result(ctx context.Context, receipt *adapters.Receipt[p.Result], directErr error) (p.Result, error) {
	if directErr != nil {
		return p.Result{}, directErr
	}
	if err := record.expect(receipt); err != nil {
		return p.Result{}, err
	}
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil || snapshot.Err() != nil {
		return p.Result{}, errors.Join(err, snapshot.Err())
	}
	for !record.received[snapshot.Info().Sequence] {
		if err := record.take(ctx); err != nil {
			return p.Result{}, err
		}
	}
	value, present := snapshot.ValueCopy()
	if !present {
		return value, errors.New("missing native result")
	}
	return value, nil
}
func (record *ledger) drain(ctx context.Context) error {
	for {
		state, err := record.inbox.Inspect()
		if err != nil {
			return err
		}
		if state.Outstanding == 0 {
			break
		}
		if err := record.take(ctx); err != nil {
			return err
		}
	}
	for sequence := range record.expected {
		if !record.received[sequence] {
			return errors.New("required evidence never delivered")
		}
	}
	return nil
}

func run(ctx context.Context) error {
	streamGate, callbackEntered, callbackGate, obsoleteGate := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	releaseStream := sync.OnceFunc(func() { close(streamGate) })
	defer releaseStream()
	releaseCallback := sync.OnceFunc(func() { close(callbackGate) })
	defer releaseCallback()
	releaseObsolete := sync.OnceFunc(func() { close(obsoleteGate) })
	defer releaseObsolete()
	lateCause := errors.New("late native veto")
	var effects, activeSockets atomic.Int64
	peer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		effects.Add(1)
		switch r.URL.Path {
		case "/held":
			_, _ = io.WriteString(w, "prefix")
			w.(http.Flusher).Flush()
			select {
			case <-streamGate:
				_, _ = io.WriteString(w, "tail")
			case <-r.Context().Done():
			}
		case "/redirect":
			http.Redirect(w, r, "/final", http.StatusFound)
		default:
			_, _ = io.WriteString(w, "ok")
		}
	}))
	peer.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			activeSockets.Add(1)
		}
		if state == http.StateClosed || state == http.StateHijacked {
			activeSockets.Add(-1)
		}
	}
	peer.Start()
	defer peer.Close()
	first := p.Settings{Name: "first", PresetName: "chrome-148", Protocol: ptr(p.HTTP1), DisableECH: ptr(true), MaxActive: ptr(2), MaxBindings: ptr(2), MaxConnections: ptr(8)}
	original := clone(first)
	*original.Protocol = p.HTTP3
	if *first.Protocol != p.HTTP1 {
		return errors.New("configuration clone aliased pointers")
	}
	prepared, err := p.Prepare(first, p.NativeOptions{})
	if err != nil {
		return err
	}
	policy, err := p.Compose(prepared, prepared, prepared, prepared, prepared, prepared)
	if err != nil {
		return err
	}
	runtime, err := framework.New(ctx, framework.Options{Operations: policy.Runtime})
	if err != nil {
		return err
	}
	defer func() {
		releaseStream()
		releaseCallback()
		releaseObsolete()
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = runtime.Close(cleanup)
	}()
	inbox, err := adapters.NewInbox[p.Result](policy.Evidence)
	if err != nil {
		return err
	}
	dependencies := p.Dependencies{Runtime: runtime.Operations(), Evidence: inbox}
	records := &ledger{inbox: inbox, expected: make(map[uint64]*adapters.Receipt[p.Result]), received: make(map[uint64]bool)}
	constructed := make(chan source, 16)
	released := make(chan *p.Owner, 16)
	refused := errors.New("candidate refused after acquisition")
	var ownersMu sync.Mutex
	var owners []*p.Owner
	bind := func(name string, mode resource.Policy) (resource.Ref[p.Handle], error) {
		return resource.Bind(runtime.Resources(), resource.Binding[p.Settings, p.Handle]{
			Name: name, Policy: mode,
			Select: func(view settings.View) (p.Settings, error) {
				snapshot, err := settings.As[p.Settings](view)
				if err != nil {
					return p.Settings{}, err
				}
				return snapshot.ValueCopy()
			},
			Clone: clone, Equal: func(left, right p.Settings) bool { return reflect.DeepEqual(left, right) },
			Build: func(lifetime context.Context, value p.Settings) (*resource.Instance[p.Handle], error) {
				deps := dependencies
				if name == "follow" && value.Name == "first" {
					deps.Native.CheckRedirect = func(request *nativehttp.Request, history []*nativehttp.Request) error {
						if request.Body != nil || request.Response != nil {
							return errors.New("veto received owning state")
						}
						for _, previous := range history {
							if previous.Body != nil || previous.Response != nil {
								return errors.New("veto history retained owning state")
							}
						}
						close(callbackEntered)
						<-callbackGate
						return lateCause
					}
				}
				owner, err := p.Open(lifetime, value, deps)
				if owner == nil {
					return nil, err
				}
				ownersMu.Lock()
				owners = append(owners, owner)
				ownersMu.Unlock()
				constructed <- source{name, value.Name, owner}
				if value.Name == "obsolete" {
					<-obsoleteGate
				}
				var once sync.Once
				instance := &resource.Instance[p.Handle]{Value: owner.Handle(), Release: func(wait context.Context) resource.ReleaseResult {
					result := owner.Release(wait)
					if result.Complete {
						once.Do(func() { released <- owner })
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
	begin := func(value p.Settings) (*resource.Update, error) {
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		prepared, err := configsource.Prepare(ctx, configsource.Schema[p.Settings]{Version: 1, Defaults: first, Validate: func(_ context.Context, value p.Settings) error { return p.Validate(value) }},
			[]configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: raw}})
		if err != nil {
			return nil, err
		}
		snapshot, err := prepared.Snapshot()
		if err != nil {
			return nil, err
		}
		return runtime.Resources().Apply(ctx, snapshot.View())
	}
	apply := func(value p.Settings) error {
		update, err := begin(value)
		if err != nil {
			return err
		}
		return update.Wait(ctx)
	}
	next := func() (source, error) {
		select {
		case entry := <-constructed:
			return entry, nil
		case <-ctx.Done():
			return source{}, ctx.Err()
		}
	}
	awaitRelease := func(wanted *p.Owner) error {
		for {
			select {
			case owner := <-released:
				if owner == wanted {
					if !owner.ShutdownComplete() {
						return errors.New("release notification preceded native cleanup")
					}
					return nil
				}
			case <-ctx.Done():
				return errors.New("retired candidate did not release")
			}
		}
	}
	if err := apply(first); err != nil {
		return err
	}
	var old, fixedSource source
	for range 2 {
		entry, err := next()
		if err != nil {
			return err
		}
		if entry.binding == "follow" {
			old = entry
		} else {
			fixedSource = entry
		}
	}
	if old.owner == nil || fixedSource.owner == nil {
		return errors.New("initial owners missing")
	}
	fixed, err := p.Using(ctx, fixedRef, policy.Budget, dependencies)
	if err != nil {
		return err
	}
	follow, err := p.Using(ctx, followRef, policy.Budget, dependencies)
	if err != nil {
		return err
	}
	request := func(path string) *nativehttp.Request {
		input, err := nativehttp.NewRequest("GET", peer.URL+path, nil)
		if err != nil {
			panic(err)
		}
		return input
	}
	stream, streamReceipt, err := follow.Open(ctx, request("/held"))
	if err != nil {
		return err
	}
	if err := records.expect(streamReceipt); err != nil {
		return err
	}
	prefix := make([]byte, 6)
	if _, err := io.ReadFull(stream, prefix); err != nil || string(prefix) != "prefix" {
		return errors.New("old response stream not entered")
	}
	callbackCtx, cancelCallback := context.WithCancel(ctx)
	defer cancelCallback()
	type callResult struct {
		receipt *adapters.Receipt[p.Result]
		err     error
	}
	callbackCall := make(chan callResult, 1)
	go func() {
		receipt, err := follow.Do(callbackCtx, ctx, request("/redirect"), p.RequestOptions{FollowRedirects: true})
		callbackCall <- callResult{receipt, err}
	}()
	select {
	case <-callbackEntered:
	case <-ctx.Done():
		return errors.New("native callback was not entered")
	}
	second := clone(first)
	second.Name = "second"
	if err := apply(second); err != nil {
		return err
	}
	current, err := next()
	if err != nil || current.binding != "follow" || current.name != "second" {
		return errors.New("Follow did not adopt exact second candidate")
	}
	cancelCallback()
	var callback callResult
	select {
	case callback = <-callbackCall:
	case <-ctx.Done():
		return errors.New("canceled waiter did not return")
	}
	if callback.receipt == nil || callback.err == nil {
		return errors.New("canceled callback waiter lost accepted work")
	}
	if err := records.expect(callback.receipt); err != nil {
		return err
	}
	snapshot, _ := callback.receipt.Snapshot()
	if snapshot.Info().Released || old.owner.ShutdownComplete() {
		return errors.New("late callback or old stream released original source")
	}
	receipt, err := follow.Do(ctx, ctx, request("/finite"))
	fresh, err := records.result(ctx, receipt, err)
	if err != nil {
		return err
	}
	receipt, err = fixed.Do(ctx, ctx, request("/finite"))
	stable, err := records.result(ctx, receipt, err)
	if err != nil {
		return err
	}
	if fresh.Source().Name != "second" || stable.Source().Name != "first" || fresh.Attribution().Source == stable.Attribution().Source {
		return errors.New("Fixed/Follow lineage normalized")
	}
	releaseStream()
	tail, err := io.ReadAll(stream)
	if err != nil || string(tail) != "tail" {
		return errors.New("old response could not finish after replacement")
	}
	if err := stream.Close(ctx); err != nil {
		return err
	}
	if old.owner.ShutdownComplete() {
		return errors.New("entered callback was not retaining retired source")
	}
	releaseCallback()
	callbackFinal, err := callback.receipt.WaitReleased(ctx)
	if err != nil || !errors.Is(callbackFinal.Err(), lateCause) {
		return errors.New("late callback cause or actual release lost")
	}
	result, _ := callbackFinal.ValueCopy()
	if result.Source().Name != "first" {
		return errors.New("late callback migrated generations")
	}
	oldResult, err := records.result(ctx, streamReceipt, nil)
	if err != nil || oldResult.Source().Name != "first" {
		return errors.New("old stream lineage changed")
	}
	if err := awaitRelease(old.owner); err != nil {
		return err
	}
	failed := clone(first)
	failed.Name = "refused"
	if err := apply(failed); !errors.Is(err, refused) {
		return errors.New("failed constructed source was adopted")
	}
	rejected, err := next()
	if err != nil {
		return err
	}
	if err := awaitRelease(rejected.owner); err != nil {
		return err
	}
	receipt, err = follow.Do(ctx, ctx, request("/finite"))
	stillCurrent, err := records.result(ctx, receipt, err)
	if err != nil || stillCurrent.Source().Name != "second" {
		return errors.New("failed candidate replaced working source")
	}
	obsolete := clone(first)
	obsolete.Name = "obsolete"
	update, err := begin(obsolete)
	if err != nil {
		return err
	}
	pending, err := next()
	if err != nil {
		return err
	}
	third := clone(first)
	third.Name = "third"
	newest, err := begin(third)
	if err != nil {
		return err
	}
	releaseObsolete()
	if err := update.Wait(ctx); !errors.Is(err, resource.ErrSuperseded) {
		return fmt.Errorf("obsolete candidate was not superseded: %w", err)
	}
	if err := newest.Wait(ctx); err != nil {
		return err
	}
	latest, err := next()
	if err != nil || latest.name != "third" {
		return errors.New("latest accepted source missing")
	}
	if err := awaitRelease(pending.owner); err != nil {
		return err
	}
	receipt, err = follow.Do(ctx, ctx, request("/finite"))
	final, err := records.result(ctx, receipt, err)
	if err != nil || final.Source().Name != "third" {
		return errors.New("obsolete candidate was used")
	}
	if err := runtime.Resources().Close(ctx); err != nil {
		return err
	}
	if err := records.drain(ctx); err != nil {
		return err
	}
	ownersMu.Lock()
	for _, owner := range owners {
		if !owner.ShutdownComplete() {
			ownersMu.Unlock()
			return errors.New("constructed owner retained cleanup")
		}
	}
	ownersMu.Unlock()
	if err := runtime.Close(ctx); err != nil {
		return err
	}
	state, err := runtime.Operations().Inspect()
	if err != nil || state.Active != 0 || state.WorkBytes != 0 {
		return errors.New("Framework retained operations")
	}
	for activeSockets.Load() != 0 {
		select {
		case <-ctx.Done():
			return errors.New("actual peer sockets not released")
		case <-time.After(time.Millisecond):
		}
	}
	if effects.Load() < 5 {
		return errors.New("no independent peer activity")
	}
	return nil
}
