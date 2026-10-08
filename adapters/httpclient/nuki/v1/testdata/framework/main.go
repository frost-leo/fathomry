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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	nuki "github.com/frost-leo/fathomry/adapters/httpclient/nuki/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	nativehttp "github.com/nukilabs/http"
	"github.com/nukilabs/tlsclient/profiles"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("nuki framework public consumer passed")
}

type socketTracker struct{ opened, closed atomic.Int64 }
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
func (tracker *socketTracker) dial(ctx context.Context, network, address string) (net.Conn, error) {
	connection, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	tracker.opened.Add(1)
	return &trackedConnection{Conn: connection, tracker: tracker}, nil
}

type constructed struct {
	binding, name string
	owner         *nuki.Owner
	tracker       *socketTracker
}

func clone(value nuki.Settings) nuki.Settings {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	var result nuki.Settings
	if err := json.Unmarshal(raw, &result); err != nil {
		panic(err)
	}
	return result
}
func wait(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func run(ctx context.Context) error {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.Header.Get("X-Source"))
	}))
	defer peer.Close()
	first := configuration("first")
	prepared, err := nuki.Prepare(first, nativeOptions())
	if err != nil {
		return err
	}
	policy, err := nuki.Compose(prepared, prepared, prepared, prepared, prepared, prepared)
	if err != nil {
		return err
	}
	runtime, err := framework.New(ctx, framework.Options{Operations: policy.Runtime})
	if err != nil {
		return err
	}
	defer cleanup(runtime.Close)
	inbox, err := adapters.NewInbox[nuki.Result](policy.Evidence)
	if err != nil {
		return err
	}
	dependencies := nuki.Dependencies{Runtime: runtime.Operations(), Evidence: inbox}
	built := make(chan constructed, 16)
	released := make(chan *nuki.Owner, 16)
	obsoleteGate := make(chan struct{})
	unblockObsolete := sync.OnceFunc(func() { close(obsoleteGate) })
	defer unblockObsolete()
	refused := errors.New("fixture candidate refused")
	bind := func(name string, mode resource.Policy) (resource.Ref[nuki.Handle], error) {
		return resource.Bind(runtime.Resources(), resource.Binding[nuki.Settings, nuki.Handle]{
			Name: name, Policy: mode, Clone: clone, Equal: func(left, right nuki.Settings) bool { return reflect.DeepEqual(left, right) },
			Select: func(view settings.View) (nuki.Settings, error) {
				snapshot, err := settings.As[nuki.Settings](view)
				if err != nil {
					return nuki.Settings{}, err
				}
				return snapshot.ValueCopy()
			},
			Build: func(ctx context.Context, value nuki.Settings) (*resource.Instance[nuki.Handle], error) {
				tracker := &socketTracker{}
				deps := dependencies
				deps.Native = nativeOptions()
				deps.Native.DialContext = tracker.dial
				deps.Native.Before = []func(context.Context, *nativehttp.Request) error{func(_ context.Context, request *nativehttp.Request) error {
					request.Header.Set("X-Source", value.Name)
					return nil
				}}
				owner, err := nuki.Open(ctx, value, deps)
				if owner == nil {
					return nil, err
				}
				built <- constructed{name, value.Name, owner, tracker}
				if value.Name == "obsolete" {
					<-obsoleteGate
				}
				var once sync.Once
				instance := &resource.Instance[nuki.Handle]{Value: owner.Handle(), Release: func(ctx context.Context) resource.ReleaseResult {
					result := owner.Release(ctx)
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
	apply := func(value nuki.Settings) (*resource.Update, error) {
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		prepared, err := configsource.Prepare(ctx, configsource.Schema[nuki.Settings]{Version: 1, Defaults: first, Validate: func(_ context.Context, value nuki.Settings) error { return nuki.Validate(value) }},
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
	next := func() (constructed, error) {
		select {
		case value := <-built:
			return value, nil
		case <-ctx.Done():
			return constructed{}, ctx.Err()
		}
	}
	awaitReleased := func(owner *nuki.Owner) error {
		select {
		case actual := <-released:
			if actual != owner || !owner.ShutdownComplete() {
				return errors.New("wrong or premature source release")
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	update, err := apply(first)
	if err != nil {
		return err
	}
	if err := update.Wait(ctx); err != nil {
		return err
	}
	var old, fixedOwner constructed
	for range 2 {
		value, err := next()
		if err != nil {
			return err
		}
		if value.binding == "follow" {
			old = value
		} else {
			fixedOwner = value
		}
	}
	fixed, err := nuki.Using(ctx, fixedRef, policy.Budget, dependencies)
	if err != nil {
		return err
	}
	follow, err := nuki.Using(ctx, followRef, policy.Budget, dependencies)
	if err != nil {
		return err
	}
	beforeFixed, _ := fixedRef.Inspect()
	beforeFollow, _ := followRef.Inspect()
	entered, allow := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(allow) })
	defer unblock()
	var scoped *nuki.Response
	held, err := follow.Consume(ctx, request(peer.URL), func(ctx context.Context, response *nuki.Response) error {
		scoped = response
		close(entered)
		select {
		case <-allow:
			_, err := io.ReadAll(response)
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil {
		return err
	}
	if err := wait(ctx, entered); err != nil {
		return err
	}
	second := configuration("second")
	update, err = apply(second)
	if err != nil {
		return err
	}
	if err := update.Wait(ctx); err != nil {
		return err
	}
	current, err := next()
	if err != nil {
		return err
	}
	afterFixed, _ := fixedRef.Inspect()
	afterFollow, _ := followRef.Inspect()
	if beforeFixed.Generation != afterFixed.Generation || beforeFollow.Generation == afterFollow.Generation || afterFollow.Retiring != 1 || old.owner.ShutdownComplete() {
		return errors.New("Fixed/Follow replacement lost held callback generation")
	}
	for _, entry := range []struct {
		client     *nuki.Client
		name       string
		generation uint64
	}{{fixed, "first", beforeFixed.Generation}, {follow, "second", afterFollow.Generation}} {
		receipt, err := entry.client.Do(ctx, request(peer.URL))
		if err != nil {
			return err
		}
		value, err := received(ctx, inbox, receipt)
		if err != nil || string(value.DataCopy()) != entry.name || value.Attribution().Source.Generation != entry.generation {
			return errors.Join(err, errors.New("source selection or generation attribution changed"))
		}
	}
	unblock()
	value, err := received(ctx, inbox, held)
	if err != nil || !value.Complete() || value.Attribution().Source.Generation != beforeFollow.Generation {
		return errors.Join(err, errors.New("held callback migrated or failed"))
	}
	if _, err := scoped.Read(make([]byte, 1)); !errors.Is(err, nuki.ErrState) {
		return errors.New("old callback retained new-read authority")
	}
	if err := awaitReleased(old.owner); err != nil {
		return err
	}
	if old.tracker.opened.Load() == 0 || old.tracker.opened.Load() != old.tracker.closed.Load() {
		return errors.New("retired physical sockets not released")
	}
	update, err = apply(configuration("refused"))
	if err != nil {
		return err
	}
	if err := update.Wait(ctx); !errors.Is(err, refused) {
		return errors.New("failed candidate was adopted")
	}
	failed, err := next()
	if err != nil {
		return err
	}
	if err := awaitReleased(failed.owner); err != nil {
		return err
	}
	status, _ := followRef.Inspect()
	if status.Generation != afterFollow.Generation {
		return errors.New("failed candidate replaced working source")
	}
	obsolete, err := apply(configuration("obsolete"))
	if err != nil {
		return err
	}
	abandoned, err := next()
	if err != nil {
		return err
	}
	latestUpdate, err := apply(configuration("latest"))
	if err != nil {
		return err
	}
	unblockObsolete()
	if err := obsolete.Wait(ctx); !errors.Is(err, resource.ErrSuperseded) {
		return errors.New("obsolete candidate was accepted")
	}
	if err := latestUpdate.Wait(ctx); err != nil {
		return err
	}
	latest, err := next()
	if err != nil {
		return err
	}
	// Both obsolete construction and the replaced current source are owned.
	for range 2 {
		select {
		case owner := <-released:
			if owner != abandoned.owner && owner != current.owner {
				return errors.New("unexpected retiring owner")
			}
			if !owner.ShutdownComplete() {
				return errors.New("unconfirmed retirement")
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	receipt, err := follow.Do(ctx, request(peer.URL))
	if err != nil {
		return err
	}
	value, err = received(ctx, inbox, receipt)
	if err != nil || string(value.DataCopy()) != "latest" {
		return errors.Join(err, errors.New("latest generation not usable"))
	}
	if err := runtime.Close(ctx); err != nil {
		return err
	}
	if !fixedOwner.owner.ShutdownComplete() || !latest.owner.ShutdownComplete() {
		return errors.New("Framework close lost ownership")
	}
	if err := drain(ctx, inbox); err != nil {
		return err
	}
	operations, err := runtime.Operations().Inspect()
	if err != nil || operations.Active != 0 || operations.WorkBytes != 0 {
		return errors.Join(err, errors.New("Framework operations remain live"))
	}
	return nil
}

func pointer[T any](value T) *T { return &value }
func configuration(name string) nuki.Settings {
	return nuki.Settings{Name: name, Mode: pointer(nuki.HTTP1Only), MaxActive: pointer(2), MaxRoutes: pointer(2),
		MaxConnections: pointer(4), MaxOrigins: pointer(4), MaxProxyTunnels: pointer(2),
		MaxRequestBytes: pointer[int64](64 << 10), MaxResponseBytes: pointer[int64](64 << 10), MaxEncodedBytes: pointer[int64](64 << 10)}
}
func nativeOptions() nuki.NativeOptions {
	profile := profiles.Chrome150
	return nuki.NativeOptions{Profile: &profile}
}
func request(address string) *nativehttp.Request {
	value, err := nativehttp.NewRequest("GET", address, nil)
	if err != nil {
		panic(err)
	}
	return value
}
func cleanup(close func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = close(ctx)
}
func received(ctx context.Context, inbox *adapters.Inbox[nuki.Result], receipt *adapters.Receipt[nuki.Result]) (nuki.Result, error) {
	if receipt == nil {
		return nuki.Result{}, errors.New("missing accepted receipt")
	}
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil {
		return nuki.Result{}, err
	}
	var delivery adapters.Delivery[nuki.Result]
	var other adapters.Snapshot[nuki.Result]
	for {
		delivery, err = inbox.NextReleased(ctx)
		if err != nil {
			return nuki.Result{}, err
		}
		independent, err := delivery.Receipt()
		if err != nil {
			return nuki.Result{}, err
		}
		other, err = independent.WaitReleased(ctx)
		if err != nil {
			return nuki.Result{}, err
		}
		if other.Info().Sequence == snapshot.Info().Sequence {
			break
		}
		if other.Info().Operation != "httpclient.nuki.open" {
			return nuki.Result{}, errors.New("unexpected independent operation")
		}
		if err := delivery.Ack(); err != nil {
			return nuki.Result{}, err
		}
	}
	if other.Info() != snapshot.Info() {
		return nuki.Result{}, errors.New("independent attribution differs")
	}
	value, present := snapshot.ValueCopy()
	copy, copied := other.ValueCopy()
	if present != copied || value.Attribution() != copy.Attribution() || string(value.DataCopy()) != string(copy.DataCopy()) {
		return nuki.Result{}, errors.New("independent result differs")
	}
	if err := delivery.Retry(); err != nil {
		return nuki.Result{}, err
	}
	for {
		delivery, err = inbox.NextReleased(ctx)
		if err != nil {
			return nuki.Result{}, err
		}
		again, err := delivery.Receipt()
		if err != nil {
			return nuki.Result{}, err
		}
		retried, _ := again.Snapshot()
		if retried.Info().Sequence == snapshot.Info().Sequence {
			if retried.Info() != snapshot.Info() {
				return nuki.Result{}, errors.New("evidence retry changed operation")
			}
			if err := delivery.Ack(); err != nil {
				return nuki.Result{}, err
			}
			break
		}
		if retried.Info().Operation != "httpclient.nuki.open" {
			return nuki.Result{}, errors.New("unexpected evidence during retry")
		}
		if err := delivery.Ack(); err != nil {
			return nuki.Result{}, err
		}
	}
	return value, snapshot.Err()
}
func drain(ctx context.Context, inbox *adapters.Inbox[nuki.Result]) error {
	for {
		status, err := inbox.Inspect()
		if err != nil {
			return err
		}
		if status.Outstanding == 0 {
			return nil
		}
		delivery, err := inbox.NextReleased(ctx)
		if err != nil {
			return err
		}
		if err := delivery.Ack(); err != nil {
			return err
		}
	}
}
