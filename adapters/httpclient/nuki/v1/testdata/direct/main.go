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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	nuki "github.com/frost-leo/fathomry/adapters/httpclient/nuki/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
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
	fmt.Println("nuki direct public consumer passed")
}

func run(ctx context.Context) error {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "7")
		w.Header().Set("X-Copied", "original")
		w.WriteHeader(http.StatusTeapot)
		_, _ = io.WriteString(w, "payload")
	}))
	defer peer.Close()
	prepared, err := nuki.Prepare(configuration("direct"), nativeOptions())
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
	inbox, err := adapters.NewInbox[nuki.Result](policy.Evidence)
	if err != nil {
		return err
	}
	owner, err := prepared.Open(ctx, nuki.Dependencies{Runtime: runtime, Evidence: inbox})
	if owner != nil {
		defer cleanup(owner.Close)
	}
	if err != nil {
		return err
	}
	client, err := owner.Client().WithID("direct-request")
	if err != nil {
		return err
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	receipt, err := client.Do(ctx, request(peer.URL).WithContext(canceled))
	if err != nil {
		return err
	}
	value, err := received(ctx, inbox, receipt)
	if err != nil || !value.Complete() || value.Metadata().StatusCode() != http.StatusTeapot || string(value.DataCopy()) != "payload" {
		return errors.Join(err, errors.New("finite/method-context contract failed"))
	}
	data := value.DataCopy()
	data[0] = 'x'
	if string(value.DataCopy()) != "payload" {
		return errors.New("finite result aliases caller copy")
	}
	for _, complete := range []bool{false, true} {
		var retained *nuki.Response
		receipt, err := client.Consume(ctx, request(peer.URL), func(ctx context.Context, response *nuki.Response) error {
			retained = response
			if complete {
				_, err := io.ReadAll(response)
				return err
			}
			_, err := response.Read(make([]byte, 1))
			return err
		})
		if err != nil {
			return err
		}
		value, err := received(ctx, inbox, receipt)
		if err != nil || value.Complete() != complete || value.DataCopy() != nil {
			return errors.Join(err, errors.New("callback scope completion changed"))
		}
		if _, err := retained.Read(make([]byte, 1)); !errors.Is(err, nuki.ErrState) {
			return errors.New("callback reader did not expire")
		}
		copy := retained.Metadata().HeadersCopy()
		copy.Set("X-Copied", "changed")
		if value.Metadata().HeadersCopy().Get("X-Copied") != "original" {
			return errors.New("metadata copy mutated result")
		}
	}
	profile, err := client.Profile(ctx)
	if err != nil || profile.SDKMode != "http1" {
		return errors.Join(err, errors.New("source diagnostics unavailable"))
	}
	build, err := nuki.Build()
	if err != nil || len(build.SDKs) < 6 {
		return errors.Join(err, errors.New("native graph diagnostics omitted"))
	}
	if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
		return errors.Join(err, errors.New("source release unconfirmed"))
	}
	if err := drain(ctx, inbox); err != nil {
		return err
	}
	status, err := runtime.Inspect()
	if err != nil || status.Active != 0 || status.WorkBytes != 0 {
		return errors.Join(err, errors.New("public ownership retained"))
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
