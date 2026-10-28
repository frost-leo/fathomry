/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package consumer

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	stdmail "net/mail"
	"reflect"
	"strings"
	"sync"
	"testing"

	mail "github.com/frost-leo/fathomry/adapters/notification/mail/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func TestFrameworkFixedFollowRetainsOriginalWireAndEvidence(t *testing.T) {
	ctx, cancel := waitContext(t)
	defer cancel()
	first, second := newPeer(t, "first"), newPeer(t, "second")
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	firstSigner, err := mail.NewDKIM(mail.DKIMOptions{Domain: "example.invalid", Selector: "first"}, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	secondSigner, err := mail.NewDKIM(mail.DKIMOptions{Domain: "example.invalid", Selector: "second"}, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	prepareA, err := mail.Prepare(first.config, mail.NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	prepareB, err := mail.Prepare(second.config, mail.NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := mail.Compose(prepareA, prepareA, prepareB, prepareB)
	if err != nil {
		t.Fatal(err)
	}
	compositionContext, stopComposition := context.WithCancel(ctx)
	defer stopComposition()
	runtime, err := adapters.New(compositionContext, policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	resourceScope, err := resource.New(compositionContext, resource.Options{})
	if err != nil {
		stopComposition()
		err = errors.Join(err, runtime.Close(context.Background()))
		t.Fatal(err)
	}
	closeComposition := func(cleanup context.Context) error {
		stopComposition()
		return errors.Join(runtime.Close(cleanup), resourceScope.Close(cleanup))
	}
	defer closeComposition(context.Background())
	inbox, err := adapters.NewInbox[mail.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	dependencies := mail.Dependencies{Runtime: runtime, Evidence: inbox}
	var mutex sync.Mutex
	seen := map[uint64]int{}
	received := make(chan adapters.Snapshot[mail.Result], 32)
	finishEvidence, stopEvidence := drainEvidence(ctx, inbox, func(_ context.Context, snapshot adapters.Snapshot[mail.Result]) error {
		mutex.Lock()
		seen[snapshot.Info().Sequence]++
		attempt := seen[snapshot.Info().Sequence]
		mutex.Unlock()
		if snapshot.Info().Operation == "notification.mail.send" && attempt == 1 {
			return errors.New("synthetic sink failure")
		}
		received <- snapshot
		return nil
	})
	defer stopEvidence(context.Background())
	clone := func(value mail.Settings) mail.Settings { return value }
	refused := errors.New("partial construction control")
	bind := func(name string, mode resource.Policy) resource.Ref[mail.Handle] {
		ref, err := resource.Bind(resourceScope, resource.Binding[mail.Settings, mail.Handle]{Name: name, Policy: mode,
			Select: func(view settings.View) (mail.Settings, error) {
				snapshot, err := settings.As[mail.Settings](view)
				if err != nil {
					return mail.Settings{}, err
				}
				return snapshot.ValueCopy()
			},
			Clone: clone, Equal: func(left, right mail.Settings) bool { return reflect.DeepEqual(left, right) },
			Build: func(lifetime context.Context, value mail.Settings) (*resource.Instance[mail.Handle], error) {
				declared := dependencies
				declared.Native.AlwaysDKIM = firstSigner
				if value.Name != "first" {
					declared.Native.AlwaysDKIM = secondSigner
				}
				owner, err := mail.Open(lifetime, value, declared)
				if owner == nil {
					return nil, err
				}
				instance := &resource.Instance[mail.Handle]{Value: owner.Handle(), Release: owner.Release}
				if value.Name == "partial" {
					return instance, errors.Join(err, refused)
				}
				return instance, err
			}})
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	fixed, follow := bind("fixed", resource.Fixed), bind("follow", resource.Follow)
	apply := func(value mail.Settings) error {
		snapshot, err := settings.New(value, clone)
		if err != nil {
			return err
		}
		store := settings.NewStore[mail.Settings]()
		if err = store.Publish(snapshot); err != nil {
			return err
		}
		view, err := store.Reader().Capture()
		if err != nil {
			return err
		}
		update, err := resourceScope.Apply(ctx, view)
		if err != nil {
			return err
		}
		return update.Wait(ctx)
	}
	if err = apply(first.config); err != nil {
		t.Fatal(err)
	}
	fixedClient, err := mail.Using(ctx, fixed, policy.Budget, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	followClient, err := mail.Using(ctx, follow, policy.Budget, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	held := make(chan struct{})
	first.mu.Lock()
	first.hold = held
	first.mu.Unlock()
	type completion struct {
		value mail.Result
		err   error
	}
	finished := make(chan completion, 1)
	go func() {
		result, err := followClient.Send(ctx, makeMessage(t, "held"))
		finished <- completion{result, err}
	}()
	select {
	case <-first.reached:
	case <-ctx.Done():
		t.Fatal("first generation did not reach DATA")
	}
	if err = apply(second.config); err != nil {
		close(held)
		t.Fatal(err)
	}
	newer, err := followClient.Send(ctx, makeMessage(t, "new"))
	if err != nil {
		close(held)
		t.Fatal(err)
	}
	stable, err := fixedClient.Send(ctx, makeMessage(t, "fixed"))
	if err != nil {
		close(held)
		t.Fatal(err)
	}
	close(held)
	older := <-finished
	if older.err != nil {
		t.Fatal(older.err)
	}
	if older.value.Source().Name != "first" || newer.Source().Name != "second" || stable.Source().Name != "first" || older.value.Attribution().Source.Generation == newer.Attribution().Source.Generation {
		t.Fatal("generation attribution migrated")
	}
	a, _ := first.snapshot()
	b, _ := second.snapshot()
	if len(a) != 2 || len(b) != 1 {
		t.Fatalf("wire retargeted/resend %d/%d", len(a), len(b))
	}
	for _, group := range []struct {
		captures []capture
		selector string
	}{{a, "first"}, {b, "second"}} {
		for _, captured := range group.captures {
			parsed, err := stdmail.ReadMessage(bytes.NewReader(captured.mime))
			if err != nil || !strings.Contains(parsed.Header.Get("DKIM-Signature"), "s="+group.selector+";") {
				t.Fatal("source signing migrated across generation", err)
			}
		}
	}
	partial := second.config
	partial.Name = "partial"
	if err = apply(partial); !errors.Is(err, refused) {
		t.Fatal("partial construction cause lost", err)
	}
	if err = closeComposition(ctx); err != nil {
		t.Fatal(err)
	}
	if err = finishEvidence(ctx); err != nil {
		t.Fatal(err)
	}
	a, _ = first.snapshot()
	b, _ = second.snapshot()
	if len(a) != 2 || len(b) != 1 {
		t.Fatal("sink redelivery repeated native work")
	}
	mutex.Lock()
	defer mutex.Unlock()
	for _, result := range []mail.Result{older.value, newer, stable} {
		if seen[result.Attribution().Sequence] != 2 {
			t.Fatal("required evidence was not independently redelivered")
		}
	}
}

func drainEvidence(ctx context.Context, inbox *adapters.Inbox[mail.Result], sink func(context.Context, adapters.Snapshot[mail.Result]) error) (finish, stop func(context.Context) error) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	var result error
	go func() {
		defer close(done)
		defer cancel()
		for {
			delivery, err := inbox.NextReleased(ctx)
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				return
			}
			if err != nil {
				result = err
				return
			}
			receipt, err := delivery.Receipt()
			if err != nil {
				result = errors.Join(err, delivery.Retry())
				return
			}
			snapshot, _ := receipt.Snapshot()
			if err := sink(ctx, snapshot); err != nil {
				if retry := delivery.Retry(); retry != nil {
					result = errors.Join(err, retry)
					return
				}
				continue
			}
			if err := delivery.Ack(); err != nil {
				result = err
				return
			}
		}
	}()
	wait := func(ctx context.Context) error {
		select {
		case <-done:
			return result
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	finish = func(ctx context.Context) error {
		if err := inbox.Seal(); err != nil {
			return err
		}
		if err := wait(ctx); err != nil {
			return err
		}
		status, err := inbox.Inspect()
		if err != nil {
			return err
		}
		if status.Outstanding != 0 {
			return errors.New("undelivered fixture evidence")
		}
		return nil
	}
	stop = func(ctx context.Context) error { cancel(); return wait(ctx) }
	return
}
