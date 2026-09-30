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

package nacos

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestManagement(t *testing.T) {
	fixture := newService(t)
	owner, _, inbox := openService(t, fixture, fixture.settings())
	client := owner.Client()
	lifetime, err := inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Run("publish metadata cas refusal and delete", func(t *testing.T) {
		input := PublishInput{Key: Key{DataID: "main"}, Content: "value: published\n", ContentType: "yaml", CASMD5: checksum("value: initial\n"), Tag: "tag", ConfigTags: "one,two", AppName: "application", SourceUser: "source", BetaIPs: "127.0.0.1"}
		result, err := client.Publish(context.Background(), input)
		if err != nil || result.State() != MutationAcknowledged {
			t.Fatal(result.State(), err)
		}
		fixture.mu.Lock()
		published := fixture.published
		fixture.mu.Unlock()
		if published.AdditionMap["type"] != input.ContentType || published.AdditionMap["tag"] != input.Tag || published.AdditionMap["config_tags"] != input.ConfigTags || published.AdditionMap["appName"] != input.AppName || published.AdditionMap["src_user"] != input.SourceUser || published.AdditionMap["betaIps"] != input.BetaIPs {
			t.Fatal("native publication metadata lost")
		}
		result, err = client.Publish(context.Background(), input)
		remote, ok := InspectError(err)
		if result.State() != MutationRejected || !errors.Is(err, ErrWrite) || !ok || remote.ErrorCode() != 409 || remote.Message() != "cas-private-canary" {
			t.Fatal("CAS evidence lost", result.State(), err)
		}
		count := fixture.mutations.Load()
		received := 0
		for range 2 {
			delivery, err := inbox.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := delivery.Receipt()
			if err != nil {
				t.Fatal(err)
			}
			facts, err := receipt.WaitReleased(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			evidence, ok := facts.ValueCopy()
			if !ok || !evidence.MutationPresent {
				t.Fatal("mutation evidence absent")
			}
			received++
			if received == 2 {
				if evidence.Mutation.State() != MutationRejected || !errors.Is(facts.Primary(), ErrWrite) {
					t.Fatal("refusal facts lost")
				}
				if err := delivery.Retry(); err != nil {
					t.Fatal(err)
				}
			} else if err := delivery.Ack(); err != nil {
				t.Fatal(err)
			}
		}
		failure := errors.New("receiver unavailable")
		if err := inbox.DeliverOne(context.Background(), func(context.Context, adapters.Snapshot[Evidence]) error { return failure }); !errors.Is(err, failure) {
			t.Fatal(err)
		}
		if err := inbox.DeliverOne(context.Background(), func(context.Context, adapters.Snapshot[Evidence]) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if fixture.mutations.Load() != count {
			t.Fatal("redelivery repeated mutation")
		}
		result, err = client.Delete(context.Background(), Key{DataID: "main"})
		if err != nil || result.State() != MutationAcknowledged {
			t.Fatal(err)
		}
		document, err := client.ReadRaw(context.Background(), Key{DataID: "main"})
		if err != nil || !document.Missing() {
			t.Fatal("delete effect absent", err)
		}
	})
	t.Run("post-dispatch cancellation is unknown", func(t *testing.T) {
		fixture.mu.Lock()
		fixture.blocked = true
		fixture.mu.Unlock()
		ctx, cancel := context.WithCancel(context.Background())
		completed := make(chan struct{})
		var result MutationResult
		var resultErr error
		go func() {
			defer close(completed)
			result, resultErr = client.Publish(ctx, PublishInput{Key: Key{DataID: "main"}, Content: "new"})
		}()
		select {
		case <-fixture.entered:
		case <-time.After(2 * time.Second):
			cancel()
			t.Fatal("no native dispatch")
		}
		cancel()
		<-completed
		if result.State() != MutationUnknown || resultErr == nil {
			t.Fatal("timeout fabricated rollback", result.State(), resultErr)
		}
		fixture.mu.Lock()
		fixture.blocked = false
		fixture.mu.Unlock()
	})
	t.Run("local refusal before dispatch", func(t *testing.T) {
		count := fixture.mutations.Load()
		for _, input := range []PublishInput{{Key: Key{DataID: "main"}}, {Key: Key{DataID: "main"}, Content: "valid", CASMD5: "bad"}, {Key: Key{DataID: "main"}, Content: strings.Repeat("x", MaxDocumentBytes+1)}} {
			result, err := client.Publish(context.Background(), input)
			if result.State() != MutationNotIssued || !errors.Is(err, ErrInput) {
				t.Fatal(result.State(), err)
			}
		}
		if fixture.mutations.Load() != count {
			t.Fatal("refused mutation dispatched")
		}
		selected := fixture.settings()
		selected.Writable = false
		readonly, _, _ := openService(t, fixture, selected)
		if result, err := readonly.Client().Delete(context.Background(), Key{DataID: "main"}); result.State() != MutationNotIssued || !errors.Is(err, ErrInput) {
			t.Fatal(result.State(), err)
		}
	})
	if err := owner.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := lifetime.Ack(); err != nil {
		t.Fatal(err)
	}
}
func TestSearch(t *testing.T) {
	fixture := newService(t)
	owner, _, _ := openService(t, fixture, fixture.settings())
	client := owner.Client()
	input := SearchInput{Mode: "accurate", DataID: "main", Group: "DEFAULT_GROUP", ConfigTags: "tag", AppName: "app"}
	for _, version3 := range []bool{false, true} {
		fixture.mu.Lock()
		fixture.version3 = version3
		fixture.mu.Unlock()
		page, err := client.Search(context.Background(), input)
		if err != nil {
			t.Fatal(err)
		}
		items := page.ItemsCopy()
		if page.Total() != 1 || page.Number() != 1 || page.Pages() != 1 || len(items) != 1 {
			t.Fatal("page metadata lost")
		}
		item := items[0]
		if item.ID() != "1" || item.Key() != (Key{Group: "DEFAULT_GROUP", DataID: "main"}) || item.Namespace() != "" || item.MD5() != checksum("value: initial\n") || item.AppName() != "fixture" || item.ContentPresent() == version3 {
			t.Fatal("item metadata lost")
		}
		if version3 && item.RawCopy() != nil || !version3 && string(item.RawCopy()) != "value: initial\n" {
			t.Fatal("content absence became empty success")
		}
		items[0] = SearchItem{}
		if page.ItemsCopy()[0].ID() != "1" {
			t.Fatal("page alias")
		}
	}
	fixture.mu.Lock()
	fixture.denied = true
	fixture.mu.Unlock()
	requests := fixture.searchV3.Load()
	_, err := client.Search(context.Background(), input)
	remote, ok := InspectError(err)
	if !errors.Is(err, ErrDenied) || !ok || remote.HTTPStatus() != 403 || fixture.searchV3.Load() != requests {
		t.Fatal("auth failure fell back or evidence lost", err)
	}
	fixture.mu.Lock()
	fixture.denied = false
	fixture.mu.Unlock()
	for _, input := range []SearchInput{{Mode: "other"}, {Mode: "accurate", Page: -1}, {Mode: "accurate", PageSize: 101}} {
		if _, err := client.Search(context.Background(), input); !errors.Is(err, ErrInput) {
			t.Fatal(err)
		}
	}
}
