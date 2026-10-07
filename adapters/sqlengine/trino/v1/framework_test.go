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

package trino_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	trino "github.com/frost-leo/fathomry/adapters/sqlengine/trino/v1"
	"github.com/frost-leo/fathomry/adapters/sqlengine/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func generationPeer(t testing.TB, label string) *wirePeer {
	t.Helper()
	var peer *wirePeer
	peer = newWirePeer(t, func(writer http.ResponseWriter, request *http.Request, body []byte) {
		if request.Method == http.MethodDelete {
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		page := map[string]any{"id": "generation", "columns": []any{wireColumn("value", "varchar", "varchar", longArgument(2147483647))}, "data": [][]string{{label}}}
		if request.Method == http.MethodPost && string(body) == "SELECT retained" {
			page["nextUri"] = peer.next("generation", 1)
		}
		writePage(t, writer, page)
	})
	return peer
}

func TestFrameworkFixedFollowAndCandidateOwnership(t *testing.T) {
	firstPeer, secondPeer, thirdPeer := generationPeer(t, "first"), generationPeer(t, "second"), generationPeer(t, "third")
	first, second, third := firstPeer.settings(), secondPeer.settings(), thirdPeer.settings()
	first.Name, second.Name, third.Name = "first", "second", "third"
	policy, err := trino.Recommend(first)
	if err != nil {
		t.Fatal(err)
	}
	var family sqlengine.Budget = policy.Budget
	policy.Runtime.MaxActive *= 8
	policy.Runtime.MaxWorkBytes *= 8
	policy.Evidence.Capacity *= 32
	policy.Evidence.MaxBytes *= 32
	runtime, err := framework.New(context.Background(), framework.Options{Operations: policy.Runtime})
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[trino.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	dependencies := trino.Dependencies{Runtime: runtime.Operations(), Evidence: inbox}
	type builtSource struct {
		binding string
		name    string
		owner   *trino.Owner
	}
	opened, released := make(chan builtSource, 16), make(chan builtSource, 16)
	var ownersMu sync.Mutex
	var owners []*trino.Owner
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error("Framework cleanup", err)
		}
		ownersMu.Lock()
		defer ownersMu.Unlock()
		for _, owner := range owners {
			if !owner.ShutdownComplete() {
				t.Error("Framework abandoned native source ownership")
			}
		}
		for state, _ := inbox.Inspect(); state.Outstanding > 0; state, _ = inbox.Inspect() {
			delivery, err := inbox.NextReleased(ctx)
			if err != nil {
				t.Error("Framework evidence cleanup", err)
				break
			}
			if err := delivery.Ack(); err != nil {
				t.Error(err)
				break
			}
		}
	})
	type applicationSettings struct {
		Database trino.Settings `json:"database"`
	}
	bind := func(name string, behavior resource.Policy) resource.Ref[trino.Handle] {
		ref, err := resource.Bind(runtime.Resources(), resource.Binding[trino.Settings, trino.Handle]{
			Name: name, Policy: behavior,
			Select: func(view settings.View) (trino.Settings, error) {
				snapshot, err := settings.As[applicationSettings](view)
				if err != nil {
					return trino.Settings{}, err
				}
				value, err := snapshot.ValueCopy()
				return value.Database, err
			},
			Clone: func(value trino.Settings) trino.Settings { return value },
			Equal: func(left, right trino.Settings) bool { return left == right },
			Build: func(ctx context.Context, value trino.Settings) (*resource.Instance[trino.Handle], error) {
				owner, err := trino.Open(ctx, value, dependencies)
				if owner == nil {
					return nil, err
				}
				built := builtSource{name, value.Name, owner}
				ownersMu.Lock()
				owners = append(owners, owner)
				ownersMu.Unlock()
				opened <- built
				var once sync.Once
				return &resource.Instance[trino.Handle]{Value: owner.Handle(), Release: func(ctx context.Context) resource.ReleaseResult {
					result := owner.Release(ctx)
					if result.Complete {
						once.Do(func() { released <- built })
					}
					return result
				}}, err
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	fixedRef, followRef := bind("fixed", resource.Fixed), bind("follow", resource.Follow)
	apply := func(value trino.Settings) *resource.Update {
		snapshot, err := settings.New(applicationSettings{Database: value}, func(value applicationSettings) applicationSettings { return value })
		if err != nil {
			t.Fatal(err)
		}
		update, err := runtime.Resources().Apply(boundedContext(t), snapshot.View())
		if err != nil {
			t.Fatal(err)
		}
		return update
	}
	construction := func() builtSource {
		select {
		case built := <-opened:
			return built
		case <-boundedContext(t).Done():
			t.Fatal("source construction never returned")
			return builtSource{}
		}
	}
	waitRelease := func(expected *trino.Owner) {
		for {
			select {
			case built := <-released:
				if built.owner == expected {
					if !expected.ShutdownComplete() {
						t.Fatal("release notification preceded native join")
					}
					return
				}
			case <-boundedContext(t).Done():
				t.Fatal("retired source never released")
			}
		}
	}
	if err := apply(first).Wait(boundedContext(t)); err != nil {
		t.Fatal(err)
	}
	var old *trino.Owner
	for range 2 {
		built := construction()
		if built.binding == "follow" {
			old = built.owner
		}
	}
	fixed, err := trino.Using(context.Background(), fixedRef, policy.Budget, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	follow, err := trino.Using(context.Background(), followRef, family, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	fixedStatus, _ := fixedRef.Inspect()
	oldStatus, _ := followRef.Inspect()
	reader, err := follow.Read(boundedContext(t), boundedContext(t), trino.Statement{SQL: "SELECT retained"})
	if err != nil || reader == nil {
		t.Fatal(err)
	}
	firstPage, err := reader.Next(boundedContext(t))
	if err != nil || string(firstPage.DataCopy()) != `[["first"]]` || firstPage.Attribution().Source.Generation != oldStatus.Generation {
		t.Fatal("initial retained read used wrong generation", err)
	}
	acknowledge(t, inbox)
	if status, _ := followRef.Inspect(); status.Borrowers != 1 {
		t.Fatal("one reader failed to pin exactly one original generation")
	}
	if err := apply(second).Wait(boundedContext(t)); err != nil {
		t.Fatal(err)
	}
	if built := construction(); built.binding != "follow" || built.name != "second" {
		t.Fatal("Fixed reconstructed or wrong candidate adopted")
	}
	current, _ := followRef.Inspect()
	if current.Generation == oldStatus.Generation || current.Retiring != 1 || old.ShutdownComplete() {
		t.Fatal("replacement revoked an old reader's source")
	}
	if status, _ := fixedRef.Inspect(); status.Generation != fixedStatus.Generation {
		t.Fatal("Fixed followed a settings replacement")
	}
	query := func(client *trino.Client, name string, generation uint64) {
		value, err := client.Query(boundedContext(t), boundedContext(t), trino.Statement{SQL: "SELECT current"})
		if err != nil || !value.Complete() || value.Source().Name != name || value.Attribution().Source.Generation != generation || string(value.DataCopy()) != `[["`+name+`"]]` {
			t.Fatal("public root retargeted incorrectly", err)
		}
		acknowledge(t, inbox)
	}
	query(fixed, "first", fixedStatus.Generation)
	query(follow, "second", current.Generation)
	beforeSecond := secondPeer.gets.Load()
	oldPage, err := reader.Next(boundedContext(t))
	if err != nil || string(oldPage.DataCopy()) != `[["first"]]` || oldPage.Attribution().Source.Generation != oldStatus.Generation || secondPeer.gets.Load() != beforeSecond {
		t.Fatal("existing reader retargeted to a new source", err)
	}
	acknowledge(t, inbox)
	if _, err := reader.Next(boundedContext(t)); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if result, err := reader.Result(boundedContext(t)); err != nil || !result.Complete() || result.Attribution().Source.Generation != oldStatus.Generation {
		t.Fatal("retained result lost original generation", err)
	}
	waitRelease(old)

	failedPeer := generationPeer(t, "failed")
	failedPeer.onReady = func(writer http.ResponseWriter, _ *http.Request) {
		writePage(t, writer, map[string]any{"id": "notready"})
	}
	failed := failedPeer.settings()
	failed.Name = "failed"
	if err := apply(failed).Wait(boundedContext(t)); err == nil {
		t.Fatal("failed actual native readiness was adopted")
	}
	failedOwner := construction().owner
	waitRelease(failedOwner)
	if status, _ := followRef.Inspect(); status.Generation != current.Generation {
		t.Fatal("failed candidate discarded last good instance")
	}
	query(follow, "second", current.Generation)

	slowPeer := generationPeer(t, "obsolete")
	started, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	slowPeer.onReady = func(writer http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		writePage(t, writer, map[string]any{"id": "ready", "columns": []any{wireColumn("version", "varchar", "varchar", longArgument(2147483647))}, "data": [][]string{{"483"}}})
	}
	obsolete := slowPeer.settings()
	obsolete.Name = "obsolete"
	obsoleteUpdate := apply(obsolete)
	select {
	case <-started:
	case <-boundedContext(t).Done():
		t.Fatal("obsolete candidate did not begin readiness")
	}
	winningUpdate := apply(third)
	unblock()
	if err := obsoleteUpdate.Wait(boundedContext(t)); !errors.Is(err, resource.ErrSuperseded) {
		t.Fatal("obsolete update did not retain superseded identity", err)
	}
	if err := winningUpdate.Wait(boundedContext(t)); err != nil {
		t.Fatal("newer candidate not adopted", err)
	}
	for range 2 {
		built := construction()
		if built.name == "obsolete" {
			waitRelease(built.owner)
		}
	}
	winning, _ := followRef.Inspect()
	query(follow, "third", winning.Generation)
	if slowPeer.posts.Load() != 0 {
		t.Fatal("obsolete readiness candidate won application access")
	}

	larger := third
	larger.Name = "larger"
	larger.MaxPageBytes *= 2
	if err := apply(larger).Wait(boundedContext(t)); err != nil {
		t.Fatal(err)
	}
	construction()
	before := thirdPeer.posts.Load()
	if _, err := follow.Query(boundedContext(t), boundedContext(t), trino.Statement{SQL: "SELECT current"}); !errors.Is(err, trino.ErrLimit) || thirdPeer.posts.Load() != before {
		t.Fatal("larger replacement dispatched with old insufficient budget", err)
	}
}
