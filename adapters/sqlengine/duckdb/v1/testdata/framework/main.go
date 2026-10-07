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
	"os"
	"sync"
	"time"

	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	duckdb "github.com/frost-leo/fathomry/adapters/sqlengine/duckdb/v1"
	"github.com/frost-leo/fathomry/adapters/sqlengine/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("duckdb Framework Fixed/Follow public consumer passed")
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	first := duckdb.Settings{Name: "first", Connections: 2, MemoryBytes: 64 << 20, ReaderChunkRows: 2, ReaderTotalRows: 100, ResultBytes: 1 << 20}
	policy, err := duckdb.Recommend(first)
	if err != nil {
		return err
	}
	var family sqlengine.Budget = policy.Budget
	policy.Runtime.MaxActive *= 6
	policy.Runtime.MaxWorkBytes *= 6
	policy.Evidence.Capacity *= 6
	policy.Evidence.MaxBytes *= 6
	runtime, err := framework.New(ctx, framework.Options{Operations: policy.Runtime})
	if err != nil {
		return err
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[duckdb.Result](policy.Evidence)
	if err != nil {
		return err
	}
	dependencies := duckdb.Dependencies{Runtime: runtime.Operations(), Evidence: inbox}
	type construction struct {
		binding string
		owner   *duckdb.Owner
	}
	opened := make(chan construction, 16)
	released := make(chan *duckdb.Owner, 16)
	refused := errors.New("fixture candidate refused")
	obsoleteGate := make(chan struct{})
	releaseObsolete := sync.OnceFunc(func() { close(obsoleteGate) })
	defer releaseObsolete()
	bind := func(name string, policy resource.Policy) (resource.Ref[duckdb.Handle], error) {
		return resource.Bind(runtime.Resources(), resource.Binding[duckdb.Settings, duckdb.Handle]{
			Name: name, Policy: policy,
			Select: func(view settings.View) (duckdb.Settings, error) {
				snapshot, err := settings.As[duckdb.Settings](view)
				if err != nil {
					return duckdb.Settings{}, err
				}
				return snapshot.ValueCopy()
			},
			Clone: func(value duckdb.Settings) duckdb.Settings { return value },
			Equal: func(left, right duckdb.Settings) bool { return left == right },
			Build: func(ctx context.Context, value duckdb.Settings) (*resource.Instance[duckdb.Handle], error) {
				owner, err := duckdb.Open(ctx, value, dependencies)
				if owner == nil {
					return nil, err
				}
				opened <- construction{name, owner}
				if value.Name == "obsolete" {
					<-obsoleteGate
				}
				var notified sync.Once
				instance := &resource.Instance[duckdb.Handle]{Value: owner.Handle(), Release: func(ctx context.Context) resource.ReleaseResult {
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
	beginApply := func(value duckdb.Settings) (*resource.Update, error) {
		schema, err := duckdb.Configuration(value)
		if err != nil {
			return nil, err
		}
		raw, err := json.Marshal(schema.Defaults)
		if err != nil {
			return nil, err
		}
		prepared, err := configsource.Prepare(ctx, schema,
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
	apply := func(value duckdb.Settings) error {
		update, err := beginApply(value)
		if err != nil {
			return err
		}
		return update.Wait(ctx)
	}
	constructed := func() (construction, error) {
		select {
		case value := <-opened:
			return value, nil
		case <-ctx.Done():
			return construction{}, errors.New("candidate construction missing")
		}
	}
	finalized := func(want *duckdb.Owner) error {
		select {
		case actual := <-released:
			if actual != want || !actual.ShutdownComplete() {
				return errors.New("wrong retired owner finalized")
			}
			return nil
		case <-ctx.Done():
			return errors.New("retired owner was not finalized")
		}
	}
	if err := apply(first); err != nil {
		return err
	}
	var old *duckdb.Owner
	for range 2 {
		value, err := constructed()
		if err != nil {
			return err
		}
		if value.binding == "follow" {
			old = value.owner
		}
	}
	fixed, err := duckdb.Using(ctx, fixedRef, policy.Budget, dependencies)
	if err != nil {
		return err
	}
	follow, err := duckdb.Using(ctx, followRef, family, dependencies)
	if err != nil {
		return err
	}
	fixedStatus, _ := fixedRef.Inspect()
	oldStatus, _ := followRef.Inspect()
	for _, client := range []*duckdb.Client{fixed, follow} {
		if _, err := client.Execute(ctx, ctx, "CREATE TABLE marker (id BIGINT)"); err != nil {
			return err
		}
		if _, err := client.Execute(ctx, ctx, "INSERT INTO marker VALUES (11)"); err != nil {
			return err
		}
	}
	reader, err := follow.Read(ctx, "SELECT 11::BIGINT, range FROM range(5) ORDER BY range")
	if err != nil {
		return err
	}
	type transactionResult struct {
		value duckdb.Result
		err   error
	}
	transactionContext, stopTransaction := context.WithCancel(ctx)
	defer stopTransaction()
	transactionDone := make(chan transactionResult, 1)
	go func() {
		value, err := follow.Transaction(transactionContext, ctx, []duckdb.Request{
			{Mode: duckdb.Append, Table: "marker", Rows: [][]any{{int64(12)}}},
			{Mode: duckdb.Query, SQL: "SELECT sum(a.i*b.i) FROM range(1000000) a(i),range(1000000) b(i)"},
		})
		transactionDone <- transactionResult{value, err}
	}()
	for {
		status, err := followRef.Inspect()
		if err != nil {
			return err
		}
		if status.Borrowers == 2 {
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			return errors.New("transaction and reader did not each pin the source")
		}
	}
	second := first
	second.Name = "second"
	if err := apply(second); err != nil {
		return err
	}
	current, err := constructed()
	if err != nil {
		return err
	}
	currentStatus, _ := followRef.Inspect()
	if current.binding != "follow" || currentStatus.Generation == oldStatus.Generation || currentStatus.Retiring != 1 || old.ShutdownComplete() {
		return errors.New("replacement revoked or lost the retained generation")
	}
	if status, _ := fixedRef.Inspect(); status.Generation != fixedStatus.Generation {
		return errors.New("Fixed source followed a replacement")
	}
	if _, err := follow.Query(ctx, ctx, "SELECT * FROM marker"); err == nil {
		return errors.New("new in-memory source falsely inherited prior database state")
	}
	value, err := follow.Query(ctx, ctx, "SELECT 22::BIGINT")
	if err != nil || value.Source().Name != "second" || value.Attribution().Source.Generation != currentStatus.Generation {
		return errors.New("future Follow root did not use adopted source")
	}
	value, err = fixed.Query(ctx, ctx, "SELECT id FROM marker")
	if err != nil || value.Snapshot().Steps[0].Rows[0][0] != int64(11) || value.Source().Name != "first" || value.Attribution().Source.Generation != fixedStatus.Generation {
		return errors.New("Fixed root changed its native source")
	}
	stopTransaction()
	select {
	case transaction := <-transactionDone:
		if !errors.Is(transaction.err, context.Canceled) || transaction.value.Source().Name != "first" || transaction.value.Attribution().Source.Generation != oldStatus.Generation || !transaction.value.Snapshot().ConnectionClosed {
			return errors.New("retained transaction changed generation or lost cancellation progress")
		}
	case <-ctx.Done():
		return errors.New("transaction cleanup did not finish")
	}
	if old.ShutdownComplete() {
		return errors.New("finishing transaction revoked retained reader")
	}
	value, err = reader.Next(ctx)
	if err != nil || value.Source().Name != "first" || value.Attribution().Source.Generation != oldStatus.Generation || value.Snapshot().Steps[0].Rows[0][0] != int64(11) {
		return errors.New("retained reader retargeted after Follow")
	}
	if value, err = reader.Close(ctx); err != nil || value.Snapshot().Steps[0].Complete {
		return errors.New("early reader close claimed EOF")
	}
	if err := finalized(old); err != nil {
		return err
	}
	rejected := second
	rejected.Name = "refused"
	if err := apply(rejected); !errors.Is(err, refused) {
		return errors.New("failed replacement lost original cause")
	}
	candidate, err := constructed()
	if err != nil {
		return err
	}
	if err := finalized(candidate.owner); err != nil {
		return err
	}
	if status, _ := followRef.Inspect(); status.Generation != currentStatus.Generation {
		return errors.New("failed candidate discarded last good source")
	}
	obsolete := second
	obsolete.Name = "obsolete"
	obsoleteUpdate, err := beginApply(obsolete)
	if err != nil {
		return err
	}
	obsoleteCandidate, err := constructed()
	if err != nil {
		return err
	}
	if obsoleteCandidate.binding != "follow" || obsoleteCandidate.owner.Info().Name != "obsolete" || obsoleteCandidate.owner.ShutdownComplete() {
		return errors.New("obsolete factory gate was not reached after real native ownership")
	}
	latest := second
	latest.Name = "latest"
	latestUpdate, err := beginApply(latest)
	if err != nil {
		return err
	}
	if err := obsoleteUpdate.Wait(ctx); !errors.Is(err, resource.ErrSuperseded) {
		return errors.New("new settings did not supersede the gated candidate")
	}
	if status, _ := followRef.Inspect(); status.Generation != currentStatus.Generation {
		return errors.New("gated obsolete candidate replaced the last good source")
	}
	releaseObsolete()
	if err := latestUpdate.Wait(ctx); err != nil {
		return err
	}
	latestCandidate, err := constructed()
	if err != nil {
		return err
	}
	latestStatus, _ := followRef.Inspect()
	if latestCandidate.owner.Info().Name != "latest" || latestStatus.Generation == currentStatus.Generation {
		return errors.New("latest generation was not adopted after superseded cleanup")
	}
	wantedCleanup := map[*duckdb.Owner]bool{obsoleteCandidate.owner: true, current.owner: true}
	for range 2 {
		select {
		case owner := <-released:
			if !wantedCleanup[owner] || !owner.ShutdownComplete() {
				return errors.New("superseded candidate or replaced source cleanup changed ownership")
			}
			delete(wantedCleanup, owner)
		case <-ctx.Done():
			return errors.New("superseded native owner cleanup was abandoned")
		}
	}
	value, err = follow.Query(ctx, ctx, "SELECT 33::BIGINT")
	if err != nil || value.Source().Name != "latest" || value.Attribution().Source.Generation != latestStatus.Generation {
		return errors.New("superseded factory result replaced the latest source")
	}
	larger := second
	larger.Name, larger.ResultBytes = "larger", 8<<20
	if err := apply(larger); err != nil {
		return err
	}
	if _, err := constructed(); err != nil {
		return err
	}
	if value, err := follow.Execute(ctx, ctx, "CREATE TABLE forbidden (id BIGINT)"); !errors.Is(err, duckdb.ErrLimit) || value.HasData() {
		return errors.New("larger replacement dispatched undercharged work")
	}
	if err := runtime.Close(ctx); err != nil {
		return err
	}
	for status, _ := inbox.Inspect(); status.Outstanding > 0; status, _ = inbox.Inspect() {
		delivery, err := inbox.NextReleased(ctx)
		if err != nil {
			return err
		}
		if err := delivery.Ack(); err != nil {
			return err
		}
	}
	usage, err := runtime.Operations().Inspect()
	if err != nil || usage.Active != 0 || usage.WorkBytes != 0 {
		return errors.New("Framework cleanup retained work reservations")
	}
	return nil
}
