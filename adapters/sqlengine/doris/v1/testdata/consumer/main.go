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
	"reflect"
	"slices"
	"strconv"
	"time"

	doris "github.com/frost-leo/fathomry/adapters/sqlengine/doris/v1"
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
	fmt.Println("doris direct and Framework Fixed/Follow consumers passed")
}
func run() error {
	var input struct{ First, Second doris.Settings }
	decoder := json.NewDecoder(os.Stdin)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := direct(ctx, input.First); err != nil {
		return err
	}
	return composed(ctx, input.First, input.Second)
}
func read(ctx context.Context, receipt *adapters.Receipt[doris.Result], err error) (doris.Result, error) {
	if err != nil {
		return doris.Result{}, err
	}
	if receipt == nil {
		return doris.Result{}, errors.New("missing receipt")
	}
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil {
		return doris.Result{}, err
	}
	value, _ := snapshot.ValueCopy()
	return value, snapshot.Err()
}
func drain(ctx context.Context, inbox *adapters.Inbox[doris.Result]) error {
	for status, _ := inbox.Inspect(); status.Outstanding > 0; status, _ = inbox.Inspect() {
		delivery, err := inbox.NextReleased(ctx)
		if err != nil {
			return err
		}
		if err := delivery.Ack(); err != nil {
			return err
		}
	}
	return nil
}
func ack(ctx context.Context, inbox *adapters.Inbox[doris.Result]) error {
	delivery, err := inbox.NextReleased(ctx)
	if err != nil {
		return err
	}
	return delivery.Ack()
}
func direct(ctx context.Context, settings doris.Settings) error {
	var policy sqlengine.Policy
	policy, err := doris.Recommend(settings)
	if err != nil {
		return err
	}
	var budget sqlengine.Budget = policy.Budget
	if policy.SourceWorkBytes <= 0 || policy.SourceEvidenceBytes <= 0 ||
		policy.Runtime.MaxWorkBytes != policy.SourceWorkBytes+int64(policy.Runtime.MaxActive-1)*budget.WorkBytes ||
		policy.Evidence.MaxBytes != policy.SourceEvidenceBytes+int64(policy.Evidence.Capacity-1)*budget.EvidenceBytes {
		return errors.New("source contributions are not included exactly once")
	}
	runtime, err := adapters.New(ctx, policy.Runtime)
	if err != nil {
		return err
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[doris.Result](policy.Evidence)
	if err != nil {
		return err
	}
	owner, err := doris.Open(ctx, settings, doris.Dependencies{Runtime: runtime, Evidence: inbox})
	if owner != nil {
		defer owner.Close(context.Background())
	}
	if err != nil {
		return err
	}
	var source sqlengine.Info = owner.Info()
	if source.Name != settings.Name || source.Revision == "" {
		return errors.New("SQL-engine source identity missing")
	}
	idle, err := runtime.Inspect()
	if err != nil || idle.Active != 1 || idle.WorkBytes != policy.SourceWorkBytes {
		return errors.New("source lifetime work differs from its declared charge")
	}
	sourceEvidence, err := inbox.Inspect()
	if err != nil || sourceEvidence.Outstanding != 1 || sourceEvidence.Bytes != policy.SourceEvidenceBytes {
		return errors.New("source evidence differs from its declared charge")
	}
	receipt, err := owner.Client().Query(ctx, "SELECT exact")
	value, err := read(ctx, receipt, err)
	if err != nil {
		return err
	}
	var provenance sqlengine.Info = value.Source()
	var attribution sqlengine.Attribution = value.Attribution()
	var attempts sqlengine.Attempts = value.Attempts()
	if provenance.Revision != source.Revision || attribution.Sequence == 0 || attempts.Observed == 0 {
		return errors.New("SQL-engine result observations missing")
	}
	cells := value.RowsCopy()[0].ValuesCopy()
	if cells[0] != nil || cells[1] == nil || string(cells[2]) != "18446744073709551615" {
		return errors.New("public values lossy")
	}
	if err := ack(ctx, inbox); err != nil {
		return err
	}
	var profile sqlengine.Profile
	profile, err = owner.Client().Profile(ctx)
	if err != nil {
		return err
	}
	if profile.SDKMode == "" {
		return errors.New("SQL-engine effective profile missing")
	}
	setup, cancel := context.WithCancel(ctx)
	cursor, root, err := owner.Client().QueryCursor(setup, ctx, "SELECT pages 5")
	cancel()
	if err != nil || cursor == nil {
		return errors.New("cursor setup failed")
	}
	total := 0
	for {
		receipt, err := cursor.Next(ctx)
		page, err := read(ctx, receipt, err)
		if err != nil {
			return err
		}
		for _, row := range page.RowsCopy() {
			if string(row.ValuesCopy()[0]) != strconv.Itoa(total) {
				return errors.New("page order changed")
			}
			total++
		}
		if err := ack(ctx, inbox); err != nil {
			return err
		}
		if page.Complete() {
			break
		}
	}
	terminal, err := read(ctx, root, nil)
	if err != nil || total != 5 || !terminal.Complete() {
		return errors.New("terminal result incomplete")
	}
	if err := cursor.Close(ctx); err != nil {
		return err
	}
	if err := owner.Close(ctx); err != nil {
		return err
	}
	if err := runtime.Close(ctx); err != nil {
		return err
	}
	released, err := runtime.Inspect()
	if err != nil || released.Active != 0 || released.WorkBytes != 0 {
		return errors.New("source work remained after actual shutdown")
	}
	retained, err := inbox.Inspect()
	if err != nil || retained.Outstanding < 1 || retained.Bytes < policy.SourceEvidenceBytes {
		return errors.New("source release erased unacknowledged evidence")
	}
	return drain(ctx, inbox)
}
func clone(value doris.Settings) doris.Settings {
	value.HTTPOrigins = slices.Clone(value.HTTPOrigins)
	return value
}
func composed(ctx context.Context, first, second doris.Settings) error {
	policy, err := doris.Recommend(first)
	if err != nil {
		return err
	}
	policy.Runtime.MaxActive *= 6
	policy.Runtime.MaxWorkBytes *= 6
	policy.Evidence.Capacity *= 6
	policy.Evidence.MaxBytes *= 6
	runtime, err := framework.New(ctx, framework.Options{Operations: policy.Runtime})
	if err != nil {
		return err
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[doris.Result](policy.Evidence)
	if err != nil {
		return err
	}
	dependencies := doris.Dependencies{Runtime: runtime.Operations(), Evidence: inbox}
	refusal := errors.New("candidate-refused")
	partialReleased := make(chan struct{}, 1)
	bind := func(name string, mode resource.Policy) (resource.Ref[doris.Handle], error) {
		return resource.Bind(runtime.Resources(), resource.Binding[doris.Settings, doris.Handle]{
			Name: name, Policy: mode, MaxGenerations: 3,
			Select: func(view settings.View) (doris.Settings, error) {
				snapshot, err := settings.As[doris.Settings](view)
				if err != nil {
					return doris.Settings{}, err
				}
				return snapshot.ValueCopy()
			},
			Clone: clone, Equal: func(a, b doris.Settings) bool { return reflect.DeepEqual(a, b) },
			Build: func(lifetime context.Context, value doris.Settings) (*resource.Instance[doris.Handle], error) {
				owner, err := doris.Open(lifetime, value, dependencies)
				if owner == nil {
					return nil, err
				}
				instance := &resource.Instance[doris.Handle]{Value: owner.Handle(), Release: func(cleanup context.Context) resource.ReleaseResult {
					released := owner.Release(cleanup)
					if value.Name == "refused" && released.Complete {
						select {
						case partialReleased <- struct{}{}:
						default:
						}
					}
					return released
				}}
				if value.Name == "refused" {
					return instance, errors.Join(err, refusal)
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
	apply := func(value doris.Settings) error {
		snapshot, err := settings.New(value, clone)
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
	fixed, err := doris.Using(ctx, fixedRef, policy.Budget, dependencies)
	if err != nil {
		return err
	}
	follow, err := doris.Using(ctx, followRef, policy.Budget, dependencies)
	if err != nil {
		return err
	}
	follow, err = follow.WithID("frozen-root")
	if err != nil {
		return err
	}
	setup, cancel := context.WithCancel(ctx)
	cursor, root, err := follow.QueryCursor(setup, ctx, "SELECT pages 5")
	cancel()
	if err != nil || cursor == nil {
		return errors.New("retained cursor setup")
	}
	if err := apply(second); err != nil {
		return err
	}
	check := func(client *doris.Client, name string, generation uint64) error {
		receipt, err := client.Query(ctx, "SELECT empty")
		value, err := read(ctx, receipt, err)
		if err != nil {
			return err
		}
		if value.Source().Name != name || value.Attribution().Source.Generation != generation {
			return errors.New("source generation changed")
		}
		return ack(ctx, inbox)
	}
	if err := check(follow, second.Name, 2); err != nil {
		return err
	}
	if err := check(fixed, first.Name, 1); err != nil {
		return err
	}
	count := 0
	for {
		receipt, err := cursor.Next(ctx)
		page, err := read(ctx, receipt, err)
		if err != nil {
			return err
		}
		if page.Source().Name != first.Name || page.Attribution().Source.Generation != 1 || page.Attribution().ID != "frozen-root" {
			return errors.New("cursor generation migrated")
		}
		count += len(page.RowsCopy())
		if err := ack(ctx, inbox); err != nil {
			return err
		}
		if page.Complete() {
			break
		}
	}
	terminal, err := read(ctx, root, nil)
	if err != nil || count != 5 || !terminal.Complete() {
		return errors.New("old generation cursor not complete")
	}
	if err := cursor.Close(ctx); err != nil {
		return err
	}
	rejected := clone(second)
	rejected.Name = "refused"
	if err := apply(rejected); !errors.Is(err, refusal) {
		return errors.New("partial candidate accepted")
	}
	select {
	case <-partialReleased:
	case <-ctx.Done():
		return errors.New("partial owner cleanup did not complete")
	}
	if err := check(follow, second.Name, 2); err != nil {
		return err
	}
	larger := clone(second)
	larger.Name = "larger"
	larger.MaxPageBytes = 16 << 20
	if err := apply(larger); err != nil {
		return err
	}
	receipt, err := follow.Exec(ctx, "INSERT forbidden-by-budget")
	_, err = read(ctx, receipt, err)
	if !errors.Is(err, doris.ErrLimit) {
		return errors.New("oversized replacement undercharged")
	}
	if err := runtime.Close(ctx); err != nil {
		return err
	}
	return drain(ctx, inbox)
}
