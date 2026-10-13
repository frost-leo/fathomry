/*
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

package chromedp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	cdpruntime "github.com/chromedp/cdproto/runtime"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func TestLiveFixedFollowRetainsOriginalGeneration(t *testing.T) {
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	config := Settings{Name: "base", ExecPath: path, NewWindow: true, TempDir: t.TempDir(), MaxSessions: new(1), MaxPages: new(1), Flags: map[string]string{"headless": "", "no-first-run": "", "disable-background-networking": ""}}
	prepared, err := Prepare(config, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	aggregate := prepared.Policy()
	aggregate.Runtime.MaxActive *= 4
	aggregate.Runtime.MaxWorkBytes *= 4
	aggregate.Evidence.Capacity *= 4
	aggregate.Evidence.MaxBytes *= 4
	runtime, err := adapters.New(context.Background(), aggregate.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[Result](aggregate.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	dependencies := Dependencies{Runtime: runtime, Evidence: inbox}
	scope, err := resource.New(context.Background(), resource.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := scope.Close(bounded(t)); err != nil {
			t.Error(err)
		}
		if err := runtime.Close(bounded(t)); err != nil {
			t.Error(err)
		}
	})
	bind := func(name string, policy resource.Policy) resource.Ref[Handle] {
		ref, err := resource.Bind(scope, resource.Binding[int, Handle]{
			Name: name, Policy: policy,
			Select: func(view settings.View) (int, error) {
				snapshot, err := settings.As[int](view)
				if err != nil {
					return 0, err
				}
				return snapshot.ValueCopy()
			},
			Clone: func(value int) int { return value }, Equal: func(a, b int) bool { return a == b },
			Build: func(ctx context.Context, value int) (*resource.Instance[Handle], error) {
				copy := config
				copy.Name = fmt.Sprintf("%s-%d", name, value)
				owner, err := Open(ctx, copy, dependencies)
				if owner == nil {
					return nil, err
				}
				return &resource.Instance[Handle]{Value: owner.Handle(), Release: owner.Release}, err
			}})
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	fixedRef, followRef := bind("fixed", resource.Fixed), bind("follow", resource.Follow)
	apply := func(value int) {
		snapshot, err := settings.New(value, func(v int) int { return v })
		if err != nil {
			t.Fatal(err)
		}
		update, err := scope.Apply(bounded(t), snapshot.View())
		if err != nil {
			t.Fatal(err)
		}
		if err := update.Wait(bounded(t)); err != nil {
			t.Fatal(err)
		}
	}
	apply(1)
	fixed, err := Using(context.Background(), fixedRef, prepared.Policy().Budget, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	follow, err := Using(context.Background(), followRef, prepared.Policy().Budget, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	old, oldReceipt, err := follow.Retain(bounded(t), ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunAction(bounded(t), old.Page(), Evaluate[string]("window.marker='original'")); err != nil {
		t.Fatal(err)
	}
	oldValue, objectReceipt, err := RunAction(bounded(t), old.Page(), Evaluate[*cdpruntime.RemoteObject]("({marker:window.marker})"))
	if err != nil {
		t.Fatal(err)
	}
	apply(2)
	latest, latestReceipt, err := follow.Retain(bounded(t), ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	captured, capturedReceipt, err := fixed.Retain(bounded(t), ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	oldInfo, _ := oldReceipt.Snapshot()
	latestInfo, _ := latestReceipt.Snapshot()
	fixedInfo, _ := capturedReceipt.Snapshot()
	if oldInfo.Info().Source.Generation == 0 || oldInfo.Info().Source.Generation == latestInfo.Info().Source.Generation || fixedInfo.Info().Source.Generation != 1 {
		t.Fatal("generation attribution")
	}
	if value, _, err := RunAction(bounded(t), old.Page(), Evaluate[string]("window.marker")); err != nil || value != "original" {
		t.Fatal("old retention followed replacement", value, err)
	}
	if value, _, err := RunAction(bounded(t), latest.Page(), Evaluate[string]("typeof window.marker")); err != nil || value != "undefined" {
		t.Fatal("new generation shared page", value, err)
	}
	if _, err := latest.Page().Object(oldValue); !errors.Is(err, ErrInput) {
		t.Fatal("replacement generation adopted an old reference", err)
	}
	oldObject, err := old.Page().Object(oldValue)
	if err != nil {
		t.Fatal("old reference followed replacement", err)
	}
	oldAnswer, answerReceipt, err := oldObject.Call(bounded(t), "function(){return this.marker}")
	if err != nil || oldAnswer == nil || string(oldAnswer.native.NativeValue().Value) != `"original"` {
		t.Fatal("original generation lost its receiver", err)
	}
	objectInfo, _ := objectReceipt.Snapshot()
	answerInfo, _ := answerReceipt.Snapshot()
	if objectInfo.Info().Source.Generation != oldInfo.Info().Source.Generation || answerInfo.Info().Source.Generation != oldInfo.Info().Source.Generation {
		t.Fatal("object evidence changed generation")
	}
	if err := oldObject.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	for _, retained := range []*Context{old, latest, captured} {
		if err := retained.Close(bounded(t)); err != nil {
			t.Fatal(err)
		}
	}
	if err := scope.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	for {
		status, err := inbox.Inspect()
		if err != nil {
			t.Fatal(err)
		}
		if status.Outstanding == 0 {
			break
		}
		drain(t, dependencies, 1)
	}
}
