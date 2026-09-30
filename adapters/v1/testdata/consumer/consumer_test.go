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

package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

func TestPublicOperation(t *testing.T) {
	ctx := context.Background()
	var options adapters.Options
	if err := json.Unmarshal([]byte(`{"max_active":1,"max_work_bytes":1024}`), &options); err != nil {
		t.Fatal(err)
	}
	owner, err := adapters.New(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(ctx)
	inbox, err := adapters.NewInbox[[]byte](adapters.EvidenceOptions{Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := adapters.Bind(owner, adapters.Declaration[[]byte]{Evidence: inbox, Copy: func(value []byte) []byte { return append([]byte(nil), value...) }})
	if err != nil {
		t.Fatal(err)
	}
	native := errors.New("native failure")
	var guard adapters.Guard
	receipt, err := endpoint.Run(ctx, adapters.Request{Operation: "consumer.read", WorkBytes: 128, EvidenceBytes: 64},
		func(call *adapters.Call[[]byte]) {
			guard, _ = call.Hold()
			_ = call.Resolve(adapters.Outcome[[]byte]{Value: []byte{7}, Present: true, Primary: native})
		})
	if err != nil {
		t.Fatal(err)
	}
	result, err := receipt.Wait(ctx)
	if err != nil || result.Info().Released || !errors.Is(result.Err(), native) {
		t.Fatal("outcome lost ownership/causes")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := receipt.WaitReleased(canceled); !errors.Is(err, adapters.ErrWait) {
		t.Fatal("live work released")
	}
	if guard.Release() != nil {
		t.Fatal("guard not released")
	}
	value, err := receipt.WaitReleased(ctx)
	if err != nil || !value.Info().Released {
		t.Fatal("release missing")
	}
	copied, present := value.ValueCopy()
	if !present || len(copied) != 1 || copied[0] != 7 {
		t.Fatal("facts missing")
	}
	copied[0] = 9
	*receipt = adapters.Receipt[[]byte]{}
	if err := inbox.DeliverOne(ctx, func(_ context.Context, result adapters.Snapshot[[]byte]) error {
		copied, present := result.ValueCopy()
		if !present || copied[0] != 7 || !errors.Is(result.Err(), native) {
			return errors.New("independent facts mutated")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	components := append(i18n.CoreComponents(), i18n.Component{Module: "fathomry", Name: "operation", BaseLocale: "en", Resources: adapters.Resources(), Directory: "resources", Definitions: adapters.Definitions()})
	catalog, err := i18n.Prepare(components...)
	if err != nil {
		t.Fatal(err)
	}
	presenter, _ := i18n.NewPresenter(catalog)
	presenter, _ = presenter.WithLocale("zh-CN")
	localized, ok := presenter.Present(value.Err()).(*i18n.Presented)
	if !ok || localized.Issue() != nil || localized.Info().Message.Locale != "zh-CN" || !errors.Is(localized, native) {
		t.Fatal("public localized occurrence failed")
	}
}
