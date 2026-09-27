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
	"errors"
	"fmt"
	"google.golang.org/grpc/codes"
	"strings"
	"testing"
	"time"

	remote "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1"
	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
	local "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
	adapters "github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	c "github.com/frost-leo/fathomry/framework/configuration/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

func acquisition(t testing.TB, err error) source.AcquisitionInfo {
	t.Helper()
	core, ok := failure.Inspect(err)
	if !ok {
		t.Fatal("acquisition lacks direct occurrence")
	}
	typed, ok := err.(source.AcquisitionFailure)
	if !ok || typed.Failure() != core {
		t.Fatal("facts not bound to directly supplied occurrence")
	}
	info, ok := typed.Acquisition()
	if !ok {
		t.Fatal("valid acquisition facts unavailable")
	}
	sourcePrivate(t, err)
	return info
}
func TestAcquisitionFactsAndSameOccurrenceBindings(t *testing.T) {
	directory := t.TempDir()
	first := file(t, directory, "private-canary-first", "format: 1")
	second := file(t, directory, "private-canary-second", string([]byte{255}))
	selected, err := local.Select(local.Settings{Name: "alpha", Documents: []local.File{{Name: "first", Path: first, Encoding: "yaml"}, {Name: "second", Path: second, Encoding: "yaml"}}, ReconcileInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, err = selected.Capture(context.Background())
	info := acquisition(t, err)
	if !errors.Is(err, local.ErrEncoding) || info != (source.AcquisitionInfo{Source: "alpha", Document: "second", Phase: source.CapturePhase}) {
		t.Fatal("known source/slot/phase lost")
	}
	catalogs, problem := c.Catalogs(local.Module())
	if problem != nil {
		t.Fatal(problem)
	}
	binding, problem := catalogs.Bindings.Resolve(local.ModuleID+":encoding", "en")
	if problem != nil {
		t.Fatal(problem)
	}
	rendered, problem := binding.Render([]i18n.Argument{{Name: "source", Value: info.Source}, {Name: "phase", Value: string(info.Phase)}})
	if problem != nil || rendered.Text == "" {
		t.Fatal("acquisition binding failed", problem)
	}
	definition, found, problem := catalogs.Errors.Lookup(local.ErrEncoding)
	if problem != nil || !found || definition.Facts != local.ModuleID+":acquisition" {
		t.Fatal("missing public fact declaration")
	}
	contract, found, problem := catalogs.Errors.Contract(definition.Facts)
	if problem != nil || !found || contract.Access != failure.PublicFacts {
		t.Fatal("fact accessor not declared")
	}
	var optionalDocument bool
	for _, field := range contract.Fields {
		if field.Name == "document" {
			optionalDocument = !field.Required && field.UnknownAllowed
		}
	}
	if !optionalDocument {
		t.Fatal("unknown slot was declared mandatory known data")
	}
	outer, problem := failure.New(local.ErrEncoding, err)
	if problem != nil {
		t.Fatal(problem)
	}
	if _, ok := any(outer).(source.AcquisitionFailure); ok {
		t.Fatal("outer occurrence borrowed inner facts")
	}
	for _, other := range []error{fmt.Errorf("wrapper: %w", err), errors.Join(err, outer)} {
		if _, ok := failure.Inspect(other); ok {
			t.Fatal("wrapper/join acquired a fabricated primary")
		}
		if _, ok := other.(source.AcquisitionFailure); ok {
			t.Fatal("wrapper/join acquired descendant facts")
		}
	}
	observer, problem := selected.Observe(context.Background())
	if problem != nil {
		t.Fatal(problem)
	}
	t.Cleanup(func() { closeOwner(t, observer.Close) })
	degraded := awaitRaw(t, observer, func(state source.State) bool { return state.Status == source.Degraded })
	observed := acquisition(t, degraded.Failure)
	if observed.Source != "alpha" || observed.Document != "second" || observed.Phase != source.ObservePhase {
		t.Fatal("observation provenance changed")
	}
	replace(t, first, string([]byte{255}))
	replace(t, second, "format: 1")
	_, err = selected.Capture(context.Background())
	if acquisition(t, err).Document != "first" {
		t.Fatal("wrong failed slot retained")
	}
	replace(t, first, "format: 1")
	betaPath := file(t, directory, "private-canary-beta", string([]byte{255}))
	beta, problem := local.Select(local.Settings{Name: "beta", Documents: []local.File{{Name: "beta-slot", Path: betaPath, Encoding: "yaml"}}})
	if problem != nil {
		t.Fatal(problem)
	}
	input := c.Plan{Modules: []adapters.Module{local.Module()}, Inputs: []c.Input{
		{Source: selected, Documents: []c.LayerDocument{{Document: "first", Layer: c.Base}, {Document: "second", Layer: c.Environment}}},
		{Source: beta, Documents: []c.LayerDocument{{Document: "beta-slot", Layer: c.Local}}},
	}}
	_, err = c.Load(context.Background(), schema(), input)
	if actual := acquisition(t, err); actual.Source != "beta" || actual.Document != "beta-slot" {
		t.Fatal("Framework lost multi-instance attribution")
	}
	info.Source = "mutated"
	_, err = selected.Capture(context.Background())
	if err != nil {
		t.Fatal("mutating copied details affected selection", err)
	}
}

func TestRemoteAcquisitionSlotAndUnknownSessionFacts(t *testing.T) {
	fixture := newProtocolFixture(t, false)
	settings := fixture.settings()
	settings.Name = "remote-alpha"
	settings.Documents = append(settings.Documents, remote.Document{Name: "second", DataID: "second"})
	fixture.mu.Lock()
	fixture.values["second"] = "format: 1"
	fixture.mu.Unlock()
	selected, err := remote.Select(settings)
	if err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	fixture.values["second"] = strings.Repeat("x", source.MaxDocumentBytes+1)
	fixture.mu.Unlock()
	_, err = selected.Capture(context.Background())
	info := acquisition(t, err)
	if !errors.Is(err, remote.ErrLimit) || info.Source != "remote-alpha" || info.Document != "second" || info.Phase != source.CapturePhase {
		t.Fatal("remote key index mapping lost")
	}
	prior := err
	authSettings := settings
	authSettings.Name, authSettings.Username, authSettings.Password = "remote-auth", "reader", "wrong-password"
	authSource, problem := remote.Select(authSettings)
	if problem != nil {
		t.Fatal(problem)
	}
	_, err = authSource.Capture(context.Background())
	if info = acquisition(t, err); !errors.Is(err, remote.ErrDenied) || info.Document != "" {
		t.Fatal("authentication failure was attributed to an unissued query")
	}
	settings.Name = "remote-beta"
	second, problem := remote.Select(settings)
	if problem != nil {
		t.Fatal(problem)
	}
	canceled, cancel := context.WithCancelCause(context.Background())
	cancel(prior)
	_, err = second.Capture(canceled)
	info = acquisition(t, err)
	if info.Source != "remote-beta" || info.Document != "" || info.Phase != source.CapturePhase || !errors.Is(err, prior) {
		t.Fatal("unknown session borrowed another occurrence's slot or lost caller cause")
	}
	fixture.server.Stop()
	settings.RequestTimeout = 30 * time.Millisecond
	unavailable, problem := remote.Select(settings)
	if problem != nil {
		t.Fatal(problem)
	}
	_, err = unavailable.Capture(context.Background())
	if info = acquisition(t, err); info.Document != "" {
		t.Fatal("session error guessed a slot")
	}
}

func TestRemoteMultiAttemptAggregateHasNoUniqueSlot(t *testing.T) {
	first, second := newProtocolFixture(t, false), newProtocolFixture(t, false)
	first.failCode.Store(int32(codes.Unavailable))
	second.failCode.Store(int32(codes.Unavailable))
	settings := first.settings()
	settings.Servers = append(settings.Servers, second.settings().Servers[0])
	selected, err := remote.Select(settings)
	if err != nil {
		t.Fatal(err)
	}
	_, err = selected.Capture(context.Background())
	info := acquisition(t, err)
	if !errors.Is(err, remote.ErrRead) || info.Document != "" || first.queries.Load() != 1 || second.queries.Load() != 1 {
		t.Fatal("multi-attempt error borrowed a single last-attempt slot")
	}
}
