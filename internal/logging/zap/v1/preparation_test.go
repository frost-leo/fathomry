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

package zap

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	sdk "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestPreparedFinalLimitsCopiesAndDependencies(t *testing.T) {
	options := OptionsV1{Name: "prepared", ExtensionLevel: "error", QueuedCalls: 2}
	layer := resource.Layer{Kind: resource.Local, Content: []byte("queued_calls: 0\nextension_level: debug\ncaller: true\n")}
	prepared, err := PrepareV1(options, true, layer)
	if err != nil {
		t.Fatal(err)
	}
	metadata := prepared.Metadata()
	if metadata.Limits.Active != 1 || metadata.Limits.Queued != 0 || metadata.Limits.QueuedBytes != 0 ||
		metadata.Limits.Bytes != metadata.WorkBytes || metadata.WorkBytes != 4<<20 || metadata.EvidenceBytes != 16<<10 ||
		metadata.SourceBytes <= metadata.DerivationBytes || metadata.PolicyBytes <= 0 || metadata.ViewBytes < MaxFieldBytes ||
		metadata.DerivationBytes != MaxDerivedBytes || metadata.MaxDerivedViews != MaxDerivedViews ||
		metadata.Files != 0 || metadata.Outputs != 1 || !metadata.Structured || metadata.FileBytes != 0 {
		t.Fatalf("incorrect resolved budget: %+v", metadata)
	}
	layer.Content[0] = 'X'
	opts := prepared.Options()
	if opts.QueuedCalls != 0 || !opts.Caller || opts.ExtensionLevel != "debug" || opts.Timeout != 5*time.Second || opts.MaxEntryBytes != 64<<10 {
		t.Fatal("effective options lost layer/default semantics")
	}
	opts.Name, opts.ExtensionLevel = "changed", "error"
	if prepared.Options().Name != "prepared" || prepared.Options().ExtensionLevel != "debug" {
		t.Fatal("returned options aliases preparation")
	}
	description := prepared.Description()
	description.Identity.Name = "changed"
	description.Provenance[0].Fields[0] = "changed"
	if prepared.Description().Identity.Name != "prepared" || prepared.Description().Provenance[0].Fields[0] == "changed" {
		t.Fatal("returned description aliases preparation")
	}
	if _, err := prepared.Select(nil); !errors.Is(err, ErrInput) {
		t.Fatal("missing declared sink accepted")
	}
	var absent *recordingSink
	if _, err := prepared.Select(absent); !errors.Is(err, ErrInput) {
		t.Fatal("typed-nil sink accepted")
	}
	for range 2 {
		sink := &recordingSink{}
		selection, err := prepared.Select(sink)
		if err != nil {
			t.Fatal(err)
		}
		selection = resource.WithLimits(selection, metadata.Limits)
		assembly, err := resource.Assemble(context.Background(), context.Background(), "prepared", selection)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := assembly.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
		inbox, err := invocation.NewInbox[Result](2, 2*metadata.EvidenceBytes)
		if err != nil {
			t.Fatal(err)
		}
		logger, err := Bind(assembly, selection, inbox, nil)
		if err != nil {
			t.Fatal("resolved budget did not bind", err)
		}
		if logger.owner.prepared.state != prepared.state || logger.policy != prepared.state || logger.owner.settings.QueuedCalls != 0 ||
			!logger.owner.settings.Caller || !logger.Enabled(zapcore.DebugLevel) || logger.Enabled(zapcore.FatalLevel) {
			t.Fatal("native construction diverged from frozen selection")
		}
		if sink.count() != 0 {
			t.Fatal("preparation/construction invoked sink")
		}
		if err := assembly.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	var zero Prepared
	if _, err := zero.Select(nil); err == nil || zero.Metadata() != (Metadata{}) || zero.PhysicalEquivalent(prepared) || zero.PhysicalEquivalent(zero) {
		t.Fatal("zero preparation became usable")
	}
}

func TestPolicyViewsSharePhysicalAdmissionAndMaintenance(t *testing.T) {
	for _, queued := range []int{0, 1} {
		t.Run(map[int]string{0: "refusal", 1: "one-waiter"}[queued], func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var enteredOnce, releaseOnce sync.Once
			sink := &recordingSink{onWrite: func(context.Context) { enteredOnce.Do(func() { close(entered) }); <-release }}
			options := OptionsV1{Name: "physical", QueuedCalls: queued, ExtensionLevel: "info"}
			fixture := bindFixture(t, options, sink, 8)
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			options.ExtensionLevel = "debug"
			prepared, err := PrepareV1(options, true)
			if err != nil {
				t.Fatal(err)
			}
			view, err := fixture.logger.WithPolicy(prepared)
			if err != nil {
				t.Fatal(err)
			}
			if view.owner != fixture.logger.owner || view.access != fixture.logger.access || view.inbox != fixture.logger.inbox || view.observer != fixture.logger.observer {
				t.Fatal("policy view minted physical ownership/admission/evidence")
			}
			type returned struct {
				receipt *invocation.Receipt[Result]
				err     error
			}
			first := make(chan returned, 1)
			go func() {
				receipt, err := fixture.logger.Log(context.Background(), fault.Correlation{Call: "old"}, zapcore.InfoLevel, "held")
				first <- returned{receipt, err}
			}()
			<-entered
			var second chan returned
			if queued == 1 {
				second = make(chan returned, 1)
				go func() {
					receipt, err := view.Log(context.Background(), fault.Correlation{Call: "new"}, zapcore.DebugLevel, "queued")
					second <- returned{receipt, err}
				}()
				await(t, func() bool { return fixture.assembly.Snapshot().Sources[0].Usage.Queued == 1 })
			}
			for _, logger := range []*Logger{fixture.logger, view} {
				if receipt, err := logger.Sync(context.Background(), fault.Correlation{Call: "sync-refused"}); receipt != nil || !errors.Is(err, resource.ErrCapacity) {
					t.Fatal("maintenance obtained extra physical allowance", err)
				}
			}
			usage := fixture.assembly.Snapshot().Sources[0].Usage
			if usage.Active != 1 || usage.Queued != queued {
				t.Fatal("views changed physical active/queued capacity")
			}
			releaseOnce.Do(func() { close(release) })
			outcome := <-first
			if result := resultOf(t, outcome.receipt, outcome.err); result.Err() != nil {
				t.Fatal(result.Err())
			}
			if second != nil {
				outcome = <-second
				if result := resultOf(t, outcome.receipt, outcome.err); result.Err() != nil || result.Outcome.Value.SinksCopy()[0].State != Written {
					t.Fatal("queued lower-level view did not complete", result.Err())
				}
			}
			receipt, err := view.Sync(context.Background(), fault.Correlation{Call: "sync"})
			if result := resultOf(t, receipt, err); result.Err() != nil || result.Outcome.Value.SinksCopy()[0].State != Synced {
				t.Fatal("shared maintenance did not resume", result.Err())
			}
		})
	}
}

func TestPolicyUpdatesDoNotConsumeCumulativeFieldDerivations(t *testing.T) {
	fixture := bindFixture(t, OptionsV1{Name: "policies"}, &recordingSink{}, 2)
	for index := range MaxDerivedViews + 1 {
		minimum := "debug"
		if index%2 == 1 {
			minimum = "error"
		}
		prepared, err := PrepareV1(OptionsV1{Name: "policies", ExtensionLevel: minimum}, true)
		if err != nil {
			t.Fatal(err)
		}
		view, err := fixture.logger.WithPolicy(prepared)
		if err != nil || view.PolicyDescription().Revision != prepared.Description().Revision {
			t.Fatal("level-only updates exhausted unrelated field-view allowance", err)
		}
	}
	if fixture.logger.owner.derivations != 0 || fixture.logger.owner.derivationBytes != 0 {
		t.Fatal("policy views consumed cumulative field retention")
	}
	for range MaxDerivedViews {
		if _, err := fixture.logger.Named("component"); err != nil {
			t.Fatal("valid field/name derivation refused", err)
		}
	}
	if _, err := fixture.logger.With(); !errors.Is(err, ErrLimit) {
		t.Fatal("aliases minted unlimited cumulative derivations", err)
	}
}

func TestDerivedFieldBytesAreCumulativeAcrossPolicyAliases(t *testing.T) {
	fixture := bindFixture(t, OptionsV1{Name: "retained"}, &recordingSink{}, 2)
	prepared, err := PrepareV1(OptionsV1{Name: "retained", ExtensionLevel: "debug"}, true)
	if err != nil {
		t.Fatal(err)
	}
	view, err := fixture.logger.WithPolicy(prepared)
	if err != nil {
		t.Fatal(err)
	}
	large := sdk.String("data", strings.Repeat("x", MaxFieldBytes-128-len("data")))
	accepted := 0
	for range MaxDerivedViews {
		logger := fixture.logger
		if accepted%2 == 1 {
			logger = view
		}
		_, err := logger.With(large)
		if errors.Is(err, ErrLimit) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		accepted++
	}
	if accepted == 0 || accepted >= MaxDerivedViews || fixture.logger.owner.derivationBytes > MaxDerivedBytes ||
		fixture.logger.owner.derivations != accepted {
		t.Fatal("retained field bytes were not bounded independently from view count")
	}
	if _, err := view.With(large); !errors.Is(err, ErrLimit) {
		t.Fatal("policy alias reset physical derivation capacity", err)
	}
}

func TestPreparedLevelChangesFitOriginalPolicyReservation(t *testing.T) {
	options := OptionsV1{Name: "levels", ExtensionLevel: "info", Outputs: []OutputV1{
		{Name: "out", Kind: "stdout", Level: "info"}, {Name: "err", Kind: "stderr", Level: "warn"},
	}}
	first, err := PrepareV1(options, true)
	if err != nil {
		t.Fatal(err)
	}
	options.ExtensionLevel, options.Outputs[0].Level, options.Outputs[1].Level = "debug", "error", "debug"
	second, err := PrepareV1(options, true)
	if err != nil {
		t.Fatal(err)
	}
	if !first.PhysicalEquivalent(second) || first.Metadata() != second.Metadata() {
		t.Fatal("legal level-only adoption exceeds the frozen physical/policy envelope")
	}
}
