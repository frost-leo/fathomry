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

package mail

import (
	"context"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestActualConsumingExecutable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "consumer")
	command := exec.CommandContext(ctx, "go", "build", "-o", binary, "./testdata/consumer")
	command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("consumer build failed: %v\n%s", err, output)
	}
	output, err := exec.CommandContext(ctx, binary).Output()
	if err != nil {
		t.Fatal("consumer execution failed")
	}
	var result struct {
		Go, SDK                        string
		ConstructedAndClosed, MailSent bool
	}
	if json.Unmarshal(output, &result) != nil || result.Go != runtime.Version() || result.SDK != "v0.8.1" || !result.ConstructedAndClosed || result.MailSent {
		t.Fatal("consumer execution evidence changed")
	}
	info, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, dependency := range info.Deps {
		if dependency.Path == "github.com/wneessen/go-mail" {
			found = dependency.Version == "v0.8.1" && dependency.Replace == nil && dependency.Sum == "h1:tVcncj02/QySVFw3zr/kXOzZcuFQqBNT6K+Rbgm/pcM="
		}
	}
	if !found {
		t.Fatal("consumer does not use the pinned unmodified SDK")
	}
}

func TestComposedSourceAndIndependentEvidence(t *testing.T) {
	peer := newPeer(t, peerOptions{})
	bound := bindTest(t, testOptions(peer), 1)
	message := plainMessage(t, "composed")
	receipt, err := bound.client.Send(context.Background(), fault.Correlation{Call: "composed", Owner: "run-test"}, message)
	got := resolved(t, receipt, err)
	want := conformance.Expected[Result]{Context: fault.Context{Provider: ProviderID, Source: "mail", Scope: "mail-test", Operation: "send", Correlation: fault.Correlation{Call: "composed", Owner: "run-test"}},
		Source: bound.client.access.Info(), Limits: LimitsV1(testOptions(peer)), Shape: invocation.Finite, Present: true, Final: true, Released: true,
		Attempts: invocation.Attempts{Exact: true, Observed: 1}, Value: func(t testing.TB, value Result) {
			messages := value.Messages()
			if len(messages) != 1 || messages[0].ID() != "composed@fixture.test" || messages[0].Effect() != Accepted || messages[0].EnhancedCode() != "2.0.0" {
				t.Error("independent effect oracle differs")
			}
		}}
	conformance.Result(t, got, want)
	// Handling a caller receipt must not free the independent evidence slot.
	if _, err = bound.client.Send(context.Background(), fault.Correlation{Call: "saturated"}, message); !errors.Is(err, invocation.ErrEvidence) {
		t.Fatal("evidence saturation permitted native entry")
	}
	captured, _ := peer.snapshot()
	if len(captured) != 1 {
		t.Fatal("saturated call sent mail")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conformance.Receive(t, ctx, bound.inbox, []conformance.Expected[Result]{want})
	conformance.Facade(t, bound.client, "Send", "SendBatch", "Profile", "String", "GoString", "Format", "LogValue", "MarshalJSON", "UnmarshalJSON")
	conformance.Runtime(t, bound.client, new(Client))
	profile := bound.client.Profile()
	profile.Options[0].Value = "mutated"
	if bound.client.Profile().Options[0].Value == "mutated" {
		t.Fatal("profile aliases")
	}
	build, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{"github.com/wneessen/go-mail"}})
	if err != nil {
		t.Fatal(err)
	}
	report, err := compatibility.Assess(build, bound.client.access, bound.client.Profile(), []compatibility.Requirement{
		{Guarantee: "smtp-mime-delivery", Layers: []compatibility.Layer{compatibility.Capability, compatibility.SDK, compatibility.Service}}}, nil)
	if err != nil || report.Require(compatibility.Policy{}) == nil {
		t.Fatal("declarations certified unobserved service support")
	}
}
func TestBorrowedClientSharesAdmissionAndOwnership(t *testing.T) {
	peer := newPeer(t, peerOptions{})
	bound := bindTest(t, testOptions(peer), 3)
	borrowed := resource.Borrow("mail-alias", bound.assembly, bound.selected)
	alias, err := resource.Assemble(context.Background(), context.Background(), "borrower", borrowed)
	if err != nil {
		t.Fatal(err)
	}
	defer alias.Close(context.Background())
	client, err := Bind(alias, borrowed, bound.inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	held, err := bound.client.access.Acquire(context.Background(), defaults(testOptions(peer)).reservation())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.Send(context.Background(), fault.Correlation{Call: "full"}, plainMessage(t, "full")); !errors.Is(err, resource.ErrCapacity) {
		t.Fatal("borrow alias created a second quota")
	}
	held.Release()
	got := sendTest(t, client, "borrowed", plainMessage(t, "borrowed"))
	if got.Err() != nil || got.Context.Source != "mail" || got.Context.Scope != "mail-test" {
		t.Fatal("borrowed source association changed")
	}
	if err = alias.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	got = sendTest(t, bound.client, "owner-live", plainMessage(t, "owner-live"))
	if got.Err() != nil || peer.connections.Load() != 1 {
		t.Fatal("borrower closed the owner's connection")
	}
	drain(t, bound.inbox)
}
func TestQueuedCancellationAndShutdownRejectNewWork(t *testing.T) {
	peer := newPeer(t, peerOptions{})
	options := testOptions(peer)
	options.QueuedCalls = 1
	bound := bindTest(t, options, 1)
	held, err := bound.client.access.Acquire(context.Background(), defaults(options).reservation())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err = bound.client.Send(ctx, fault.Correlation{Call: "queue"}, plainMessage(t, "queue")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("queued call ignored deadline")
	}
	held.Release()
	if bound.inbox.Usage().Outstanding != 0 || peer.connections.Load() != 0 {
		t.Fatal("canceled admission retained evidence or dialed")
	}
	if err = bound.assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = bound.client.Send(context.Background(), fault.Correlation{Call: "closed"}, plainMessage(t, "closed")); err == nil {
		t.Fatal("closed owner admitted work")
	}
}
func TestConcurrentCallsBoundSocketsAndFreezeReuse(t *testing.T) {
	peer := newPeer(t, peerOptions{})
	options := testOptions(peer)
	options.MaxActive = 4
	options.QueuedCalls = 16
	bound := bindTest(t, options, 16)
	message := plainMessage(t, "shared")
	var workers sync.WaitGroup
	failures := make(chan error, 16)
	for index := range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			receipt, err := bound.client.Send(context.Background(), fault.Correlation{Call: "parallel-" + strconv.Itoa(index)}, message)
			if err == nil {
				if result, ok := receipt.Result(); ok {
					err = result.Err()
				} else {
					err = errors.New("missing result")
				}
			}
			failures <- err
		}()
	}
	workers.Wait()
	for range 16 {
		if err := <-failures; err != nil {
			t.Fatal(err)
		}
	}
	captured, _ := peer.snapshot()
	if len(captured) != 16 || peer.maximum.Load() > 4 || peer.connections.Load() > 4 {
		t.Fatal("concurrent calls exceeded source or lost messages")
	}
	drain(t, bound.inbox)
}

func TestShutdownRetainsActiveNativeUseUntilCancellationJoins(t *testing.T) {
	peer := newPeer(t, peerOptions{stall: "DATA"})
	bound := bindTest(t, testOptions(peer), 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	message := plainMessage(t, "shutdown")
	done := make(chan error, 1)
	go func() {
		receipt, err := bound.client.Send(ctx, fault.Correlation{Call: "shutdown"}, message)
		if err == nil {
			if result, present := receipt.Result(); present {
				err = result.Err()
			} else {
				err = errors.New("missing shutdown result")
			}
		}
		done <- err
	}()
	deadline := time.After(time.Second)
	for {
		select {
		case command := <-peer.reached:
			if command == "DATA" {
				goto entered
			}
		case <-deadline:
			t.Fatal("native call did not reach the controlled stall")
		}
	}
entered:
	if err := bound.assembly.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("shutdown claimed an active call was released")
	}
	sources := bound.assembly.Snapshot().Sources
	if len(sources) != 1 || sources[0].Released || sources[0].Usage.Active != 1 {
		t.Fatal("shutdown lost native ownership")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("active shutdown call lost cancellation evidence")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled shutdown call did not join")
	}
	drain(t, bound.inbox)
	if err := bound.assembly.Close(context.Background()); err != nil {
		t.Fatal("shutdown could not complete after native use ended")
	}
	sources = bound.assembly.Snapshot().Sources
	if !sources[0].Released || !sources[0].Quiescent || sources[0].Usage.Active != 0 {
		t.Fatal("completed shutdown retained unaccounted native use")
	}
}
