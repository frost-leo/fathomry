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

package configuration

import (
	"context"
	"errors"
	"slices"
	"time"

	nacos "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1"
	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	viper "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

// Input is an immutable, admitted selection for a Fixed resource binding.
// Copies share private immutable data. It is not an application Config or wire DTO.
type Input struct{ state *loadInput }
type loadInput struct {
	dotenv    BootstrapInput
	variables []Variable
}

// PrepareInput admits and detaches caller data into a settings snapshot
// suitable for Scope.Apply. It performs no file, environment or service access.
func PrepareInput(ctx context.Context, dotenv BootstrapInput, variables []Variable) (settings.Snapshot[Input], error) {
	if err := admitDotenvInput(ctx, dotenv); err != nil {
		return settings.Snapshot[Input]{}, err
	}
	if len(variables) > configsource.MaxVariables {
		return settings.Snapshot[Input]{}, at(ErrLimit, "startup.variables", -1)
	}
	size := 0
	for _, value := range variables {
		if len(value.Path) > configsource.MaxDocumentBytes-size {
			return settings.Snapshot[Input]{}, at(ErrLimit, "startup.variables", -1)
		}
		size += len(value.Path)
		if len(value.Value) > configsource.MaxDocumentBytes-size {
			return settings.Snapshot[Input]{}, at(ErrLimit, "startup.variables", -1)
		}
		size += len(value.Value)
	}
	input := Input{state: &loadInput{dotenv: copyDotenv(dotenv), variables: slices.Clone(variables)}}
	return settings.New(input, func(value Input) Input { return value })
}

// Policy controls the finite bootstrap phase and early cleanup. Zero
// budgets mean 5s; BootstrapTimeout accepts 1ms..5m and CleanupTimeout 1ms..1m.
// Accepted dotenv replaces the inner cleanup upper bound. The Resource scope's
// own timeout/context may constrain it further. Receive is optional, bounded,
// non-panicking and context-cooperative; failure retains/requeues the same facts.
// It must remain usable until the Resource scope confirms shutdown.
type Policy struct {
	BootstrapTimeout time.Duration
	CleanupTimeout   time.Duration
	Receive          func(context.Context, Evidence) error
}

// Evidence holds exactly one provider's released observation. Explicit
// receipt/cause inspection can expose sensitive native details. A successful
// Receive transfers handling responsibility; it does not prove durable storage.
type Evidence struct {
	Viper *adapters.Snapshot[viper.Evidence]
	Nacos *adapters.Snapshot[nacos.Evidence]
}

// Info describes accepted configuration selection, never application readiness.
type Info struct {
	Environment   string
	Mode          string
	Provider      string
	Watching      bool
	SchemaVersion uint32
	Documents     []Document
}

// Loaded is a non-owning accepted-configuration capability obtained through
// Resource or BindConfiguration's read-only result Reader.
// InitialView is immutable; Reader may observe later accepted Watch data.
// Both data handles remain valid after cleanup. Scope retains Close authority.
type Loaded struct{ state *loaded }
type loaded struct {
	view   settings.View
	reader settings.Reader
	info   Info
}

// InitialView returns the first accepted immutable snapshot, not the latest data.
func (value Loaded) InitialView() (settings.View, error) {
	if value.state == nil {
		return settings.View{}, at(ErrHandle, "startup.view", -1)
	}
	return value.state.view, nil
}

// Reader exposes accepted data without source ownership; zero Loaded gives zero Reader.
func (value Loaded) Reader() settings.Reader {
	if value.state == nil {
		return settings.Reader{}
	}
	return value.state.reader
}

// Info copies bounded source-selection metadata without credentials or locations.
func (value Loaded) Info() (Info, error) {
	if value.state == nil {
		return Info{}, at(ErrHandle, "startup.info", -1)
	}
	info := value.state.info
	info.Documents = slices.Clone(info.Documents)
	return info, nil
}

// BindConfiguration registers the two-stage configuration loader on the existing
// Resource owner. No schema factory or source is invoked during binding.
// Scope.Apply receives PrepareInput's View; construction loads bootstrap,
// then main configuration, and returns a non-owning Loaded capability. This is
// resource integration, not application startup; no business services are created.
// Construction failure still transfers every created owner to Resource cleanup.
// The returned Reader captures a settings.Snapshot[Loaded] after main acceptance,
// before Resource adoption or lease acquisition. It owns data, not a resource;
// it remains valid through cancellation and cleanup. Before acceptance Capture
// returns settings.ErrUnconfigured. Explicit Ref.Retry may publish a later accepted
// construction; previously captured Views remain unchanged.
// Scope declarations seal on Apply. Later business resources use their own scope
// and the accepted main-configuration View, not this binding's bootstrap Input.
func BindConfiguration[T any](scope *resource.Scope, name string, schema SchemaFactory[T], policy Policy) (resource.Ref[Loaded], settings.Reader, error) {
	if schema == nil {
		return resource.Ref[Loaded]{}, settings.Reader{}, at(ErrDeclaration, "startup.bind", -1)
	}
	if policy.BootstrapTimeout == 0 {
		policy.BootstrapTimeout = 5 * time.Second
	}
	if policy.CleanupTimeout == 0 {
		policy.CleanupTimeout = 5 * time.Second
	}
	if policy.BootstrapTimeout < time.Millisecond || policy.BootstrapTimeout > 5*time.Minute ||
		policy.CleanupTimeout < time.Millisecond || policy.CleanupTimeout > time.Minute {
		return resource.Ref[Loaded]{}, settings.Reader{}, at(ErrDeclaration, "startup.policy", -1)
	}
	accepted := settings.NewStore[Loaded]()
	ref, err := resource.Bind(scope, resource.Binding[Input, Loaded]{
		Name: name, Policy: resource.Fixed,
		Select: func(view settings.View) (Input, error) {
			snapshot, err := settings.As[Input](view)
			if err != nil {
				return Input{}, err
			}
			input, err := snapshot.ValueCopy()
			if err != nil {
				return Input{}, err
			}
			if input.state == nil {
				return Input{}, at(ErrHandle, "startup.input", -1)
			}
			return input, nil
		},
		Clone: func(value Input) Input { return value },
		Build: func(lifetime context.Context, input Input) (*resource.Instance[Loaded], error) {
			return buildConfiguration(lifetime, input, schema, policy, accepted)
		},
	})
	if err != nil {
		return resource.Ref[Loaded]{}, settings.Reader{}, err
	}
	return ref, accepted.Reader(), nil
}

type configurationOwner[T any] struct {
	cancel        context.CancelFunc
	runtime       *adapters.Runtime
	local         *adapters.Inbox[viper.Evidence]
	remote        *adapters.Inbox[nacos.Evidence]
	session       *Session[T]
	shutdown      time.Duration
	receive       func(context.Context, Evidence) error
	evidenceError error
}

func buildConfiguration[T any](lifetime context.Context, input Input, factory SchemaFactory[T], policy Policy, accepted settings.Store[Loaded]) (*resource.Instance[Loaded], error) {
	// Resource cancels generations before Release. The owned source lifetime
	// ends through explicit cleanup, while construction still obeys lifetime.
	owned, cancel := context.WithCancel(context.WithoutCancel(lifetime))
	owner := &configurationOwner[T]{cancel: cancel, shutdown: policy.CleanupTimeout, receive: policy.Receive}
	instance := &resource.Instance[Loaded]{Release: owner.release}
	var err error
	owner.runtime, err = adapters.New(owned, adapters.Options{MaxActive: 2, MaxWorkBytes: 64 << 20})
	if err != nil {
		return instance, err
	}
	owner.local, err = adapters.NewInbox[viper.Evidence](adapters.EvidenceOptions{Capacity: 4, MaxBytes: 4 * 64 << 10})
	if err != nil {
		return instance, err
	}
	owner.remote, err = adapters.NewInbox[nacos.Evidence](adapters.EvidenceOptions{Capacity: 4, MaxBytes: 4 * 64 << 10})
	if err != nil {
		return instance, err
	}
	dependencies := Dependencies{Local: viper.Dependencies{Runtime: owner.runtime, Evidence: owner.local},
		Remote: nacos.Dependencies{Runtime: owner.runtime, Evidence: owner.remote}}
	bootstrapContext, stop := context.WithTimeout(lifetime, policy.BootstrapTimeout)
	defer stop()
	selected, err := LoadBootstrap(bootstrapContext, input.state.dotenv, dependencies.Local)
	if err != nil {
		return instance, err
	}
	owner.shutdown = selected.ShutdownTimeout
	if err := owner.receiveLocal(bootstrapContext); err != nil {
		return instance, err
	}
	stop()
	if owner.evidenceError != nil {
		return instance, owner.evidenceError
	}
	if err := lifetime.Err(); err != nil {
		return instance, at(ErrClosed, "startup.construct", -1, err, context.Cause(lifetime))
	}
	initial, stopInitial := context.WithTimeout(lifetime, selected.InitialTimeout)
	defer stopInitial()
	schema, err := factory(initial, selected.Configuration.Environment)
	if err != nil {
		return instance, at(ErrDeclaration, "startup.schema", -1, err)
	}
	if err := initial.Err(); err != nil {
		return instance, at(ErrWait, "startup.schema", -1, err, context.Cause(initial))
	}
	owner.session, err = loadConfiguration(owned, initial, selected, schema, input.state.variables, dependencies, &accepted)
	if err != nil {
		if owner.session != nil {
			// Initial waiting can race acceptance. Result custody already belongs to
			// publication; fence any later result before Resource starts cleanup.
			owner.session.state.stop(err)
		}
		return instance, err
	}
	view, err := accepted.Reader().Capture()
	if err != nil {
		return instance, err
	}
	snapshot, err := settings.As[Loaded](view)
	if err != nil {
		return instance, err
	}
	instance.Value, err = snapshot.ValueCopy()
	return instance, err
}

func (owner *configurationOwner[T]) release(parent context.Context) resource.ReleaseResult {
	ctx, cancel := context.WithTimeout(parent, owner.shutdown)
	defer cancel()
	var problem error
	if owner.session != nil {
		problem = owner.session.Close(ctx)
		if !owner.session.ShutdownComplete() {
			return resource.ReleaseResult{Err: at(ErrCleanup, "startup.session", -1, problem)}
		}
	}
	if owner.runtime != nil {
		if err := owner.runtime.Close(ctx); err != nil {
			return resource.ReleaseResult{Err: errors.Join(problem, err)}
		}
	}
	if owner.local != nil {
		problem = errors.Join(problem, owner.local.Seal())
	}
	if owner.remote != nil {
		problem = errors.Join(problem, owner.remote.Seal())
	}
	local, remote := owner.receiveLocal(ctx), owner.receiveRemote(ctx)
	if local != nil || remote != nil {
		return resource.ReleaseResult{Err: errors.Join(problem, local, remote)}
	}
	owner.cancel()
	return resource.ReleaseResult{Complete: true, Err: errors.Join(problem, owner.evidenceError)}
}

func (owner *configurationOwner[T]) receiveLocal(ctx context.Context) error {
	return receiveEvidence(ctx, owner.local, &owner.evidenceError, func(snapshot adapters.Snapshot[viper.Evidence]) error {
		if owner.receive != nil {
			return owner.receive(ctx, Evidence{Viper: &snapshot})
		}
		return nil
	})
}
func (owner *configurationOwner[T]) receiveRemote(ctx context.Context) error {
	return receiveEvidence(ctx, owner.remote, &owner.evidenceError, func(snapshot adapters.Snapshot[nacos.Evidence]) error {
		if owner.receive != nil {
			if err := owner.receive(ctx, Evidence{Nacos: &snapshot}); err != nil {
				return err
			}
		}
		value, present := snapshot.ValueCopy()
		if present && value.MutationPresent {
			owner.evidenceError = errors.Join(owner.evidenceError, at(ErrObservation, "startup.mutation", -1))
		}
		return nil
	})
}
func receiveEvidence[T any](ctx context.Context, inbox *adapters.Inbox[T], observed *error, receive func(adapters.Snapshot[T]) error) error {
	if inbox == nil {
		return nil
	}
	for {
		status, err := inbox.Inspect()
		if err != nil {
			return err
		}
		if status.Outstanding == 0 {
			return nil
		}
		delivery, err := inbox.NextReleased(ctx)
		if err != nil {
			return err
		}
		receipt, err := delivery.Receipt()
		if err != nil {
			return errors.Join(err, delivery.Retry())
		}
		snapshot, err := receipt.WaitReleased(ctx)
		if err != nil {
			return errors.Join(err, delivery.Retry())
		}
		if err := receive(snapshot); err != nil {
			return errors.Join(at(ErrCleanup, "startup.evidence", -1, err), delivery.Retry())
		}
		*observed = errors.Join(*observed, snapshot.Err())
		if err := delivery.Ack(); err != nil {
			return errors.Join(err, delivery.Retry())
		}
	}
}
