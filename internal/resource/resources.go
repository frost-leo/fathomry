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

package resource

import (
	"context"
	"errors"
	"reflect"
	"sync"

	"github.com/frost-leo/fathomry/internal/fault"
)

// ReleaseFunc executes synchronous, Provider-specific cleanup. It must account for
// dependent/background/native resources, honor its documented cancellation limits,
// and never reenter Assembly.Close. The manager launches no goroutine to hide a
// blocked callback. Callback completion is not evidence of external effect reversal.
type ReleaseFunc func(context.Context) ReleaseResult

// ReleaseResult separates positive shutdown evidence from errors. False means not
// confirmed, not evidence of ongoing work or of no effect. Quiescent confirms that
// this resource's users/background work no longer need its dependencies; Released
// confirms release of all resources this record owns. Both are required for completion.
// Continue explicitly supplies the next cleanup/reconciliation action, if any.
// The original callback is NEVER automatically retried after an incomplete result.
// A continuation must report actual evidence, not equate a later no-op nil with release.
type ReleaseResult struct {
	Quiescent bool
	Released  bool
	Err       error
	Continue  ReleaseFunc
}

// Resource is returned only to composition, never to a business consumer.
// Acquired must be true whenever construction acquired any cleanup responsibility,
// including on error; Release must then retain all necessary cleanup handles.
// Capability must be a non-owning facade with no raw client/lifecycle escape,
// including through its dynamic type, callbacks, or returned values. A narrow
// interface over a raw client does not meet this contract.
// Check is optional explicitly authorized readiness work, separate from construction.
type Resource[C any] struct {
	Acquired   bool
	Capability C
	Release    ReleaseFunc
	Check      func(context.Context) error
}

// Factory gets an isolated settings copy on every construction. Closures and
// injected dependencies are Provider-owned, explicit, and concurrency-safe;
// no ambient configuration discovery or undeclared sharing is permitted.
type Factory[T, C any] func(context.Context, T) (Resource[C], error)

// Spec is a sealed heterogeneous assembly input, produced by Select/Borrow/Delegate.
// It is not a registry entry or a runtime name-based service locator.
type Spec interface{ specification() *specification }

// Selection binds a capability's static type to an explicitly selected source.
// Its zero value is invalid. A selection can be reused in independent assemblies.
type Selection[C any] struct {
	spec *specification
	_    [0]func() C
}

func (selection Selection[C]) specification() *specification { return selection.spec }

// Select records an already prepared configuration and constructor without running
// it. Even an ignored Prepare error produces an invalid selection rejected before
// any constructor in the assembly runs.
func Select[T, C any](prepared Prepared[T], factory Factory[T, C]) Selection[C] {
	spec := &specification{mode: Owned, description: prepared.Description()}
	if prepared.state != nil && factory != nil {
		spec.construct = func(ctx context.Context) (constructed, error) {
			settings, err := prepared.settings()
			if err != nil {
				return constructed{}, err
			}
			value, err := factory(ctx, settings)
			return constructed{acquired: value.Acquired, capability: value.Capability,
				release: value.Release, check: value.Check}, err
		}
	}
	spec.name = spec.description.Identity.Name
	return Selection[C]{spec: spec}
}

// Borrow selects an existing source under a local alias. It acquires a lease on
// the SAME authoritative resource record; closing the new assembly returns only
// that lease. The owner cannot release the resource until all borrowing scopes end.
func Borrow[C any](name string, from *Assembly, selected Selection[C]) Selection[C] {
	return sharedSelection(name, from, selected, Borrowed)
}

// Delegate transfers exclusive cleanup ownership to the receiving assembly at its
// acquisition step. Preflight failure leaves ownership unchanged. After acceptance,
// even later initialization failure leaves cleanup with the returned receiver.
// Only single-entry donor assemblies can delegate: transferring one member of an
// ordered dependency set would break lifetime ordering. Its entire owned dependency
// set must be contained within that record. Active borrowers prevent transfer.
// Composition must quiesce all previously handed
// out capabilities first; this does not revoke arbitrary Go values or transfer
// closure ownership into a concrete SDK. The donor can no longer Bind this source.
func Delegate[C any](name string, from *Assembly, selected Selection[C]) Selection[C] {
	return sharedSelection(name, from, selected, Delegated)
}

func sharedSelection[C any](name string, from *Assembly, selected Selection[C], mode Ownership) Selection[C] {
	spec := &specification{name: name, mode: mode, from: from, original: selected.spec}
	if selected.spec != nil {
		spec.description = selected.spec.description.Clone()
	}
	return Selection[C]{spec: spec}
}

type specification struct {
	name        string
	mode        Ownership
	description Description
	construct   func(context.Context) (constructed, error)
	from        *Assembly
	original    *specification
	limits      *Limits
}

type constructed struct {
	acquired   bool
	capability any
	release    ReleaseFunc
	check      func(context.Context) error
}

// Assembly owns startup/shutdown coordination for an explicitly ordered source set.
// Do not copy it. Snapshots, binding, sharing, and Close may run concurrently.
// Composition must choose distinct scope labels wherever evidence from independent
// assemblies is combined; no global scope registry or process identity is inferred.
// Capabilities and callback-captured dependencies retain their documented concurrency
// contracts. No per-call state, Run identity, retry policy, or SDK ownership is
// handed to business consumers by this manager.
type Assembly struct {
	mu      sync.Mutex
	scope   string
	ready   bool
	primary error
	entries []*entry
	gate    chan struct{}
}

type entry struct {
	spec   *specification
	record *record
	lease  bool
	check  func(context.Context) error
	uses   int
}

type record struct {
	mu            sync.Mutex
	info          Info
	capability    any
	owner         *Assembly
	admitting     bool
	borrowers     int
	quiescent     bool
	released      bool
	next          ReleaseFunc
	cleanupErrors []error
	admission     admission
}

// Assemble preflights the entire selected set before acquiring any resources.
// Factories run in declaration order, then explicit readiness checks run in that
// order. Dependencies must precede their consumers, including closure captures.
// On failure, the returned non-nil Assembly retains any incomplete responsibility;
// Bind always refuses a failed assembly. cleanupCtx is caller-owned and separate
// from ctx; neither is stored after this call. There is no implicit rollback.
// Framework-owned cancellation exits retain ctx.Err() and context.Cause(ctx);
// constructor/readiness errors remain primary when those callbacks return errors.
func Assemble(ctx, cleanupCtx context.Context, scope string, selected ...Spec) (*Assembly, error) {
	if ctx == nil || cleanupCtx == nil || !validID(scope) {
		return nil, ErrSelection.New(fault.Context{Operation: "assemble"})
	}
	specs := make([]*specification, len(selected))
	names := make(map[string]bool)
	sharedModes := make(map[*record]Ownership)
	for index, selected := range selected {
		if nilValue(selected) {
			return nil, ErrSelection.New(fault.Context{Operation: "preflight", Scope: scope})
		}
		spec := selected.specification()
		if spec == nil || !validID(spec.name) || names[spec.name] ||
			!validID(spec.description.Identity.Provider) || spec.description.Revision == "" ||
			spec.mode == Owned && spec.construct == nil ||
			spec.limits != nil && (spec.mode != Owned || !validLimits(*spec.limits)) {
			return nil, ErrSelection.New(fault.Context{Operation: "preflight", Scope: scope})
		}
		if spec.mode != Owned {
			resource, err := existing(spec, nil, false)
			if err != nil {
				return nil, err
			}
			if previous, exists := sharedModes[resource]; exists && (previous == Delegated || spec.mode == Delegated) {
				return nil, ErrSelection.New(fault.Context{Operation: "preflight", Scope: scope})
			}
			sharedModes[resource] = spec.mode
		}
		names[spec.name] = true
		specs[index] = spec
	}
	assembly := &Assembly{scope: scope, gate: make(chan struct{}, 1)}
	fail := func(err error) (*Assembly, error) {
		assembly.primary = err
		cleanup := assembly.Close(cleanupCtx)
		return assembly, ErrAssembly.New(fault.Context{Operation: "assemble", Scope: scope}, err, cleanup)
	}
	for _, spec := range specs {
		if err := ctx.Err(); err != nil {
			return fail(ErrInitialization.New(location(scope, spec.description, "construct"), err, context.Cause(ctx)))
		}
		if spec.mode != Owned {
			shared, err := existing(spec, assembly, true)
			if err != nil {
				return fail(err)
			}
			assembly.entries = append(assembly.entries, &entry{spec: spec, record: shared, lease: spec.mode == Borrowed})
			continue
		}
		value, err := spec.construct(ctx)
		acquired := value.acquired || value.release != nil
		resource := &record{info: Info{Scope: scope, Configuration: spec.description},
			capability: value.capability, owner: assembly, admitting: true,
			quiescent: !acquired, released: !acquired, next: value.release}
		resource.admission.changed = make(chan struct{})
		if spec.limits != nil {
			resource.admission.limits = *spec.limits
		}
		assembly.entries = append(assembly.entries, &entry{spec: spec, record: resource, check: value.check})
		if err != nil {
			return fail(ErrInitialization.New(location(scope, spec.description, "construct"), err))
		}
		if !acquired || value.release == nil || nilValue(value.capability) {
			return fail(ErrInitialization.New(location(scope, spec.description, "construct"),
				errors.New("source: constructor omitted a capability or cleanup responsibility")))
		}
	}
	for _, entry := range assembly.entries {
		if err := ctx.Err(); err != nil {
			return fail(ErrInitialization.New(location(scope, entry.spec.description, "check"), err, context.Cause(ctx)))
		}
		if entry.check != nil {
			if err := entry.check(ctx); err != nil {
				return fail(ErrInitialization.New(location(scope, entry.spec.description, "check"), err))
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return fail(ErrInitialization.New(fault.Context{Operation: "assemble", Scope: scope}, err, context.Cause(ctx)))
	}
	assembly.ready = true
	return assembly, nil
}

func location(scope string, description Description, operation string) fault.Context {
	return fault.Context{Scope: scope, Source: description.Identity.Name,
		Provider: description.Identity.Provider, Operation: operation}
}

// existing locks only the donor assembly and then the single shared record.
// No user callback executes while either lock is held.
func existing(spec *specification, receiver *Assembly, acquire bool) (*record, error) {
	fail := func() (*record, error) { return nil, ErrSelection.New(fault.Context{Operation: "share"}) }
	if spec.from == nil || spec.original == nil {
		return fail()
	}
	spec.from.mu.Lock()
	defer spec.from.mu.Unlock()
	if !spec.from.ready || spec.mode == Delegated && len(spec.from.entries) != 1 {
		return fail()
	}
	for _, entry := range spec.from.entries {
		if entry.spec != spec.original {
			continue
		}
		resource := entry.record
		resource.mu.Lock()
		defer resource.mu.Unlock()
		if !resource.admitting || resource.released ||
			(entry.spec.mode == Borrowed && !entry.lease) ||
			(entry.spec.mode != Borrowed && resource.owner != spec.from) {
			return fail()
		}
		if spec.mode == Delegated && (resource.owner != spec.from || resource.borrowers != 0 || resource.admission.queue.Len() != 0) {
			return fail()
		}
		if acquire {
			if spec.mode == Borrowed {
				resource.borrowers++
			} else {
				resource.owner = receiver
			}
		}
		return resource, nil
	}
	return fail()
}

func nilValue(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	}
	return false
}

// Bind is the trusted outer binding boundary, not a business service locator.
// Only the exact selected token is accepted; there is no name fallback or cast
// across capability types. Pass its non-owning facade, not Assembly/Resource, to
// business code. The caller must stop using it before ending its owning/borrowing
// scope. Controlled integrations also bind AccessFor and do not expose this
// underlying capability as an unconstrained public operation path.
func Bind[C any](assembly *Assembly, selected Selection[C]) (C, Info, error) {
	var zero C
	fail := func() (C, Info, error) {
		return zero, Info{}, ErrSelection.New(fault.Context{Operation: "bind"})
	}
	if assembly == nil || selected.spec == nil {
		return fail()
	}
	assembly.mu.Lock()
	defer assembly.mu.Unlock()
	if !assembly.ready {
		return fail()
	}
	for _, entry := range assembly.entries {
		if entry.spec != selected.spec {
			continue
		}
		resource := entry.record
		resource.mu.Lock()
		defer resource.mu.Unlock()
		if entry.spec.mode == Borrowed {
			if !entry.lease {
				return fail()
			}
		} else if resource.owner != assembly {
			return fail()
		}
		value, ok := resource.capability.(C)
		if !ok {
			return fail()
		}
		return value, Info{Scope: resource.info.Scope, Configuration: resource.info.Configuration.Clone()}, nil
	}
	return fail()
}

// Snapshot copies metadata and error slices; it never exposes lifecycle callbacks
// or client handles. Per-record observations are consistent, not a transactionally
// simultaneous snapshot of separate resources being closed by other assemblies.
func (assembly *Assembly) Snapshot() Report {
	if assembly == nil {
		return Report{}
	}
	assembly.mu.Lock()
	defer assembly.mu.Unlock()
	report := Report{Ready: assembly.ready, Primary: assembly.primary}
	for _, entry := range assembly.entries {
		resource := entry.record
		resource.mu.Lock()
		status := Status{Name: entry.spec.name, Info: Info{Scope: resource.info.Scope, Configuration: resource.info.Configuration.Clone()},
			Ownership: entry.spec.mode, OwnerScope: resource.owner.scope, Borrowers: resource.borrowers - resource.admission.active,
			Quiescent: resource.quiescent, Released: resource.released,
			CanContinue: resource.next != nil, CleanupErrors: append([]error(nil), resource.cleanupErrors...)}
		status.Usage = resource.admission.usage()
		status.Limits = resource.admission.limits
		switch {
		case entry.spec.mode == Borrowed:
			status.Returned = !entry.lease
			status.Pending = entry.lease
			status.CanContinue = false
		case resource.owner != assembly:
			status.Ownership = Transferred
			status.CanContinue = false
		default:
			status.Pending = !resource.quiescent || !resource.released
		}
		resource.mu.Unlock()
		report.Sources = append(report.Sources, status)
	}
	return report
}

// Close refuses new bindings/shares in this scope, then releases in reverse order.
// A pending resource conservatively retains all earlier dependencies, including
// borrowed leases. No precise independent-branch cleanup is claimed.
// Calls serialize; waiting for another Close honors ctx, but arbitrary callbacks
// cannot be forcibly canceled. Historical cleanup errors remain visible even after
// confirmed completion; incomplete cleanup always returns ErrIncomplete.
func (assembly *Assembly) Close(ctx context.Context) error {
	if assembly == nil || assembly.gate == nil || ctx == nil {
		return ErrSelection.New(fault.Context{Operation: "close"})
	}
	assembly.mu.Lock()
	assembly.ready = false
	for _, entry := range assembly.entries {
		entry.record.mu.Lock()
		if entry.record.owner == assembly {
			entry.record.admitting = false
		}
		entry.record.admission.signal()
		entry.record.mu.Unlock()
	}
	assembly.mu.Unlock()
	select {
	case assembly.gate <- struct{}{}:
		defer func() { <-assembly.gate }()
	case <-ctx.Done():
		return assembly.closeError(errors.Join(ctx.Err(), context.Cause(ctx)))
	}
	var interrupted error
	for index := len(assembly.entries) - 1; index >= 0; index-- {
		entry := assembly.entries[index]
		resource := entry.record
		assembly.mu.Lock()
		resource.mu.Lock()
		if entry.uses != 0 {
			if err := ctx.Err(); err != nil {
				interrupted = errors.Join(err, context.Cause(ctx))
			}
			resource.mu.Unlock()
			assembly.mu.Unlock()
			break
		}
		if entry.spec.mode == Borrowed {
			if entry.lease {
				resource.borrowers--
				entry.lease = false
			}
			resource.mu.Unlock()
			assembly.mu.Unlock()
			continue
		}
		assembly.mu.Unlock()
		if resource.owner != assembly || resource.quiescent && resource.released {
			resource.mu.Unlock()
			continue
		}
		if err := ctx.Err(); err != nil {
			interrupted = errors.Join(err, context.Cause(ctx))
			resource.mu.Unlock()
			break
		}
		if resource.borrowers != 0 || resource.next == nil {
			resource.mu.Unlock()
			break
		}
		next := resource.next
		resource.next = nil
		resource.mu.Unlock()
		result := next(ctx)
		resource.mu.Lock()
		resource.quiescent = resource.quiescent || result.Quiescent
		resource.released = resource.released || result.Released
		if result.Err != nil {
			resource.cleanupErrors = append(resource.cleanupErrors,
				ErrCleanup.New(location(resource.info.Scope, resource.info.Configuration, "release"), result.Err))
		}
		complete := resource.quiescent && resource.released
		if !complete {
			resource.next = result.Continue
		}
		resource.mu.Unlock()
		if !complete {
			break
		}
	}
	return assembly.closeError(interrupted)
}

func (assembly *Assembly) closeError(interrupted error) error {
	report := assembly.Snapshot()
	var causes []error
	if interrupted != nil {
		causes = append(causes, interrupted)
	}
	pending := false
	for _, status := range report.Sources {
		if status.Ownership == Owned || status.Ownership == Delegated {
			causes = append(causes, status.CleanupErrors...)
		}
		pending = pending || status.Pending
	}
	location := fault.Context{Operation: "close", Scope: assembly.scope}
	if pending {
		return ErrIncomplete.New(location, causes...)
	}
	if len(causes) != 0 {
		return ErrCleanup.New(location, causes...)
	}
	return nil
}
