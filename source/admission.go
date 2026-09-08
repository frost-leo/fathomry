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

package source

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/frost-leo/fathomry/failure"
)

var (
	ErrAdmission = failure.MustDefine(failure.Definition{Code: "fathomry.source.admission_failed", Component: "source", Version: 1})
	ErrCapacity  = failure.MustDefine(failure.Definition{Code: "fathomry.source.capacity_exhausted", Component: "source", Version: 1})
)

// Limits are immutable process-local ceilings on one authoritative resource,
// shared across all its aliases. Active counts admitted root uses, not physical
// connections or SDK attempts. Bytes are declared reservations, NOT measured heap,
// SDK buffers, native memory or wire traffic. Providers must bound those separately.
// Queued == 0 rejects overload without waiting. MaxLeases bounds live borrowing
// nodes per admitted root, including retained ancestors; nesting never waits.
// All fields must be positive except Queued and QueuedBytes, which may both be zero.
type Limits struct {
	Active      int
	Queued      int
	Bytes       int64
	QueuedBytes int64
	MaxLeases   int
}

func (limits Limits) valid() bool {
	return limits.Active > 0 && limits.Queued >= 0 && limits.Bytes > 0 &&
		limits.MaxLeases > 0 && (limits.Queued == 0 && limits.QueuedBytes == 0 ||
		limits.Queued > 0 && limits.QueuedBytes > 0)
}

// WithLimits selects an immutable admission policy before assembly. Only owned
// selections accept a policy; borrowed and delegated selections inherit the
// original record's policy and cannot create another copy of its allowance.
// Invalid limits are rejected by Assemble before any factory runs.
func WithLimits[C any](selected Selection[C], limits Limits) Selection[C] {
	if selected.spec == nil {
		return Selection[C]{}
	}
	spec := *selected.spec
	spec.limits = &limits
	return Selection[C]{spec: &spec}
}

// Usage contains current counts only, never completed call history. ActiveBytes
// and QueuedBytes are independently reserved bytes. No SDK attempts are inferred.
type Usage struct {
	Active      int
	Queued      int
	ActiveBytes int64
	QueuedBytes int64
}

type admission struct {
	limits      Limits
	active      int
	bytes       int64
	queue       list.List
	queuedBytes int64
	changed     chan struct{}
}

func (admission *admission) usage() Usage {
	return Usage{Active: admission.active, Queued: admission.queue.Len(),
		ActiveBytes: admission.bytes, QueuedBytes: admission.queuedBytes}
}

func (admission *admission) signal() {
	if admission.changed != nil {
		close(admission.changed)
		admission.changed = make(chan struct{})
	}
}

// Access is a trusted integration's admission capability for a bound scope.
// It exposes no client or shutdown action. A business facade must not expose it
// or the underlying Bind result as a bypass. Copies refer to the same scope.
type Access struct {
	assembly *Assembly
	entry    *entry
}

// AccessFor binds the exact selected token, only when its resource has Limits.
// Existing borrowing scopes may continue after owner shutdown; closing their own
// scope stops their admission. Binding an alias never resets the shared limits.
func AccessFor[C any](assembly *Assembly, selected Selection[C]) (*Access, error) {
	fail := func() (*Access, error) {
		return nil, ErrSelection.New(failure.Attribution{Operation: "access"})
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
		if !resource.admission.limits.valid() || resource.released ||
			entry.spec.mode == Borrowed && !entry.lease ||
			entry.spec.mode != Borrowed && resource.owner != assembly {
			return fail()
		}
		return &Access{assembly: assembly, entry: entry}, nil
	}
	return fail()
}

// Info returns independent source metadata, including the original configuration
// revision rather than a borrowing alias. A zero Access has empty metadata.
func (access *Access) Info() Info {
	if access == nil || access.entry == nil {
		return Info{}
	}
	resource := access.entry.record
	resource.mu.Lock()
	defer resource.mu.Unlock()
	return Info{Scope: resource.info.Scope, Configuration: copyDescription(resource.info.Configuration)}
}

// Limits returns the original immutable admission policy. It is separate from
// the Provider configuration revision and cannot be changed through an alias.
func (access *Access) Limits() Limits {
	if access == nil || access.entry == nil {
		return Limits{}
	}
	resource := access.entry.record
	resource.mu.Lock()
	defer resource.mu.Unlock()
	return resource.admission.limits
}

// Acquire reserves one root use and its declared byte envelope. FIFO overload
// waiting runs in the caller's goroutine and is bounded in both count and bytes.
// Canceled waiters are removed, and cancellation is checked again at admission.
// Success borrows the SAME resource record, not a second lifecycle or SDK client.
// It is the Provider's duty to check the work context immediately before SDK entry.
func (access *Access) Acquire(ctx context.Context, bytes int64) (*Lease, error) {
	if access == nil || access.assembly == nil || access.entry == nil || ctx == nil || bytes < 0 {
		return nil, ErrAdmission.New(failure.Attribution{Operation: "acquire"})
	}
	assembly, entry := access.assembly, access.entry
	resource := entry.record
	location := attribution(resource.info.Scope, resource.info.Configuration, "acquire")
	var queued *list.Element
	for {
		assembly.mu.Lock()
		resource.mu.Lock()
		policy := &resource.admission
		var reason error
		switch {
		case ctx.Err() != nil:
			reason = errors.Join(ctx.Err(), context.Cause(ctx))
		case !assembly.ready || resource.released ||
			entry.spec.mode == Borrowed && !entry.lease ||
			entry.spec.mode != Borrowed && resource.owner != assembly:
			reason = ErrSelection.New(location)
		case !policy.limits.valid() || bytes > policy.limits.Bytes:
			reason = ErrCapacity.New(location)
		}
		canStart := reason == nil && policy.active < policy.limits.Active &&
			bytes <= policy.limits.Bytes-policy.bytes &&
			(policy.queue.Len() == 0 || queued != nil && policy.queue.Front() == queued)
		if reason == nil && !canStart && queued == nil {
			if policy.queue.Len() >= policy.limits.Queued || bytes > policy.limits.QueuedBytes-policy.queuedBytes {
				reason = ErrCapacity.New(location)
			} else {
				queued = policy.queue.PushBack(struct{}{})
				policy.queuedBytes += bytes
			}
		}
		if reason != nil || canStart {
			if queued != nil {
				policy.queue.Remove(queued)
				policy.queuedBytes -= bytes
				policy.signal()
			}
			if canStart {
				policy.active++
				policy.bytes += bytes
				resource.borrowers++
				entry.uses++
				family := &leaseFamily{access: access, bytes: bytes, nodes: 1, limit: policy.limits.MaxLeases}
				node := &leaseNode{family: family, references: 1, done: make(chan struct{})}
				resource.mu.Unlock()
				assembly.mu.Unlock()
				return &Lease{node: node}, nil
			}
			resource.mu.Unlock()
			assembly.mu.Unlock()
			return nil, ErrAdmission.New(location, reason)
		}
		changed := policy.changed
		resource.mu.Unlock()
		assembly.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
		}
	}
}

// Lease is a bounded borrowing tree over ONE admitted root use. Copies refer to
// the same node; Release is idempotent. Retain transfers responsibility without an
// unowned interval or another admission/quota acquisition. Each retained node must
// be released only after its actual users no longer need the resource. Canceling
// a context is not such evidence. SDK cleanup uses an existing lease, not Acquire.
type Lease struct{ node *leaseNode }

type leaseFamily struct {
	mu     sync.Mutex
	access *Access
	bytes  int64
	nodes  int
	limit  int
}

type leaseNode struct {
	family     *leaseFamily
	parent     *leaseNode
	references int
	released   bool
	done       chan struct{}
}

// Retain creates a child obligation on a live lease. It never waits for capacity.
// The entire subtree, including its bytes and SDK work, must fit the root's
// original reservation. This is borrowing, not a grant of new physical capacity.
func (lease *Lease) Retain() (*Lease, error) {
	if lease == nil || lease.node == nil {
		return nil, ErrAdmission.New(failure.Attribution{Operation: "retain"})
	}
	node := lease.node
	family := node.family
	family.mu.Lock()
	defer family.mu.Unlock()
	if node.released {
		return nil, ErrAdmission.New(failure.Attribution{Operation: "retain"})
	}
	if family.nodes >= family.limit {
		return nil, ErrCapacity.New(failure.Attribution{Operation: "retain"})
	}
	family.nodes++
	node.references++
	child := &leaseNode{family: family, parent: node, references: 1, done: make(chan struct{})}
	return &Lease{node: child}, nil
}

// Release confirms this node's own use is over. Children continue to protect the
// resource and all ordered dependencies. It never calls an SDK or closes an owner.
func (lease *Lease) Release() {
	if lease == nil || lease.node == nil {
		return
	}
	node := lease.node
	family := node.family
	family.mu.Lock()
	defer family.mu.Unlock()
	if node.released {
		return
	}
	node.released = true
	for node != nil {
		node.references--
		if node.references != 0 {
			return
		}
		family.nodes--
		if node.parent == nil {
			access := family.access
			access.assembly.mu.Lock()
			resource := access.entry.record
			resource.mu.Lock()
			resource.borrowers--
			resource.admission.active--
			resource.admission.bytes -= family.bytes
			access.entry.uses--
			resource.admission.signal()
			resource.mu.Unlock()
			access.assembly.mu.Unlock()
		}
		close(node.done)
		node = node.parent
	}
}

// Done closes only after this node and every descendant have released. A zero
// lease returns nil (no confirmation), not an already-completed signal.
func (lease *Lease) Done() <-chan struct{} {
	if lease == nil || lease.node == nil {
		return nil
	}
	return lease.node.done
}

func (access Access) Format(state fmt.State, verb rune) {
	_, _ = io.WriteString(state, "source.Access[restricted]")
}
func (lease Lease) Format(state fmt.State, verb rune) {
	_, _ = io.WriteString(state, "source.Lease[restricted]")
}
func (access Access) MarshalJSON() ([]byte, error) {
	return nil, errors.New("source: runtime access serialization is unsupported")
}
func (access *Access) UnmarshalJSON([]byte) error {
	return errors.New("source: runtime access reconstruction is unsupported")
}
func (lease Lease) MarshalJSON() ([]byte, error) {
	return nil, errors.New("source: runtime lease serialization is unsupported")
}
func (lease *Lease) UnmarshalJSON([]byte) error {
	return errors.New("source: runtime lease reconstruction is unsupported")
}
