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

package redis

import (
	"strings"

	"github.com/frost-leo/fathomry/adapters/cache/v1"
	"github.com/frost-leo/fathomry/adapters/internal/errorbridge"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	native "github.com/frost-leo/fathomry/internal/cache/redis/v9"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
)

// Capability is an explicit semantic owner, never inferred from a script body.
type Capability string

const (
	Cache     Capability = "cache"
	Messaging Capability = "messaging"
)

func (value Capability) valid() bool { return value == Cache || value == Messaging }

// Command owns immutable binary-safe arguments and declared capability. Exact
// source grants remain required. Scripts/functions, list queues and extensions
// need explicit semantic selection; Streams/PubSub always require Messaging.
type Command struct {
	private
	native     native.Command
	capability Capability
	valid      bool
}

// NewCommand validates hard construction bounds before copying arguments. Source
// bounds and exact grants are checked again before native dispatch. Empty strings
// and arbitrary binary bytes are preserved. It never sends a command.
func NewCommand(capability Capability, args ...string) (Command, error) {
	if !capability.valid() || len(args) == 0 || len(args) > 65536 || len(args[0]) == 0 || len(args[0]) > 80 {
		return Command{}, problem(capability, ErrInput, "command")
	}
	bytes := 0
	for _, arg := range args {
		if len(arg) > 16<<20-bytes-32 {
			return Command{}, problem(capability, ErrLimit, "command")
		}
		bytes += len(arg) + 32
	}
	const messaging = " XADD XDEL XLEN XRANGE XREVRANGE XREAD XREADGROUP XGROUP XINFO XACK XNACK XCFGSET XPENDING XCLAIM XAUTOCLAIM XTRIM XSETID XACKDEL XDELEX PUBLISH SPUBLISH PUBSUB "
	if strings.Contains(messaging, " "+strings.ToUpper(args[0])+" ") && capability != Messaging {
		return Command{}, problem(capability, ErrInput, "classification")
	}
	return Command{native: native.NewCommand(args...), capability: capability, valid: true}, nil
}

// WithKeyPosition selects explicit native module routing, not another authority.
func (command Command) WithKeyPosition(position int8) Command {
	command.native = command.native.WithKeyPosition(position)
	return command
}
func (command Command) Capability() Capability { return command.capability }

// ReplyState preserves technical evidence independently from value presence.
type ReplyState uint8

const (
	NotEntered ReplyState = iota
	Unknown
	Replied
	CacheOrReply
	TransactionAborted
)

type Reply struct {
	private
	native     native.Reply
	capability Capability
	err        error
}

func (reply Reply) State() ReplyState      { return ReplyState(reply.native.State()) }
func (reply Reply) Capability() Capability { return reply.capability }
func (reply Reply) HasValue() bool         { return reply.native.HasValue() }
func (reply Reply) Value() Value {
	return Value{native: reply.native.Value(), valid: reply.native.HasValue(), capability: reply.capability}
}
func (reply Reply) Err() error { return reply.err }

// ValueKind has an explicit unavailable zero, distinct from decoded Redis null.
type ValueKind uint8

const (
	Unavailable ValueKind = iota
	Null
	Text
	Integer
	Double
	Boolean
	Array
	Map
	ErrorValue
	BigInteger
)

type Value struct {
	private
	native     native.Value
	valid      bool
	capability Capability
}
type Pair struct {
	private
	key, value Value
}

func (pair Pair) Key() Value   { return pair.key }
func (pair Pair) Value() Value { return pair.value }
func (value Value) Kind() ValueKind {
	if !value.valid {
		return Unavailable
	}
	return ValueKind(value.native.Kind()) + 1
}
func (value Value) Text() (string, bool) {
	text, ok := value.native.Text()
	return text, value.valid && ok
}
func (value Value) Bytes() ([]byte, bool) {
	if !value.valid {
		return nil, false
	}
	return value.native.Bytes()
}
func (value Value) Integer() (int64, bool) {
	number, ok := value.native.Integer()
	return number, value.valid && ok
}
func (value Value) Double() (float64, bool) {
	number, ok := value.native.Double()
	return number, value.valid && ok
}
func (value Value) Boolean() (bool, bool) {
	flag, ok := value.native.Boolean()
	return flag, value.valid && ok
}
func (value Value) BigInteger() (string, bool) {
	text, ok := value.native.BigInteger()
	return text, value.valid && ok
}
func (value Value) Err() error { return translate(value.native.Err(), "reply", value.capability) }
func (value Value) Elements() []Value {
	elements := value.native.Elements()
	if elements == nil {
		return nil
	}
	result := make([]Value, len(elements))
	for index, item := range elements {
		result[index] = Value{native: item, valid: true, capability: value.capability}
	}
	return result
}
func (value Value) Pairs() []Pair {
	pairs := value.native.Pairs()
	if pairs == nil {
		return nil
	}
	result := make([]Pair, len(pairs))
	for index, item := range pairs {
		result[index] = Pair{key: Value{native: item.Key(), valid: true, capability: value.capability}, value: Value{native: item.Value(), valid: true, capability: value.capability}}
	}
	return result
}

// ResultKind prevents local session cleanup from masquerading as command success.
type ResultKind uint8

const (
	NoResult ResultKind = iota
	Commands
	Lifecycle
	SubscriptionEvent
)

// Result is immutable and concurrent-readable. Replies and nested getters return
// detached outer storage. Errors preserve original causes for sensitive deliberate
// inspection. Neither a successful lifecycle nor a reply proves business delivery.
type Result struct {
	private
	kind             ResultKind
	capability       Capability
	replies          []Reply
	source           cache.Info
	attribution      cache.Attribution
	attempts         cache.Attempts
	nativePresent    bool
	primary, cleanup error
	aggregate        error
}

func (result Result) Kind() ResultKind               { return result.kind }
func (result Result) Capability() Capability         { return result.capability }
func (result Result) Replies() []Reply               { return append([]Reply(nil), result.replies...) }
func (result Result) Source() cache.Info             { return result.source }
func (result Result) Attribution() cache.Attribution { return result.attribution }
func (result Result) Attempts() cache.Attempts       { return result.attempts }

// NativePresent is the provider's outcome-presence flag, not value presence or
// effect certainty. For lifecycle results it denotes acquisition, not server ack.
func (result Result) NativePresent() bool { return result.nativePresent }
func (result Result) Primary() error      { return result.primary }
func (result Result) Cleanup() error      { return result.cleanup }

// AggregateError reports dispatch/EXEC causes not already represented by a
// positional reply (for example EXECABORT after queue errors). Nil is not a
// statement about transaction effects or retry safety.
func (result Result) AggregateError() error { return result.aggregate }
func (result Result) Err() error            { return combine(result.primary, result.cleanup) }

func sourceInfo(value source.Info) cache.Info {
	return cache.Info{Scope: value.Scope, Provider: value.Configuration.Identity.Provider, Name: value.Configuration.Identity.Name,
		Revision: value.Configuration.Revision, FormatVersion: value.Configuration.Format}
}
func publicAttribution(info adapters.Info) cache.Attribution {
	return cache.Attribution{Runtime: info.Runtime, Operation: info.Operation, ID: info.ID, Sequence: info.Sequence,
		Parent: info.Parent, Depth: info.Depth, Source: info.Source}
}
func project(value invocation.Result[native.Result], info adapters.Info, capability Capability, kind ResultKind, commands []Command) Result {
	result := Result{kind: kind, capability: capability, source: sourceInfo(value.Source),
		attribution: publicAttribution(info),
		attempts:    cache.Attempts{Observed: value.Attempts.Observed, Exact: value.Attempts.Exact}, nativePresent: value.Outcome.Present}
	for index, reply := range value.Outcome.Value.Commands() {
		owner := capability
		if index < len(commands) {
			owner = commands[index].capability
		}
		result.replies = append(result.replies, Reply{native: reply, capability: owner, err: translate(reply.Err(), "command", owner)})
	}
	// The native batch error normally repeats its first failed command. Retain
	// every positional command owner instead of relabeling the first cause cache.
	causes := make([]error, 0, len(result.replies)+1)
	for _, reply := range result.replies {
		causes = append(causes, reply.err)
	}
	result.aggregate = aggregateCause(value.Outcome.Primary, value.Outcome.Value.Commands(), capability)
	causes = append(causes, result.aggregate)
	result.primary = combine(causes...)
	result.cleanup = translate(value.Outcome.Cleanup, "cleanup", capability)
	return result
}

// Native batches normally repeat the first command error, but can additionally
// report EXECABORT or a transport/dispatch failure. Remove only causes already
// represented by a positional reply; never discard independent aggregate facts.
func aggregateCause(primary error, replies []native.Reply, capability Capability) error {
	if len(replies) == 0 {
		return translate(primary, "operation", capability)
	}
	var causes []error
	remaining := 8192
	retained := false
	retainRemainder := func() {
		if !retained {
			retained = true
			causes = append(causes, translate(primary, "dispatch", capability))
		}
	}
	var visit func(error)
	visit = func(err error) {
		if err == nil {
			return
		}
		if remaining == 0 {
			retainRemainder()
			return
		}
		remaining--
		for _, reply := range replies {
			if errorbridge.Contains(reply.Err(), err, 128) {
				return
			}
		}
		if _, ok := failure.Inspect(err); ok {
			causes = append(causes, err)
			return
		}
		if internal, ok := err.(*fault.Error); ok {
			kind := internal.Diagnostic().Kind
			if kind != invocation.ErrFailed && kind != native.ErrCommand {
				causes = append(causes, translate(err, "dispatch", capability))
				return
			}
		}
		if wrapped, ok := err.(interface{ Unwrap() []error }); ok {
			children := wrapped.Unwrap()
			if len(children) > 0 {
				for _, child := range children {
					if remaining == 0 {
						retainRemainder()
						break
					}
					visit(child)
				}
				return
			}
		}
		causes = append(causes, translate(err, "dispatch", capability))
	}
	visit(primary)
	return combine(causes...)
}
