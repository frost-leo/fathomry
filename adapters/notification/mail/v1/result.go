/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package mail

import (
	"github.com/frost-leo/fathomry/adapters/notification/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/notification/mail/v0"
	source "github.com/frost-leo/fathomry/internal/resource"
	"slices"
)

// Result is immutable process-local evidence, not a durable delivery record.
type Result struct {
	private
	native      native.Result
	present     bool
	source      notification.Info
	attribution notification.Attribution
	attempts    notification.Attempts
}

func project(value invocation.Result[native.Result], metadata adapters.Info) Result {
	return Result{native: value.Outcome.Value, present: value.Outcome.Present, source: info(value.Source),
		attribution: notification.Attribution{Runtime: metadata.Runtime, Operation: metadata.Operation, ID: metadata.ID, Sequence: metadata.Sequence, Parent: metadata.Parent, Depth: metadata.Depth, Source: metadata.Source},
		attempts:    notification.Attempts{Observed: value.Attempts.Observed, Exact: value.Attempts.Exact}}
}
func info(value source.Info) notification.Info {
	result := notification.Info{Scope: value.Scope, Provider: value.Configuration.Identity.Provider, Name: value.Configuration.Identity.Name, Revision: value.Configuration.Revision, FormatVersion: value.Configuration.Format}
	for _, layer := range value.Configuration.Provenance {
		result.Provenance = append(result.Provenance, notification.LayerInfo{Kind: uint8(layer.Kind), Fields: slices.Clone(layer.Fields)})
	}
	return result
}
func (owner *Owner) Info() notification.Info { return owner.Handle().Info() }
func (handle Handle) Info() notification.Info {
	if handle.state == nil {
		return notification.Info{}
	}
	return info(handle.state.info)
}
func (value Result) HasData() bool                         { return value.present }
func (value Result) Source() notification.Info             { return value.source.Clone() }
func (value Result) Attribution() notification.Attribution { return value.attribution }
func (value Result) Attempts() notification.Attempts       { return value.attempts }
func (value Result) BytesWritten() int64                   { return value.native.BytesWritten() }
func (value Result) PreparedMessage() (Message, bool) {
	message, ok := value.native.PreparedMessage()
	return Message{native: message}, ok
}

// ResponseErrors preserves native SMTP failures, including successful declared
// response recovery. Ordinary error formatting is safe; cause inspection is not.
func (value Result) ResponseErrors() []error {
	values := value.native.ResponseErrors()
	for index := range values {
		values[index] = translate(values[index], "smtp-response")
	}
	return values
}

func (value Result) ConnectionErrors() []error {
	values := value.native.ConnectionErrors()
	for index := range values {
		values[index] = translate(values[index], "connection")
	}
	return values
}

type Effect uint8

const (
	NotAttempted Effect = iota
	NotAccepted
	Unknown
	Accepted
)

type Stage string

const (
	StageCompose   Stage = "compose"
	StageConnect   Stage = "connect"
	StageMail      Stage = "mail"
	StageRecipient Stage = "recipient"
	StageData      Stage = "data"
	StageBody      Stage = "body"
	StageAck       Stage = "ack"
)

// Delivery preserves the original position and independent SMTP stages. Accepted
// means final relay 250 only; cleanup failures never undo that fact.
type Delivery struct {
	private
	native native.Delivery
}

func (value Delivery) ID() string           { return value.native.ID() }
func (value Delivery) Sender() string       { return value.native.Sender() }
func (value Delivery) Effect() Effect       { return Effect(value.native.Effect()) }
func (value Delivery) Stage() Stage         { return Stage(value.native.Stage()) }
func (value Delivery) Code() int            { return value.native.Code() }
func (value Delivery) EnhancedCode() string { return value.native.EnhancedCode() }
func (value Delivery) Err() error           { return translate(value.native.Err(), "send") }

// ServerResponse deliberately exposes the bounded, sensitive successful DATA
// reply (for example a relay queue ID), never a delivery or recipient-read proof.
// It is empty without a successful DATA reply, but a successful reply may also
// have empty text. Use Effect/Code for acceptance. Subsequent cleanup retains it.
func (value Delivery) ServerResponse() string { return value.native.ServerResponse() }

func (value Delivery) Recipients() []Recipient {
	values := value.native.Recipients()
	result := make([]Recipient, len(values))
	for i, item := range values {
		result[i] = Recipient{native: item}
	}
	return result
}

// Recipient.Accepted is RCPT acceptance, not DATA completion or human delivery.
type Recipient struct {
	private
	native native.Recipient
}

func (value Recipient) Address() string      { return value.native.Address() }
func (value Recipient) Attempted() bool      { return value.native.Attempted() }
func (value Recipient) Accepted() bool       { return value.native.Accepted() }
func (value Recipient) Code() int            { return value.native.Code() }
func (value Recipient) EnhancedCode() string { return value.native.EnhancedCode() }
func (value Result) Messages() []Delivery {
	values := value.native.Messages()
	if values == nil {
		return nil
	}
	result := make([]Delivery, len(values))
	for i, item := range values {
		result[i] = Delivery{native: item}
	}
	return result
}

type SessionInfo struct {
	private
	Network, Authentication, Greeting string
	TLS                               bool
	TLSVersion, CipherSuite           uint16
	Port, ConnectionAttempts          int
	Extensions                        []Extension
}
type Extension struct {
	private
	Name, Value string
	Supported   bool
}

func (value Result) SessionInfo() SessionInfo {
	input := value.native.SessionInfo()
	result := SessionInfo{Network: input.Network, Authentication: input.Authentication, Greeting: input.Greeting, TLS: input.TLS, TLSVersion: input.TLSVersion, CipherSuite: input.CipherSuite, Port: input.Port, ConnectionAttempts: input.ConnectionAttempts}
	for _, item := range input.Extensions {
		result.Extensions = append(result.Extensions, Extension{Name: item.Name, Value: item.Value, Supported: item.Supported})
	}
	return result
}

type HeaderChange struct {
	private
	Name                    string
	Dropped, Added, Changed bool
}

func (value Result) HeaderChanges() []HeaderChange {
	input := value.native.HeaderChanges()
	if input == nil {
		return nil
	}
	result := make([]HeaderChange, len(input))
	for i, item := range input {
		result[i] = HeaderChange{Name: item.Name, Dropped: item.Dropped, Added: item.Added, Changed: item.Changed}
	}
	return result
}

// ProcessInfo is local process evidence. InputBytes counts successful stdin writes,
// not bytes consumed. Started or nonzero exit may coexist with external effects.
type ProcessInfo struct {
	private
	Started, ExitKnown, StderrTruncated bool
	ExitCode                            int
	InputBytes                          int64
	stderr                              []byte
}

func (value ProcessInfo) StderrCopy() []byte { return slices.Clone(value.stderr) }
func (value Result) ProcessInfo() ProcessInfo {
	input := value.native.ProcessInfo()
	return ProcessInfo{Started: input.Started, ExitKnown: input.ExitKnown, StderrTruncated: input.StderrTruncated, ExitCode: input.ExitCode, InputBytes: input.InputBytes, stderr: input.StderrCopy()}
}
func (value Result) Observations() []notification.Observation {
	var result []notification.Observation
	for _, delivery := range value.native.Messages() {
		if delivery.Effect() == native.Accepted {
			result = append(result, notification.RelayAccepted)
			break
		}
	}
	process := value.native.ProcessInfo()
	if process.Started && process.ExitKnown && process.ExitCode == 0 {
		result = append(result, notification.LocalProcessSucceeded)
	}
	return result
}
