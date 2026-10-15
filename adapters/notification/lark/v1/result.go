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

package lark

import (
	"github.com/frost-leo/fathomry/adapters/notification/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/notification/lark/v3"
	source "github.com/frost-leo/fathomry/internal/resource"
	"net/http"
	"slices"
)

// Result is immutable process-local evidence, not a durable delivery record.
type Result struct {
	private
	native            native.Result
	present           bool
	source            notification.Info
	attribution       notification.Attribution
	attempts          notification.Attempts
	socket            *native.WebSocketResult
	http              HTTPWrite
	unforwarded       []invocation.Result[native.WebSocketResult]
	unforwardedEvents []UnforwardedEvent
	technicalID       string
	primary, cleanup  error
}

func project(value invocation.Result[native.Result], metadata adapters.Info) Result {
	return Result{native: value.Outcome.Value, present: value.Outcome.Present, source: info(value.Source), technicalID: value.Context.Correlation.Call,
		attribution: attribution(metadata), attempts: notification.Attempts{Observed: value.Attempts.Observed, Exact: value.Attempts.Exact}, primary: translate(value.Outcome.Primary, "operation"), cleanup: translate(value.Outcome.Cleanup, "cleanup")}
}

// Err and Cleanup retain native failures for explicitly inspected embedded facts,
// including unforwarded ACKs on a saturated listener. Neither prescribes retry.
func (value Result) Err() error { return value.primary }

// TechnicalID identifies the original managed native call, including fallback
// facts that share their listener's public record rather than inventing a child.
func (value Result) TechnicalID() string { return value.technicalID }
func (value Result) Cleanup() error      { return value.cleanup }
func attribution(value adapters.Info) notification.Attribution {
	return notification.Attribution{Runtime: value.Runtime, Operation: value.Operation, ID: value.ID, Sequence: value.Sequence, Parent: value.Parent, Depth: value.Depth, Source: value.Source}
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

// Effect describes the exact requested operation, not human delivery or rendering.
type Effect uint8

const (
	NotAttempted Effect = iota
	Unknown
	Rejected
	Accepted
	Partial
)

func (value Result) Effect() Effect { return Effect(value.native.Effect()) }

type Exchange struct {
	private
	native native.Exchange
}

func (value Result) Exchange() Exchange     { return Exchange{native: value.native.Exchange()} }
func (value Exchange) HTTPStatus() int      { return value.native.HTTPStatus() }
func (value Exchange) APICode() (int, bool) { return value.native.APICode() }
func (value Exchange) RequestID() string    { return value.native.RequestID() }
func (value Exchange) RetryAfter() string   { return value.native.RetryAfter() }

// AuthenticationStage is auxiliary traffic, never a notification attempt.
type AuthenticationStage struct {
	private
	native native.AuthenticationStage
}

func (value AuthenticationStage) Name() string    { return value.native.Name() }
func (value AuthenticationStage) Attempted() bool { return value.native.Attempted() }
func (value AuthenticationStage) Exchange() Exchange {
	return Exchange{native: value.native.Exchange()}
}
func (value AuthenticationStage) Err() error     { return translate(value.native.Err(), "authenticate") }
func (value AuthenticationStage) Cleanup() error { return translate(value.native.Cleanup(), "cleanup") }
func (value AuthenticationStage) Effect() Effect { return Effect(value.native.Effect()) }
func (value Result) AuthenticationStages() []AuthenticationStage {
	input := value.native.AuthenticationStages()
	if input == nil {
		return nil
	}
	result := make([]AuthenticationStage, len(input))
	for index, item := range input {
		result[index] = AuthenticationStage{native: item}
	}
	return result
}
func (value Result) Authentication() (Exchange, bool) {
	exchange, ok := value.native.Authentication()
	return Exchange{native: exchange}, ok
}
func (value Result) Target() string                      { return value.native.Target() }
func (value Result) UUID() string                        { return value.native.UUID() }
func (value Result) MessageID() string                   { return value.native.MessageID() }
func (value Result) CardID() string                      { return value.native.CardID() }
func (value Result) AssetKey() string                    { return value.native.AssetKey() }
func (value Result) JSONData() []byte                    { return value.native.JSONData() }
func (value Result) ResponseJSON() []byte                { return value.native.ResponseJSON() }
func (value Result) Bytes() []byte                       { return value.native.Bytes() }
func (value Result) ContentType() string                 { return value.native.ContentType() }
func (value Result) Headers() http.Header                { return value.native.Headers() }
func (value Result) FileName() string                    { return value.native.FileName() }
func (value Result) RelatedIDs() []string                { return value.native.RelatedIDs() }
func (value Result) InvalidMessageIDs() []string         { return value.native.InvalidMessageIDs() }
func (value Result) InvalidUserIDs() []string            { return value.native.InvalidUserIDs() }
func (value Result) InvalidTargets() map[string][]string { return value.native.InvalidTargets() }
func (value Result) PartialFields() map[string][]byte    { return value.native.PartialFields() }
func (value Result) NextPage() (string, bool)            { return value.native.NextPage() }

type MessageInfo struct {
	private
	native native.MessageInfo
}

func (value MessageInfo) ID() string            { return value.native.ID() }
func (value MessageInfo) ChatID() string        { return value.native.ChatID() }
func (value MessageInfo) Type() string          { return value.native.Type() }
func (value MessageInfo) Content() string       { return value.native.Content() }
func (value MessageInfo) SenderID() string      { return value.native.SenderID() }
func (value MessageInfo) Deleted() (bool, bool) { return value.native.Deleted() }
func (value MessageInfo) Updated() (bool, bool) { return value.native.Updated() }
func (value Result) Messages() []MessageInfo {
	input := value.native.Messages()
	if input == nil {
		return nil
	}
	result := make([]MessageInfo, len(input))
	for index, item := range input {
		result[index] = MessageInfo{native: item}
	}
	return result
}
func (value Result) ResolvedRecipient() (Recipient, bool) {
	input, ok := value.native.ResolvedRecipient()
	return Recipient{Type: input.Type, ID: input.ID}, ok
}

// Event is authenticated data, not a handled event. JSONData is deliberately
// sensitive and may contain callback action tokens or an app ticket.
type Event struct {
	private
	native native.Event
}

func (value Event) ID() string         { return value.native.ID() }
func (value Event) Type() string       { return value.native.Type() }
func (value Event) AppID() string      { return value.native.AppID() }
func (value Event) TenantKey() string  { return value.native.TenantKey() }
func (value Event) CreateTime() string { return value.native.CreateTime() }
func (value Event) Challenge() string  { return value.native.Challenge() }
func (value Event) Schema() string     { return value.native.Schema() }
func (value Event) JSONData() []byte   { return value.native.JSONData() }
func (value Result) Event() (Event, bool) {
	event, ok := value.native.Event()
	return Event{native: event}, ok
}
func (value Result) Observations() []notification.Observation {
	var result []notification.Observation
	if _, ok := value.native.Event(); ok {
		result = append(result, notification.CallbackAuthenticated)
	} else if value.native.Effect() == native.Accepted || value.native.Effect() == native.Partial {
		result = append(result, notification.PlatformAcknowledged)
	}
	if value.socket != nil {
		if ack, ok := value.socket.Acknowledgement(); ok && ack.Effect() == native.WebSocketWritten {
			result = append(result, notification.AcknowledgementWritten)
		}
	}
	return result
}
