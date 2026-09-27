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

package nacos

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/frost-leo/fathomry/adapters/configsource/internal/owned"
	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	native "github.com/frost-leo/fathomry/internal/configsource/nacos/v2"
)

// Server explicitly selects both channels of one authorized member; no discovery
// or port offset is inferred. Settings are sensitive ordinary serializable DTOs.
type Server struct {
	HTTPURL     string `json:"http_url"`
	GRPCAddress string `json:"grpc_address"`
}

// Document selects a sensitive native key under a non-secret logical slot Name.
// Empty Group selects DEFAULT_GROUP. Native names are not diagnostic labels.
type Document struct {
	Name   string `json:"name"`
	Group  string `json:"group"`
	DataID string `json:"data_id"`
}

// Settings is plain bootstrap data, suitable inside a project configuration DTO.
// Durations are nanoseconds. Zero request/retry/reconcile values retain the private
// profile defaults: 10s/100ms/30s. TLS is verified unless AllowInsecure explicitly
// selects plaintext for an isolated target. Username/password are paired.
type Settings struct {
	Name              string        `json:"name"`
	Namespace         string        `json:"namespace"`
	AppName           string        `json:"app_name"`
	Servers           []Server      `json:"servers"`
	Documents         []Document    `json:"documents"`
	Username          string        `json:"username"`
	Password          string        `json:"password"`
	RootCAPEM         string        `json:"root_ca_pem"`
	AllowInsecure     bool          `json:"allow_insecure"`
	RequestTimeout    time.Duration `json:"request_timeout"`
	RetryDelay        time.Duration `json:"retry_delay"`
	ReconcileInterval time.Duration `json:"reconcile_interval"`
}
type selection struct {
	owned.Guard
	options native.OptionsV1
	slots   []string
}

// Select validates/copies bootstrap without constructing a native Client or
// reading environment, opening files, discovering endpoints or contacting servers.
func Select(input Settings) (source.Selection, error) {
	if len(input.Documents) < 1 || len(input.Documents) > source.MaxDocuments || len(input.Servers) < 1 || len(input.Servers) > native.MaxServers {
		return nil, owned.Fail(ErrSettings)
	}
	options := native.OptionsV1{Name: input.Name, Namespace: input.Namespace, AppName: input.AppName, Username: input.Username, Password: input.Password,
		RootCAPEM: input.RootCAPEM, AllowInsecure: input.AllowInsecure, RequestTimeout: input.RequestTimeout, RetryDelay: input.RetryDelay, ReconcileInterval: input.ReconcileInterval}
	seen := make(map[string]bool, len(input.Documents))
	for _, document := range input.Documents {
		if !owned.Label(document.Name) || seen[document.Name] {
			return nil, owned.Fail(ErrSettings)
		}
		seen[document.Name] = true
		options.Keys = append(options.Keys, native.KeyV1{Group: document.Group, DataID: document.DataID})
	}
	for _, server := range input.Servers {
		options.Servers = append(options.Servers, native.ServerV1{HTTPURL: server.HTTPURL, GRPCAddress: server.GRPCAddress})
	}
	if err := native.ValidateOptions(options); err != nil {
		return nil, owned.Fail(ErrSettings)
	}
	options.Name = strings.Clone(options.Name)
	options.Namespace = strings.Clone(options.Namespace)
	options.AppName = strings.Clone(options.AppName)
	options.Username = strings.Clone(options.Username)
	options.Password = strings.Clone(options.Password)
	options.RootCAPEM = strings.Clone(options.RootCAPEM)
	for index := range options.Keys {
		options.Keys[index].Group = strings.Clone(options.Keys[index].Group)
		options.Keys[index].DataID = strings.Clone(options.Keys[index].DataID)
	}
	for index := range options.Servers {
		options.Servers[index].HTTPURL = strings.Clone(options.Servers[index].HTTPURL)
		options.Servers[index].GRPCAddress = strings.Clone(options.Servers[index].GRPCAddress)
	}
	selected := &selection{options: options}
	for _, document := range input.Documents {
		selected.slots = append(selected.slots, strings.Clone(document.Name))
	}
	return selected, nil
}
func (selected *selection) Description() (source.Description, error) {
	if selected == nil {
		return source.Description{}, owned.Fail(source.ErrValue)
	}
	return source.Description{Name: selected.options.Name, Module: ModuleID, Documents: append([]string(nil), selected.slots...), Observable: true}, nil
}
func (selected *selection) acquisition(condition failure.Condition, phase source.Phase, document int, causes ...error) error {
	info := source.AcquisitionInfo{Source: selected.options.Name, Phase: phase}
	if document >= 0 && document < len(selected.slots) {
		info.Document = selected.slots[document]
	}
	return owned.Acquisition(condition, info, causes...)
}

func (selected *selection) batch(documents []*native.Document, phase source.Phase) (source.Batch, error) {
	if len(documents) != len(selected.slots) {
		return nil, selected.acquisition(ErrProtocol, phase, -1)
	}
	entries := make([]owned.Entry, 0, len(documents))
	remaining := source.MaxBatchBytes
	for index, document := range documents {
		if document == nil {
			return nil, selected.acquisition(ErrProtocol, phase, index)
		}
		presence := source.Present
		if document.Missing() {
			presence = source.Missing
		}
		raw := document.RawCopy()
		if len(raw) > remaining {
			return nil, selected.acquisition(ErrLimit, phase, index)
		}
		remaining -= len(raw)
		entries = append(entries, owned.Entry{Name: selected.slots[index], Presence: presence, Raw: raw})
	}
	return owned.NewBatch(entries)
}
func (selected *selection) Capture(ctx context.Context) (source.Batch, error) {
	if selected == nil || owned.Nil(ctx) {
		return nil, owned.Fail(source.ErrValue)
	}
	nativeContext := owned.NativeContext(ctx)
	client, err := native.Open(nativeContext, selected.options)
	if err != nil {
		return nil, selected.mapError(err, ctx, source.CapturePhase, -1)
	}
	documents, failedDocument, readErr := client.ReadRawAll(nativeContext)
	// Finite cleanup remains joined even after the read context expires. A caller
	// deadline is cooperative; no transient owner is abandoned to claim promptness.
	cleanup := context.Background()
	closeErr := client.Close(cleanup)
	primary := selected.mapError(readErr, ctx, source.CapturePhase, failedDocument)
	if primary == nil && ctx.Err() != nil {
		primary = selected.acquisition(ErrRead, source.CapturePhase, -1, ctx.Err(), context.Cause(ctx))
	}
	if primary != nil || closeErr != nil {
		return nil, selected.combine(primary, selected.mapError(closeErr, cleanup, source.ClosePhase, -1))
	}
	return selected.batch(documents, source.CapturePhase)
}
func (selected *selection) Observe(ctx context.Context) (source.Observer, error) {
	if selected == nil || owned.Nil(ctx) {
		return nil, owned.Fail(source.ErrValue)
	}
	return owned.Observe(ctx, func(lifetime context.Context, publish func(source.Batch, error)) error {
		nativeContext := owned.NativeContext(lifetime)
		client, err := native.Open(nativeContext, selected.options)
		if err != nil {
			publish(nil, selected.mapError(err, lifetime, source.ObservePhase, -1))
			return nil
		}
		subscription, err := client.ObserveRaw(nativeContext, func(documents []*native.Document, failedDocument int, err error) {
			if err != nil {
				publish(nil, selected.mapError(err, lifetime, source.ObservePhase, failedDocument))
				return
			}
			batch, err := selected.batch(documents, source.ObservePhase)
			publish(batch, err)
		})
		if err != nil {
			publish(nil, selected.mapError(err, lifetime, source.ObservePhase, -1))
		} else {
			<-lifetime.Done()
			// Native observation is joined before its Client. These waits are not the
			// caller's Close wait; expiration never discards cleanup responsibility.
			_ = subscription.Close(context.Background())
		}
		cleanup := context.Background()
		return selected.mapError(client.Close(cleanup), cleanup, source.ClosePhase, -1)
	}), nil
}
func (selected *selection) combine(primary, cleanup error) error {
	if primary == nil {
		return cleanup
	}
	if cleanup == nil {
		return primary
	}
	return selected.acquisition(ErrRead, source.CapturePhase, -1, primary, cleanup)
}
func (selected *selection) mapError(err error, ctx context.Context, phase source.Phase, document int) error {
	if err == nil {
		return nil
	}
	condition := ErrRead
	switch {
	case phase == source.ClosePhase:
		condition = ErrClose
	case errors.Is(err, native.ErrDenied):
		condition = ErrDenied
	case errors.Is(err, native.ErrLimit):
		condition = ErrLimit
	case errors.Is(err, native.ErrDecode), errors.Is(err, native.ErrUnsupported):
		condition = ErrProtocol
	}
	var causes []error
	if errors.Is(err, context.Canceled) {
		causes = append(causes, context.Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		causes = append(causes, context.DeadlineExceeded)
	}
	if ctx.Err() != nil {
		causes = append(causes, ctx.Err(), context.Cause(ctx))
	}
	return selected.acquisition(condition, phase, document, causes...)
}
