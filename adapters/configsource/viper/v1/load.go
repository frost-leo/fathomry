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

package viper

import (
	"context"
	"io"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/configsource/viper/v1"
)

// Evidence contains bounded non-payload facts. Lifecycle records resolve at
// actual subscription cleanup. Errors remain in the independent common receipt.
type Evidence struct {
	Documents int
	Missing   bool
}

// Dependencies are borrowed once. The caller owns runtime shutdown and evidence
// reception; retrying a failed receiver never repeats the native operation.
type Dependencies struct {
	Runtime  *adapters.Runtime
	Evidence *adapters.Inbox[Evidence]
	Observer *adapters.Observer
}

// Client is a non-owning concurrent-safe capability entry. It owns no transport
// and cannot close the common runtime. Copies share the same binding.
type Client struct {
	private
	endpoint adapters.Endpoint[Evidence]
}

// New performs no source I/O or implicit global lookup.
func New(dependencies Dependencies) (*Client, error) {
	endpoint, err := adapters.Bind(dependencies.Runtime, adapters.Declaration[Evidence]{Evidence: dependencies.Evidence, Observer: dependencies.Observer, Copy: func(value Evidence) Evidence { return value }})
	if err != nil {
		return nil, err
	}
	return &Client{endpoint: endpoint}, nil
}

// Input selects exactly one literal absolute File or borrowed Reader. Load never
// closes Readers or forcibly interrupts blocked Read/Open/Stat/Close calls.
type Input struct {
	private
	Settings Settings
	File     string
	Reader   io.Reader
}

// Load returns the whole ordered batch or nil. Original bytes, native queries
// and live environment remain distinct; this is not application preparation.
func (client *Client) Load(ctx context.Context, inputs []Input) ([]*Document, error) {
	var documents []*Document
	err := client.run(ctx, "load", func(ctx context.Context) (Evidence, error) {
		if len(inputs) == 0 || len(inputs) > MaxSources {
			return Evidence{}, fail(ErrInput, "load")
		}
		selected := make([]native.LoadInput, len(inputs))
		for index, input := range inputs {
			options, err := options(input.Settings)
			if err != nil {
				return Evidence{}, err
			}
			selected[index] = native.LoadInput{Options: options, File: input.File, Reader: input.Reader}
		}
		result, err := native.Load(ctx, selected)
		if err != nil {
			return Evidence{}, translate(err, "load")
		}
		documents = make([]*Document, len(result))
		for index, value := range result {
			documents[index] = &Document{native: value}
		}
		return Evidence{Documents: len(documents)}, nil
	})
	if err != nil {
		return nil, err
	}
	return documents, nil
}

// RawFile returns original bytes and positive absence separately. Present-empty
// content is success. limit is 0..MaxDocumentBytes; zero permits only empty files.
// It neither decodes syntax nor applies required/optional application policy.
func (client *Client) RawFile(ctx context.Context, path string, limit int) ([]byte, bool, error) {
	var raw []byte
	var missing bool
	err := client.run(ctx, "raw_file", func(ctx context.Context) (Evidence, error) {
		var err error
		raw, missing, err = native.RawFile(ctx, path, limit)
		return Evidence{Documents: 1, Missing: missing}, translate(err, "raw_file")
	})
	if err != nil {
		return nil, false, err
	}
	return raw, missing, nil
}
func (client *Client) run(ctx context.Context, operation string, work func(context.Context) (Evidence, error)) error {
	if client == nil {
		return fail(ErrInput, operation)
	}
	receipt, err := client.endpoint.Run(ctx, request(operation), func(call *adapters.Call[Evidence]) {
		facts, err := work(call.Context())
		_ = call.Resolve(adapters.Outcome[Evidence]{Value: facts, Present: true, Primary: err})
	})
	if err != nil {
		return err
	}
	snapshot, _ := receipt.Snapshot()
	return snapshot.Err()
}
func request(operation string) adapters.Request {
	return adapters.Request{Operation: "config.viper." + operation, WorkBytes: 8 << 20, EvidenceBytes: 64 << 10}
}
