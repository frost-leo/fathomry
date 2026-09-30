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
	native "github.com/frost-leo/fathomry/internal/configsource/nacos/v2"
)

// MutationState reports native effect evidence, never retry permission or rollback.
type MutationState uint8

const (
	MutationNotIssued    MutationState = 0
	MutationUnknown      MutationState = 1
	MutationAcknowledged MutationState = 2
	MutationRejected     MutationState = 3
)

// MutationResult remains meaningful alongside an error, including acknowledgement
// followed by a deadline. Zero means no mutation dispatch was observed.
type MutationResult struct {
	private
	state MutationState
}

func (value MutationResult) State() MutationState { return value.state }

// PublishInput carries native write/CAS metadata. Content is nonempty UTF-8 up to
// one MiB. Empty CASMD5 means unconditional, not create-only. Permission remains
// subject to both Settings.Writable/key selection and service ACLs.
type PublishInput struct {
	private
	Key         Key
	Content     string
	ContentType string
	CASMD5      string
	Tag         string
	ConfigTags  string
	AppName     string
	SourceUser  string
	BetaIPs     string
}

// Publish never retries a dispatched mutation. Acknowledgement does not certify
// immediate visibility from every read replica or configuration cache.
func (client *Client) Publish(ctx context.Context, input PublishInput) (MutationResult, error) {
	var result MutationResult
	err := client.run(ctx, "publish", func(ctx context.Context, selected *native.Client) (Evidence, error) {
		value, err := selected.Publish(ctx, native.PublishInputV1{Key: nativeKey(input.Key), Content: input.Content, ContentType: input.ContentType, CASMD5: input.CASMD5, Tag: input.Tag, ConfigTags: input.ConfigTags, AppName: input.AppName, SourceUser: input.SourceUser, BetaIPs: input.BetaIPs})
		result = mutation(value)
		return Evidence{FailedIndex: -1, MutationPresent: true, Mutation: result}, err
	})
	return result, err
}

// Delete performs one permitted native removal and preserves its dispatch result.
func (client *Client) Delete(ctx context.Context, key Key) (MutationResult, error) {
	var result MutationResult
	err := client.run(ctx, "delete", func(ctx context.Context, selected *native.Client) (Evidence, error) {
		value, err := selected.Delete(ctx, nativeKey(key))
		result = mutation(value)
		return Evidence{FailedIndex: -1, MutationPresent: true, Mutation: result}, err
	})
	return result, err
}
func mutation(value native.MutationResult) MutationResult {
	state := MutationUnknown
	switch value.State() {
	case native.MutationNotIssued:
		state = MutationNotIssued
	case native.MutationUnknown:
		state = MutationUnknown
	case native.MutationAcknowledged:
		state = MutationAcknowledged
	case native.MutationRejected:
		state = MutationRejected
	}
	return MutationResult{state: state}
}
