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
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/internal/invocation"
	request "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_request"
	"google.golang.org/grpc/codes"
)

// MutationState reports only observed dispatch/response evidence, never rollback
// certainty or permission to retry. Unknown includes a lost/malformed response.
type MutationState uint8

const (
	// MutationNotIssued means no mutation RPC was attempted; setup may have run.
	MutationNotIssued MutationState = 0
	// MutationUnknown means attempted, without validated acknowledgement/rejection.
	MutationUnknown MutationState = 1
	// MutationAcknowledged retains success evidence even alongside a later timeout.
	MutationAcknowledged MutationState = 2
	// MutationRejected records a native 301/401/403/409 rejection, not retry policy.
	MutationRejected MutationState = 3
)

// MutationResult stays meaningful on an error, particularly a timeout after send.
type MutationResult struct {
	private
	state MutationState
}

func (result MutationResult) State() MutationState { return result.state }

// PublishInputV1 supplies native publication/CAS metadata. Empty CASMD5 means
// unconditional publication, not create-only. Content is nonempty UTF-8 up to 1 MiB.
// Encryption/cloud plugins are not inferred or installed.
type PublishInputV1 struct {
	private
	Key         KeyV1
	Content     string
	ContentType string
	CASMD5      string
	Tag         string
	ConfigTags  string
	AppName     string
	SourceUser  string
	BetaIPs     string
}

func (client *Client) permits(selected KeyV1) bool {
	if client == nil || client.cancel == nil {
		return false
	}
	value := normalizeKey(selected)
	return identifier(value.Group, 128, false) && identifier(value.DataID, 128, false) &&
		(client.settings.Dynamic || slices.Contains(client.settings.Keys, value))
}
func (client *Client) selectKeys(selected []KeyV1) ([]key, error) {
	if len(selected) == 0 || len(selected) > MaxKeys {
		return nil, fail(ErrInput, "select-keys")
	}
	result := make([]key, 0, len(selected))
	for _, item := range selected {
		value := normalizeKey(item)
		if !client.permits(item) || slices.Contains(result, value) {
			return nil, fail(ErrInput, "select-keys")
		}
		result = append(result, key{strings.Clone(value.Group), strings.Clone(value.DataID)})
	}
	return result, nil
}

// ReadRaw preserves positive missing/empty evidence for one explicitly permitted key.
func (client *Client) ReadRaw(ctx context.Context, selected KeyV1) (*Document, error) {
	keys, err := client.selectKeys([]KeyV1{selected})
	if err != nil {
		return nil, err
	}
	documents, err := client.readMode(ctx, keys, true, nil)
	if err != nil {
		return nil, err
	}
	return documents[0], nil
}

// Publish sends one native request after local permission/admission. It never
// retries or fails over after dispatch; CAS refusal retains the native error code.
// Acknowledgement does not prove every read replica/cache has observed the value.
func (client *Client) Publish(ctx context.Context, input PublishInputV1) (MutationResult, error) {
	if !client.permits(input.Key) || !client.settings.Writable || len(input.Content) == 0 || len(input.Content) > MaxDocumentBytes || !utf8.ValidString(input.Content) {
		return MutationResult{}, fail(ErrInput, "publish")
	}
	for _, item := range []string{input.ContentType, input.Tag, input.ConfigTags, input.AppName, input.SourceUser, input.BetaIPs} {
		if !identifier(item, 512, true) {
			return MutationResult{}, fail(ErrInput, "publish")
		}
	}
	if input.CASMD5 != "" {
		decoded, err := hex.DecodeString(input.CASMD5)
		if err != nil || len(decoded) != 16 {
			return MutationResult{}, fail(ErrInput, "publish-cas")
		}
	}
	selected := normalizeKey(input.Key)
	value := request.NewConfigPublishRequest(selected.Group, selected.DataID, client.settings.Namespace, input.Content, input.CASMD5)
	value.AdditionMap = map[string]string{"type": input.ContentType, "tag": input.Tag, "config_tags": input.ConfigTags, "appName": input.AppName, "src_user": input.SourceUser, "betaIps": input.BetaIPs}
	value.RequestId = strconv.FormatUint(client.sequence.Add(1), 10)
	return client.mutate(ctx, value, "ConfigPublishResponse")
}

// Delete sends one native removal request under explicit write/key permissions.
func (client *Client) Delete(ctx context.Context, selected KeyV1) (MutationResult, error) {
	if !client.permits(selected) || !client.settings.Writable {
		return MutationResult{}, fail(ErrInput, "delete")
	}
	value := normalizeKey(selected)
	query := request.NewConfigRemoveRequest(value.Group, value.DataID, client.settings.Namespace)
	query.RequestId = strconv.FormatUint(client.sequence.Add(1), 10)
	return client.mutate(ctx, query, "ConfigRemoveResponse")
}
func (client *Client) mutate(ctx context.Context, value request.IRequest, expected string) (result MutationResult, resultErr error) {
	work, end, err := client.enter(ctx)
	if err != nil {
		return result, err
	}
	defer end()
	budget, stop, err := (invocation.Budget{Limit: client.settings.Timeout}).Context(work, invocation.Execute)
	if err != nil {
		return result, fail(ErrWrite, "budget", err)
	}
	defer stop()
	lease, err := client.access.Acquire(budget, reservationBytes)
	if err != nil {
		return result, err
	}
	defer lease.Release()
	var setupErrors []error
	start := int(client.preferred.Load() % uint64(len(client.settings.Servers)))
	for offset := range client.settings.Servers {
		index := (start + offset) % len(client.settings.Servers)
		current, err := client.newSession(budget, index, nil)
		if err != nil {
			setupErrors = append(setupErrors, err)
			if !errors.Is(err, ErrUnavailable) || budget.Err() != nil {
				break
			}
			continue
		}
		token, err := client.token(budget, index)
		if err != nil {
			current.close()
			setupErrors = append(setupErrors, err)
			if !errors.Is(err, ErrUnavailable) || budget.Err() != nil {
				break
			}
			continue
		}
		if current.ctx.Err() != nil {
			err := current.failure("before-dispatch", current.ctx.Err())
			current.close()
			return result, fail(ErrWrite, "before-dispatch", err)
		}
		result.state = MutationUnknown
		payload, err := current.unary.Request(current.ctx, client.envelope(value, token))
		if err != nil {
			if code := rpcCode(err); code == codes.Unauthenticated || code == codes.PermissionDenied {
				client.forgetToken(index, token)
			}
			wrapped := current.failure("mutation", err)
			current.close()
			return result, fail(ErrWrite, "mutation", wrapped)
		}
		_, err = decodeResponse(payload, expected)
		if err != nil {
			var remote *RemoteError
			if errors.As(err, &remote) {
				switch remote.ErrorCode() {
				case 301, 401, 403, 409:
					result.state = MutationRejected
				}
			}
			if errors.Is(err, ErrDenied) {
				client.forgetToken(index, token)
			}
			current.close()
			return result, fail(ErrWrite, "mutation", err)
		}
		result.state = MutationAcknowledged
		client.preferred.Store(uint64(index))
		current.close()
		if budget.Err() != nil {
			return result, fail(ErrWrite, "acknowledged", budget.Err(), context.Cause(budget))
		}
		return result, nil
	}
	return result, fail(ErrWrite, "setup", append(setupErrors, budget.Err(), context.Cause(budget))...)
}

func (*PublishInputV1) Format(state fmt.State, _ rune) { restricted(state) }
func (*PublishInputV1) LogValue() slog.Value           { return slog.StringValue("nacos[restricted]") }
func (*MutationResult) Format(state fmt.State, _ rune) { restricted(state) }
func (*MutationResult) LogValue() slog.Value           { return slog.StringValue("nacos[restricted]") }
