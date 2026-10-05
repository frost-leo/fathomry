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

package minio

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// SignRequest delegates only GET, HEAD or PUT. Expiry is explicit, in whole
// seconds. MatchETag and IfAbsent are signed conditions; IfAbsent is PUT-only.
// PUT cannot select an existing version. No arbitrary headers or query escape.
type SignRequest struct {
	private
	Method    string
	Address   Address
	Expiry    time.Duration
	MatchETag string
	IfAbsent  bool
}

// Delegation is a sensitive bearer capability, not object-effect evidence.
// URL and HeadersCopy deliberately extract secrets. Client closure cannot revoke it.
type Delegation struct {
	private
	data *delegationData
}
type delegationData struct {
	url     string
	headers http.Header
	method  string
	expiry  time.Duration
	token   bool
}

func (value Delegation) URL() string {
	if value.data == nil {
		return ""
	}
	return value.data.url
}
func (value Delegation) HeadersCopy() http.Header {
	if value.data == nil {
		return nil
	}
	return value.data.headers.Clone()
}
func (value Delegation) Method() string {
	if value.data == nil {
		return ""
	}
	return value.data.method
}
func (value Delegation) Expiry() time.Duration {
	if value.data == nil {
		return 0
	}
	return value.data.expiry
}

// StaticToken reports that token validity may shorten the requested expiry.
// False does not guarantee credential validity or continued authorization.
func (value Delegation) StaticToken() bool { return value.data != nil && value.data.token }
func (result Result) Delegation() (Delegation, bool) {
	if result.data == nil || result.data.delegation == nil {
		return Delegation{}, false
	}
	return Delegation{data: result.data.delegation}, true
}

// Presign performs local signing with frozen explicit credentials and region.
// Admission and evidence are required even though issuance performs no HTTP I/O.
func (client *Client) Presign(ctx context.Context, id fault.Correlation, request SignRequest) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx); err != nil {
		return nil, err
	}
	value := client.owner.settings
	granted := false
	switch request.Method {
	case http.MethodGet:
		granted = value.PresignGET
	case http.MethodHead:
		granted = value.PresignHEAD
	case http.MethodPut:
		granted = value.PresignPUT
	default:
		return nil, failure(ErrUnsupported, "sign-method")
	}
	if !granted {
		return nil, failure(ErrAuthority, "sign-grant")
	}
	if err := client.address(request.Address, request.Method == http.MethodPut); err != nil {
		return nil, err
	}
	if request.Expiry < time.Second || request.Expiry > value.MaxPresignExpiry || request.Expiry%time.Second != 0 ||
		!validETag(request.MatchETag) || request.IfAbsent && request.MatchETag != "" ||
		request.IfAbsent && request.Method != http.MethodPut || request.Method == http.MethodPut && request.Address.VersionID != "" {
		return nil, failure(ErrInput, "sign")
	}
	headers := make(http.Header)
	if request.MatchETag != "" {
		headers.Set("If-Match", "\""+request.MatchETag+"\"")
		if request.Method == http.MethodPut && request.MatchETag == "*" {
			headers.Set("If-Match", "*")
		}
	}
	if request.IfAbsent {
		headers.Set("If-None-Match", "*")
	}
	query := make(url.Values)
	if request.Address.VersionID != "" {
		query.Set("versionId", request.Address.VersionID)
	}
	return client.start(ctx, id, "presign", false, func(work context.Context, state *exchange, data *resultData) (error, error) {
		signed, err := client.owner.native.PresignHeader(work, request.Method, value.Bucket, request.Address.Key, request.Expiry, query, headers)
		if err != nil {
			return nativeFailure(ErrAuthority, "presign", work, err), nil
		}
		if signed.Scheme+"://"+signed.Host != value.Endpoint || signed.Path != "/"+value.Bucket+"/"+request.Address.Key {
			return failure(ErrProtocol, "signed-target"), nil
		}
		actual := ";" + signed.Query().Get("X-Amz-SignedHeaders") + ";"
		for name := range headers {
			if !strings.Contains(actual, ";"+strings.ToLower(name)+";") {
				return failure(ErrProtocol, "signed-condition"), nil
			}
		}
		if err := work.Err(); err != nil {
			return err, nil
		}
		data.delegation = &delegationData{signed.String(), headers.Clone(), request.Method, request.Expiry, value.SessionToken != ""}
		data.complete = true
		return nil, nil
	})
}
