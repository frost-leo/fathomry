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

package iceberg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"
)

func newStorageClient(s settings, transport http.RoundTripper) *s3.Client {
	return s3.New(s3.Options{BaseEndpoint: aws.String(s.StorageEndpoint), Region: s.StorageRegion,
		Credentials:  credentials.NewStaticCredentialsProvider(s.StorageAccessKey, s.StorageSecretKey, s.StorageSessionToken),
		HTTPClient:   &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		UsePathStyle: true, Retryer: aws.NopRetryer{}, Logger: logging.Nop{},
		DisableS3ExpressSessionAuth: aws.Bool(true), DisableMultiRegionAccessPoints: true,
		ContinueHeaderThresholdBytes: -1, RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired})
}

type storageWriteKey struct{}
type storageTransport struct {
	owner *connection
	base  http.RoundTripper
}

func (transport *storageTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	state, _ := request.Context().Value(exchangeKey{}).(*exchange)
	if state == nil || state.owner != transport.owner {
		return nil, failure(ErrAuthority, "storage-context")
	}
	s := transport.owner.settings
	endpoint, _ := url.Parse(s.StorageEndpoint)
	bucket, prefix := s.storageAddress()
	path := request.URL.Path
	bucketHead := request.Method == "HEAD" && (path == "/"+bucket || path == "/"+bucket+"/")
	if request.URL.Scheme != endpoint.Scheme || request.URL.Host != endpoint.Host || request.URL.User != nil ||
		(!bucketHead && !strings.HasPrefix(path, "/"+bucket+"/"+prefix)) {
		return nil, failure(ErrAuthority, "storage-endpoint")
	}
	if request.Method != "HEAD" && request.Method != "GET" && request.Method != "PUT" {
		return nil, failure(ErrUnsupported, "storage-operation")
	}
	for key, values := range request.URL.Query() {
		if key != "x-id" || len(values) != 1 ||
			values[0] != "HeadBucket" && values[0] != "HeadObject" && values[0] != "GetObject" && values[0] != "PutObject" {
			return nil, failure(ErrUnsupported, "storage-query")
		}
	}
	state.mu.Lock()
	state.storageRequests++
	allowed := state.storageRequests <= 3*s.MaxFileOps+1
	state.mu.Unlock()
	if !allowed {
		return nil, failure(ErrLimit, "storage-requests")
	}
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	var expected int64
	if request.Method == "GET" {
		var start, end int64
		if _, err := fmt.Sscanf(request.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || start != 0 || end < 0 || end >= int64(s.MaxObjectBytes) {
			return nil, failure(ErrInput, "storage-range")
		}
		expected = end + 1
	}
	if request.Method == "PUT" {
		if !s.Writes || request.ContentLength < 0 || request.ContentLength > int64(s.MaxObjectBytes) || request.Header.Get("If-None-Match") != "*" {
			return nil, failure(ErrAuthority, "storage-write")
		}
		index, ok := request.Context().Value(storageWriteKey{}).(int)
		state.mu.Lock()
		if !ok || index < 0 || index >= len(state.files) {
			state.mu.Unlock()
			return nil, failure(ErrAuthority, "storage-evidence")
		}
		state.files[index].Effect = Unknown
		state.mu.Unlock()
	}
	base := transport.base
	if base == nil {
		base = transport.owner.transport
	}
	response, err := base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		state.note(response.Body.Close(), true)
		return nil, failure(ErrAuthority, "storage-redirect")
	}
	var data []byte
	if request.Method == "GET" && response.StatusCode < 300 {
		wanted := fmt.Sprintf("bytes 0-%d/%d", expected-1, expected)
		if response.StatusCode != http.StatusPartialContent || response.Header.Get("Content-Range") != wanted ||
			response.ContentLength >= 0 && response.ContentLength != expected {
			state.note(response.Body.Close(), true)
			return nil, failure(ErrProtocol, "storage-range")
		}
		data = make([]byte, int(expected))
		_, err = io.ReadFull(response.Body, data)
	} else {
		state.mu.Lock()
		remaining := min(int64(32<<10), int64(s.MaxMetadataBytes)-state.catalogBytes)
		state.mu.Unlock()
		if remaining < 0 {
			state.note(response.Body.Close(), true)
			return nil, failure(ErrLimit, "storage-control")
		}
		data, err = io.ReadAll(io.LimitReader(response.Body, remaining+1))
		if int64(len(data)) > remaining {
			err = failure(ErrLimit, "storage-control")
		}
		state.mu.Lock()
		state.catalogBytes += int64(len(data))
		state.mu.Unlock()
	}
	state.note(response.Body.Close(), true)
	if err != nil {
		return nil, err
	}
	response.Body = io.NopCloser(bytes.NewReader(data))
	return response, nil
}
func storageFailure(err error) error {
	if err == nil {
		return nil
	}
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return errors.Join(fs.ErrNotExist, err)
		}
	}
	return err
}
func storageContext(ctx context.Context, state *exchange) context.Context {
	return withExchange(ctx, state)
}
