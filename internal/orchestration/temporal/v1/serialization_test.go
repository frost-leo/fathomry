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

package temporal_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	commonpb "go.temporal.io/api/common/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

type versionCodec struct {
	version string
	legacy  bool
}

func (codec versionCodec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	result := make([]*commonpb.Payload, len(payloads))
	for index, payload := range payloads {
		data, err := proto.Marshal(payload)
		if err != nil {
			return nil, err
		}
		result[index] = &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("binary/gh61-fixture"), "version": []byte(codec.version)}, Data: data}
	}
	return result, nil
}

func (codec versionCodec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	result := make([]*commonpb.Payload, len(payloads))
	for index, payload := range payloads {
		if string(payload.Metadata["encoding"]) != "binary/gh61-fixture" {
			result[index] = payload
			continue
		}
		version := string(payload.Metadata["version"])
		if version != codec.version && !(codec.legacy && version == "1") {
			return nil, errors.New("unsupported fixture encoding version")
		}
		decoded := &commonpb.Payload{}
		if err := proto.Unmarshal(payload.Data, decoded); err != nil {
			return nil, err
		}
		result[index] = decoded
	}
	return result, nil
}

type retainedPayloadStore struct {
	mu                       sync.Mutex
	payloads                 map[string]*commonpb.Payload
	bytes, stores, retrieves int
	failReads                bool
}

func newRetainedPayloadStore() *retainedPayloadStore {
	return &retainedPayloadStore{payloads: make(map[string]*commonpb.Payload)}
}

func (*retainedPayloadStore) Name() string { return "gh61-memory" }

func (*retainedPayloadStore) Type() string { return "bounded-test-memory" }

func (store *retainedPayloadStore) Store(ctx converter.StorageDriverStoreContext, payloads []*commonpb.Payload) ([]converter.StorageDriverClaim, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Context.Err(); err != nil {
		return nil, err
	}
	claims := make([]converter.StorageDriverClaim, len(payloads))
	for index, payload := range payloads {
		size := proto.Size(payload)
		if size > 64<<10 || store.bytes+size > 4<<20 {
			return nil, errors.New("test payload storage bound reached")
		}
		encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
		if err != nil {
			return nil, err
		}
		hash := sha256.Sum256(encoded)
		id := strconv.Itoa(store.stores)
		store.stores++
		store.bytes += size
		store.payloads[id] = proto.Clone(payload).(*commonpb.Payload)
		claims[index] = converter.StorageDriverClaim{ClaimData: map[string]string{"id": id, "sha256": hex.EncodeToString(hash[:])}}
	}
	return claims, nil
}

func (store *retainedPayloadStore) Retrieve(ctx converter.StorageDriverRetrieveContext, claims []converter.StorageDriverClaim) ([]*commonpb.Payload, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Context.Err(); err != nil {
		return nil, err
	}
	if store.failReads {
		return nil, errors.New("test payload storage is unavailable")
	}
	result := make([]*commonpb.Payload, len(claims))
	for index, claim := range claims {
		payload, exists := store.payloads[claim.ClaimData["id"]]
		if !exists {
			return nil, errors.New("unknown test payload claim")
		}
		encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
		if err != nil {
			return nil, err
		}
		hash := sha256.Sum256(encoded)
		if claim.ClaimData["sha256"] != hex.EncodeToString(hash[:]) {
			return nil, errors.New("test payload integrity mismatch")
		}
		store.retrieves++
		result[index] = proto.Clone(payload).(*commonpb.Payload)
	}
	return result, nil
}

func serializationRuntime(version string, legacy bool, store *retainedPayloadStore) temporal.RuntimeOptions {
	data := converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), versionCodec{version: version, legacy: legacy})
	return temporal.RuntimeOptions{DataConverter: data, ExternalStorage: converter.ExternalStorage{Drivers: []converter.StorageDriver{store}, PayloadSizeThreshold: 1}}
}

func TestNativeRemoteCodecAndLegacyReader(t *testing.T) {
	legacy := converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), versionCodec{version: "1"})
	payload, err := legacy.ToPayloads("original", int64(7))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(converter.NewPayloadCodecHTTPHandler(versionCodec{version: "2", legacy: true}))
	defer server.Close()
	remote := converter.NewRemoteDataConverter(converter.GetDefaultDataConverter(), converter.RemoteDataConverterOptions{Endpoint: server.URL, Client: http.Client{Timeout: time.Second}})
	var value string
	var number int64
	if err := remote.FromPayloads(payload, &value, &number); err != nil || value != "original" || number != 7 {
		t.Fatal("compatible remote reader failed", err)
	}
	strict := converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), versionCodec{version: "2"})
	if err := strict.FromPayloads(payload, &value, &number); err == nil {
		t.Fatal("incompatible reader accepted legacy payload")
	}
	newPayload, err := remote.ToPayloads("new")
	if err != nil {
		t.Fatal(err)
	}
	if string(newPayload.Payloads[0].Metadata["version"]) != "2" {
		t.Fatal("remote codec did not use selected writer")
	}
	if err := legacy.FromPayloads(newPayload, &value); err == nil {
		t.Fatal("old-only reader accepted a future encoding")
	}
	server.Close()
	if err := remote.FromPayloads(payload, &value, &number); err == nil {
		t.Fatal("missing codec endpoint became empty success")
	}
}

func TestNativeRuntimeConvertersAreAppliedBeforeRPC(t *testing.T) {
	runtime := serializationRuntime("2", true, newRetainedPayloadStore())
	fixture := newRuntimeFixture(t, 1, runtime, nil)
	executions, inbox := executionBinding(t, fixture)
	_, err := executions.ExecuteWorkflow(context.Background(), fault.Correlation{Call: "external-start"}, sdk.StartWorkflowOptions{ID: "external", TaskQueue: "unit"}, "definition", "stored-input")
	if err != nil {
		t.Fatal(err)
	}
	store := runtime.ExternalStorage.Drivers[0].(*retainedPayloadStore)
	store.mu.Lock()
	stored := store.stores
	store.mu.Unlock()
	if stored == 0 {
		t.Fatal("configured native converter/storage was ignored")
	}
	receiveExecution(t, inbox)
}
