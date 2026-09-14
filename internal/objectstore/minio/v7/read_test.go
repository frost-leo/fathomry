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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestFullChecksumRangeAndExpectedDigest(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "valid", true: "corrupt"}[corrupt], func(t *testing.T) {
			server, options := newPeer(t)
			fixture := bindFixture(t, options, 4)
			content := []byte("good")
			sum := sha256.Sum256(content)
			server.mu.Lock()
			server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
				if request.Method != "GET" {
					return false
				}
				objectHeaders(writer, storedObject{body: content, etag: "opaque", version: "v1"})
				writer.Header().Set("X-Amz-Checksum-Type", "FULL_OBJECT")
				writer.Header().Set("X-Amz-Checksum-Sha256", base64.StdEncoding.EncodeToString(sum[:]))
				if corrupt {
					_, _ = io.WriteString(writer, "evil")
				} else {
					_, _ = writer.Write(content)
				}
				return true
			}
			server.mu.Unlock()
			receipt, err := fixture.client.Read(deadline(t), correlation("checksum"), ReadRequest{Address: Address{Key: "owned/checksum"}, ExpectedSHA256: hex.EncodeToString(sum[:])})
			result := settle(t, receipt, err)
			if corrupt {
				if result.Err() == nil || result.Outcome.Value.Transfer().Verified || result.Outcome.Value.Complete() {
					t.Fatal("corrupt full-object body accepted")
				}
				if string(result.Outcome.Value.DataCopy()) != "evil" {
					t.Fatal("partial/error data erased")
				}
			} else if result.Err() != nil || !result.Outcome.Value.Transfer().Verified {
				t.Fatal("valid checksum control rejected", result.Err())
			}
		})
	}
}
func TestReadLimitsRangeVersionAndPartialSink(t *testing.T) {
	server, options := newPeer(t)
	options.MaxReadBytes = 4
	fixture := bindFixture(t, options, 16)
	old := put(t, fixture.client, "owned/data", []byte("123456"))
	object, _ := old.Object()
	receipt, err := fixture.client.Read(deadline(t), correlation("oversize"), ReadRequest{Address: object.Address})
	if result := settle(t, receipt, err); !errors.Is(result.Err(), ErrLimit) || len(result.Outcome.Value.DataCopy()) != 0 {
		t.Fatal("read allocation bound weakened")
	}
	sum := sha256.Sum256([]byte("23"))
	receipt, err = fixture.client.Read(deadline(t), correlation("range"), ReadRequest{Address: object.Address, Offset: 1, Length: 2, ExpectedSHA256: hex.EncodeToString(sum[:])})
	if result := settle(t, receipt, err); result.Err() != nil || string(result.Outcome.Value.DataCopy()) != "23" || !result.Outcome.Value.Transfer().Verified {
		t.Fatal("bounded range failed", result.Err())
	}
	sinkErr := errors.New("sink-canary")
	receipt, err = fixture.client.Download(deadline(t), correlation("partial"), ReadRequest{Address: object.Address}, &partialWriter{cause: sinkErr})
	partial := settle(t, receipt, err)
	if !errors.Is(partial.Err(), sinkErr) || partial.Outcome.Value.Transfer().Bytes != 2 || partial.Outcome.Value.Complete() {
		t.Fatal("partial sink effects lost")
	}
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.Method != "GET" {
			return false
		}
		objectHeaders(writer, storedObject{body: []byte("xx"), version: "wrong"})
		_, _ = io.WriteString(writer, "xx")
		return true
	}
	server.mu.Unlock()
	receipt, err = fixture.client.Read(deadline(t), correlation("wrong-version"), ReadRequest{Address: object.Address})
	if result := settle(t, receipt, err); !errors.Is(result.Err(), ErrProtocol) || len(result.Outcome.Value.DataCopy()) != 0 {
		t.Fatal("version silently weakened")
	}
	receipt, err = fixture.client.Read(deadline(t), correlation("ignored-range"), ReadRequest{Address: Address{Key: object.Address.Key}, Length: 2})
	if result := settle(t, receipt, err); !errors.Is(result.Err(), ErrProtocol) {
		t.Fatal("ignored range silently accepted")
	}
}

type partialWriter struct{ cause error }

func (writer *partialWriter) Write(body []byte) (int, error) { return min(len(body), 2), writer.cause }
func TestEOFWithAnotherCauseIsNotSuccessfulEmpty(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 4)
	cause := errors.New("reader-terminal-failure")
	input := failingReader{strings.NewReader(""), errors.Join(io.EOF, cause)}
	before := server.count()
	receipt, err := fixture.client.Put(deadline(t), deadline(t), correlation("joined-eof"), WriteRequest{Key: "owned/joined", Size: 0}, input)
	result := settle(t, receipt, err)
	if !errors.Is(result.Err(), cause) || result.Outcome.Value.Complete() || server.count() != before {
		t.Fatal("EOF-bearing failure certified as a successful empty upload")
	}
}
func TestCanceledRequestsAndUnsupportedPathsDoNoIO(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 4)
	before := server.count()
	ctx, cancel := context.WithCancel(deadline(t))
	cancel()
	if receipt, err := fixture.client.Copy(ctx, correlation("canceled-copy"), CopyRequest{Source: Address{Key: "owned/source"}, Key: "owned/dest"}); receipt != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("pre-canceled copy accepted")
	}
	if receipt, err := fixture.client.Copy(deadline(t), correlation("conditional-copy"), CopyRequest{Source: Address{Key: "owned/source"}, Key: "owned/dest", IfAbsent: true}); receipt != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatal("unsupported copy condition weakened")
	}
	if _, err := fixture.client.Read(deadline(t), correlation("outside"), ReadRequest{Address: Address{Key: "outside"}}); !errors.Is(err, ErrAuthority) {
		t.Fatal("prefix escaped")
	}
	if _, err := fixture.client.Put(deadline(t), deadline(t), correlation("encryption"), WriteRequest{Key: "owned/sse", Encryption: "SSE-C"}, bytes.NewReader(nil)); !errors.Is(err, ErrUnsupported) {
		t.Fatal("unsupported encryption weakened")
	}
	if server.count() != before {
		t.Fatal("rejected/canceled operation caused native request")
	}
}
