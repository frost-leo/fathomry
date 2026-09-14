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
	"context"
	"errors"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type testRoundTrip func(*http.Request) (*http.Response, error)

func (fn testRoundTrip) RoundTrip(request *http.Request) (*http.Response, error) { return fn(request) }

type failingCloseBody struct {
	io.Reader
	cause error
}

func (body failingCloseBody) Close() error { return body.cause }

func TestStorageCloseFailureRemainsIndependent(t *testing.T) {
	_, options := newPeer(t)
	f := bindFixture(t, options, 4)
	cause := errors.New("storage-body-close-failure")
	state := newExchange(f.client.owner, nil)
	transport := &storageTransport{owner: f.client.owner, base: testRoundTrip(func(request *http.Request) (*http.Response, error) {
		header := http.Header{"Content-Range": []string{"bytes 0-3/4"}, "Content-Length": []string{"4"}, "ETag": []string{`"fixed"`}}
		return &http.Response{StatusCode: 206, Header: header, ContentLength: 4, Request: request, Body: failingCloseBody{Reader: strings.NewReader("data"), cause: cause}}, nil
	})}
	client := newStorageClient(defaults(options), transport)
	output, err := client.GetObject(storageContext(deadline(t), state), &s3.GetObjectInput{Bucket: aws.String("fixture"), Key: aws.String("owned/data"), Range: aws.String("bytes=0-3")})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(output.Body)
	if err != nil {
		t.Fatal(err)
	}
	_ = output.Body.Close()
	primary, cleanup := state.finish()
	if string(data) != "data" || primary != nil || !errors.Is(cleanup, cause) {
		t.Fatal("response close failure lost or conflated with payload")
	}
}
func TestStorageDoesNotRetryOrOverwrite(t *testing.T) {
	peer, options := newPeer(t)
	f := bindFixture(t, options, 4)
	state := newExchange(f.client.owner, nil)
	files := &fileIO{ctx: deadline(t), state: state}
	peer.mu.Lock()
	peer.objects["owned/existing"] = []byte("old")
	peer.mu.Unlock()
	writer, err := files.Create(options.Location + "existing")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = writer.Write([]byte("new"))
	if err = writer.Close(); err == nil {
		t.Fatal("existing object overwritten")
	}
	peer.mu.Lock()
	data := string(peer.objects["owned/existing"])
	peer.mu.Unlock()
	if data != "old" {
		t.Fatal("conditional write failed")
	}
	var attempts atomic.Int64
	peer.mu.Lock()
	peer.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.Method == "PUT" && strings.HasPrefix(request.URL.Path, "/fixture/") {
			attempts.Add(1)
			writer.Header().Set("Content-Type", "application/xml")
			writer.WriteHeader(503)
			_, _ = io.WriteString(writer, "<Error><Code>SlowDown</Code></Error>")
			return true
		}
		return false
	}
	peer.mu.Unlock()
	writer, err = files.Create(options.Location + "failed")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = writer.Write([]byte("new"))
	if err = writer.Close(); err == nil {
		t.Fatal("failed storage call accepted")
	}
	if attempts.Load() != 1 {
		t.Fatalf("storage retry count=%d", attempts.Load())
	}
}
func TestNoOtherIntegrationDependency(t *testing.T) {
	command := exec.CommandContext(context.Background(), "go", "list", "-deps", ".")
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(output), "\n") {
		if strings.HasPrefix(line, "github.com/frost-leo/fathomry/internal/") {
			switch line {
			case "github.com/frost-leo/fathomry/internal/fault", "github.com/frost-leo/fathomry/internal/resource",
				"github.com/frost-leo/fathomry/internal/invocation", "github.com/frost-leo/fathomry/internal/compatibility",
				"github.com/frost-leo/fathomry/internal/tableformat/iceberg/v0":
			default:
				t.Fatalf("unexpected integration dependency: %s", line)
			}
		}
		if strings.Contains(line, "github.com/minio/minio-go") {
			t.Fatal("MinIO SDK imported by Iceberg")
		}
	}
}
