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
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.yaml.in/yaml/v3"
)

type privateServiceConfig struct {
	MinIO struct {
		Endpoint string `yaml:"api_endpoint"`
		Root     struct {
			Username string `yaml:"username"`
			Password string `yaml:"password"`
		} `yaml:"root"`
		Fathomry struct {
			AccessKey string `yaml:"access_key"`
			SecretKey string `yaml:"secret_key"`
			Bucket    string `yaml:"bucket"`
			Region    string `yaml:"region"`
		} `yaml:"fathomry"`
	} `yaml:"minio"`
}

func serviceClient(t testing.TB, options OptionsV1) *native.Core {
	t.Helper()
	endpoint, err := url.Parse(options.Endpoint)
	if err != nil {
		t.Fatal("invalid fixture endpoint")
	}
	trust, err := tlsConfig(defaults(options))
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{Proxy: nil, TLSClientConfig: trust, ResponseHeaderTimeout: 5 * time.Second, MaxResponseHeaderBytes: 32 << 10, DisableCompression: true}
	t.Cleanup(transport.CloseIdleConnections)
	client, err := native.NewCore(endpoint.Host, &native.Options{Creds: credentials.NewStaticV4(options.AccessKey, options.SecretKey, ""),
		Region: options.Region, Secure: !options.Plaintext, Transport: transport, BucketLookup: native.BucketLookupPath, MaxRetries: 1})
	if err != nil {
		t.Fatal("independent fixture client unavailable")
	}
	return client
}
func loadService(t *testing.T) OptionsV1 {
	t.Helper()
	path := os.Getenv("FATHOMRY_MINIO_TEST_CONFIG")
	if path == "" {
		t.Skip("explicit private MinIO fixture not supplied; no real-service acceptance")
	}
	if os.Getenv("FATHOMRY_MINIO_TEST_WRITES") != "1" {
		t.Fatal("explicit isolated-prefix mutation authorization required")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal("private fixture unavailable")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 64<<10 {
		t.Fatal("private fixture permissions/size invalid")
	}
	var config privateServiceConfig
	decoder := yaml.NewDecoder(io.LimitReader(file, 64<<10+1))
	if decoder.Decode(&config) != nil || decoder.Decode(new(any)) != io.EOF {
		t.Fatal("invalid private MinIO fixture")
	}
	value := config.MinIO.Fathomry
	options := OptionsV1{Name: "service", Endpoint: config.MinIO.Endpoint, Plaintext: strings.HasPrefix(config.MinIO.Endpoint, "http://"),
		Region: value.Region, Bucket: value.Bucket, AccessKey: value.AccessKey, SecretKey: value.SecretKey, Writes: true, Timeout: 30 * time.Second}
	if err := validate(defaults(options)); err != nil {
		t.Fatal("unsupported service fixture profile")
	}
	// Only this read-only preflight uses the configured administrator. All object
	// operations, independent read-back and cleanup use the dedicated account.
	preflight := options
	preflight.AccessKey = config.MinIO.Root.Username
	preflight.SecretKey = config.MinIO.Root.Password
	if preflight.AccessKey == "" || preflight.SecretKey == "" {
		t.Fatal("read-only versioning preflight credentials required")
	}
	versioning, err := serviceClient(t, preflight).GetBucketVersioning(deadline(t), options.Bucket)
	if err != nil || versioning.Status != "" {
		t.Fatal("refusing writes without observed never-enabled bucket versioning")
	}
	return options
}
func TestMinIOServiceIntegration(t *testing.T) { runServiceAcceptance(t, loadService(t)) }

// Running the identical fixture against loopback validates the fixture's own
// branches, not the real service's durability, authorization or atomicity.
func TestServiceFixtureAgainstProtocolPeer(t *testing.T) {
	server, options := newPeer(t)
	server.mu.Lock()
	server.unversioned = true
	server.mu.Unlock()
	options.Versions = false
	options.Tags = false
	runServiceAcceptance(t, options)
}
func runServiceAcceptance(t *testing.T, options OptionsV1) {
	t.Helper()
	tokenBytes := make([]byte, 12)
	if _, err := rand.Read(tokenBytes); err != nil {
		t.Fatal(err)
	}
	token := hex.EncodeToString(tokenBytes)
	options.Prefix = "fathomry-gh38-" + token + "/"
	t.Logf("test-owned namespace: %s (no bucket/account identifiers logged)", options.Prefix)
	independent := serviceClient(t, options)
	// An empty result from an independent authenticated iterator is required.
	for object := range independent.ListObjectsIter(deadline(t), options.Bucket, native.ListObjectsOptions{Prefix: options.Prefix, Recursive: true, MaxKeys: 1}) {
		if object.Err != nil {
			t.Fatal("test prefix absence unconfirmed")
		}
		t.Fatal("test namespace already exists")
	}
	pending, err := independent.ListMultipartUploads(deadline(t), options.Bucket, options.Prefix, "", "", "", 1)
	if err != nil || pending.Bucket != options.Bucket || len(pending.Uploads) != 0 || pending.IsTruncated {
		t.Fatal("test namespace multipart absence unconfirmed")
	}
	owned := map[string]bool{}
	uploads := map[string]string{}
	fixture := bindFixture(t, options, 32)
	t.Cleanup(func() {
		keys := make([]string, 0, len(owned))
		for key := range owned {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for uploadID, key := range uploads {
			_, err := independent.ListObjectParts(deadline(t), options.Bucket, key, uploadID, 0, 1)
			if err != nil {
				var response native.ErrorResponse
				if errors.As(err, &response) && response.Code == "NoSuchUpload" {
					continue
				}
				t.Error("owned upload presence unconfirmed; not assuming cleanup")
				continue
			}
			if err := independent.AbortMultipartUpload(deadline(t), options.Bucket, key, uploadID); err != nil {
				t.Error("owned upload cleanup failed")
			}
		}
		for _, key := range keys {
			info, err := independent.StatObject(deadline(t), options.Bucket, key, native.StatObjectOptions{})
			if err != nil {
				var response native.ErrorResponse
				if errors.As(err, &response) && response.Code == "NoSuchKey" {
					continue
				}
				t.Error("test object ownership unconfirmed; refusing deletion")
				continue
			}
			if info.Headers.Get("X-Amz-Meta-Fixture") != token || info.VersionID != "" && info.VersionID != "null" {
				t.Error("test ownership marker changed; refusing deletion")
				continue
			}
			if err := independent.RemoveObject(deadline(t), options.Bucket, key, native.RemoveObjectOptions{}); err != nil {
				t.Error("exact owned object cleanup failed")
			}
		}
		for object := range independent.ListObjectsIter(deadline(t), options.Bucket, native.ListObjectsOptions{Prefix: options.Prefix, Recursive: true, MaxKeys: 32}) {
			if object.Err != nil {
				t.Error("cleanup absence inspection failed")
			} else {
				t.Error("test-owned namespace has residual objects")
			}
			break
		}
		page, err := independent.ListMultipartUploads(deadline(t), options.Bucket, options.Prefix, "", "", "", 32)
		if err != nil || len(page.Uploads) != 0 || page.IsTruncated {
			t.Error("test-owned namespace has unconfirmed multipart cleanup")
		}
		if !t.Failed() {
			t.Log("exact owned objects/uploads cleaned; independent namespace absence observed")
		}
	})
	consume := func(receipt *invocation.Receipt[Result], err error) invocation.Result[Result] {
		result := settle(t, receipt, err)
		if id := result.Outcome.Value.Transfer().UploadID; id != "" {
			// Keys are assigned by each upload caller below, before any cleanup can run.
			if _, known := uploads[id]; !known {
				t.Fatal("fixture lost upload ownership mapping")
			}
		}
		return result
	}
	write := func(name string, body []byte, size int64, condition bool) invocation.Result[Result] {
		key := options.Prefix + name
		owned[key] = true
		receipt, err := fixture.client.Put(deadline(t), deadline(t), correlation("service-"+name), WriteRequest{Key: key, Size: size, IfAbsent: condition, Metadata: map[string]string{"fixture": token}}, bytes.NewReader(body))
		result := settle(t, receipt, err)
		if id := result.Outcome.Value.Transfer().UploadID; id != "" {
			uploads[id] = key
		}
		return result
	}
	small := []byte("service-content")
	if result := write("small", small, int64(len(small)), true); result.Err() != nil {
		t.Fatal("service single upload failed", result.Err())
	}
	if result := write("empty", []byte{}, -1, true); result.Err() != nil {
		t.Fatal("service empty upload failed", result.Err())
	}
	large := bytes.Repeat([]byte("m"), 5<<20+1)
	created := write("multipart", large, -1, true)
	if created.Err() != nil || created.Outcome.Value.Transfer().PartsAcknowledged != 2 {
		t.Fatal("service multipart upload failed", created.Err())
	}
	rejected := write("multipart", large, int64(len(large)), true)
	if !errors.Is(rejected.Err(), ErrCondition) || rejected.Outcome.Value.Transfer().Effect == Acknowledged {
		t.Fatal("real final condition not enforced", rejected.Err())
	}
	digest := sha256.Sum256(large)
	request := ReadRequest{Address: Address{Key: options.Prefix + "multipart"}, ExpectedSHA256: hex.EncodeToString(digest[:])}
	receipt, err := fixture.client.Read(deadline(t), correlation("service-read"), request)
	read := consume(receipt, err)
	if read.Err() != nil || !read.Outcome.Value.Transfer().Verified || !bytes.Equal(read.Outcome.Value.DataCopy(), large) {
		t.Fatal("service content/checksum mismatch", read.Err())
	}
	nativeBody, _, _, err := independent.GetObject(deadline(t), options.Bucket, request.Address.Key, native.GetObjectOptions{})
	if err != nil {
		t.Fatal("independent service GET failed")
	}
	observed, readErr := io.ReadAll(io.LimitReader(nativeBody, int64(len(large))+1))
	closeErr := nativeBody.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(observed, large) {
		t.Fatal("independent service bytes differ")
	}
	sink := sha256.New()
	receipt, err = fixture.client.Download(deadline(t), correlation("service-download"), request, sink)
	if result := consume(receipt, err); result.Err() != nil || !result.Outcome.Value.Transfer().Verified || !bytes.Equal(sink.Sum(nil), digest[:]) {
		t.Fatal("real streaming download failed", result.Err())
	}
	receipt, err = fixture.client.Read(deadline(t), correlation("service-range"), ReadRequest{Address: request.Address, Offset: 7, Length: 31})
	if result := consume(receipt, err); result.Err() != nil || !bytes.Equal(result.Outcome.Value.DataCopy(), large[7:38]) {
		t.Fatal("real range failed", result.Err())
	}
	copiedKey := options.Prefix + "copy"
	owned[copiedKey] = true
	receipt, err = fixture.client.Copy(deadline(t), correlation("service-copy"), CopyRequest{Source: Address{Key: options.Prefix + "small"}, Key: copiedKey})
	if result := consume(receipt, err); result.Err() != nil {
		t.Fatal("real native copy failed", result.Err())
	}
	for key, expected := range map[string][]byte{options.Prefix + "small": small, options.Prefix + "empty": {}, copiedKey: small} {
		body, _, _, err := independent.GetObject(deadline(t), options.Bucket, key, native.GetObjectOptions{})
		if err != nil {
			t.Fatal("independent small/empty/copy GET failed")
		}
		observed, readErr := io.ReadAll(io.LimitReader(body, int64(len(expected))+1))
		closeErr := body.Close()
		if readErr != nil || closeErr != nil || !bytes.Equal(observed, expected) {
			t.Fatal("independent small/empty/copy content differs")
		}
	}
	failedKey := options.Prefix + "aborted"
	owned[failedKey] = true
	inputErr := errors.New("service-fixture-reader-failure")
	receipt, err = fixture.client.Put(deadline(t), deadline(t), correlation("service-abort"), WriteRequest{Key: failedKey, Size: -1, Metadata: map[string]string{"fixture": token}}, failingReader{bytes.NewReader(large[:5<<20]), inputErr})
	aborted := settle(t, receipt, err)
	if uploadID := aborted.Outcome.Value.Transfer().UploadID; uploadID != "" {
		uploads[uploadID] = failedKey
	}
	if !errors.Is(aborted.Outcome.Primary, inputErr) || aborted.Outcome.Cleanup != nil || !aborted.Outcome.Value.Transfer().AbortAcknowledged {
		t.Fatal("real multipart cleanup failed", aborted.Err())
	}
	receipt, err = fixture.client.List(deadline(t), correlation("service-list"), ListRequest{Prefix: options.Prefix})
	if result := consume(receipt, err); result.Err() != nil || !result.Outcome.Value.Complete() || len(result.Outcome.Value.ObjectsCopy()) != 4 {
		t.Fatal("real listing incomplete", result.Err())
	}
	receipt, err = fixture.client.Remove(deadline(t), correlation("service-remove"), []Address{{Key: copiedKey}})
	if result := consume(receipt, err); result.Err() != nil || result.Outcome.Value.RemovalsCopy()[0].Effect != Acknowledged {
		t.Fatal("real exact removal failed", result.Err())
	}
	receipt, err = fixture.client.Read(deadline(t), correlation("service-missing"), ReadRequest{Address: Address{Key: copiedKey}})
	if result := consume(receipt, err); !errors.Is(result.Err(), ErrMissing) {
		t.Fatal("real missing confused with empty")
	}
	failures := 0
	for fixture.inbox.Usage().Outstanding > 0 {
		delivery, err := fixture.inbox.Next(deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		result, err := delivery.Receipt().WaitReleased(deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if errors.Is(result.Err(), ErrCondition) {
			failures++
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if failures != 1 {
		t.Fatal("independent real condition-failure evidence lost")
	}
	t.Log("single/unknown-empty/multipart/condition/read/range/copy/list/remove and independent evidence exercised")
}
