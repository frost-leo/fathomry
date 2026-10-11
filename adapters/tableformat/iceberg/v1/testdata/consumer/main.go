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

package main

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	iceberg "github.com/frost-leo/fathomry/adapters/tableformat/iceberg/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

// The test harness copies the pinned old-writer fixture into this standalone
// module. The program has no runtime checkout or Internal package dependency.
//
//go:embed data
var fixtureData embed.FS

func fixturePeer() (*httptest.Server, error) {
	objects := map[string][]byte{}
	err := fs.WalkDir(fixtureData, "data", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || entry.Name() == "README.md" {
			return nil
		}
		value, err := fixtureData.ReadFile(path)
		if err == nil {
			objects["owned/historical/"+strings.TrimPrefix(path, "data/")] = value
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	var index struct {
		Location string `json:"metadata-location"`
	}
	if err = json.Unmarshal(objects["owned/historical/fixture.json"], &index); err != nil {
		return nil, err
	}
	reply, err := json.Marshal(map[string]any{"metadata-location": index.Location, "metadata": json.RawMessage(objects[strings.TrimPrefix(index.Location, "s3://fixture/")])})
	if err != nil {
		return nil, err
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.WriteHeader(405)
			return
		}
		if r.URL.Path == "/catalog/v1/config" {
			_, _ = w.Write([]byte(`{"defaults":{"prefix":"warehouse"}}`))
			return
		}
		if r.URL.Path == "/catalog/v1/warehouse/namespaces/isolated/tables/historical" {
			_, _ = w.Write(reply)
			return
		}
		value, found := objects[strings.TrimPrefix(r.URL.Path, "/fixture/")]
		if !found {
			w.WriteHeader(404)
			return
		}
		digest := sha256.Sum256(value)
		etag := strconv.Quote(hex.EncodeToString(digest[:]))
		if expected := r.Header.Get("If-Match"); expected != "" && expected != etag {
			w.WriteHeader(412)
			return
		}
		w.Header().Set("ETag", etag)
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.Itoa(len(value)))
			return
		}
		if requested := r.Header.Get("Range"); requested != "" {
			var first, last int
			if _, err := fmt.Sscanf(requested, "bytes=%d-%d", &first, &last); err != nil || first < 0 || last < first || last >= len(value) {
				w.WriteHeader(416)
				return
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", first, last, len(value)))
			w.Header().Set("Content-Length", strconv.Itoa(last-first+1))
			w.WriteHeader(206)
			_, _ = w.Write(value[first : last+1])
			return
		}
		_, _ = w.Write(value)
	})), nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("iceberg independent direct and Fixed/Follow passed")
}
func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	dataPeer, err := fixturePeer()
	if err != nil {
		return err
	}
	defer dataPeer.Close()
	if err := dataConsumer(ctx, dataPeer.URL); err != nil {
		return err
	}
	peer := func(label string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if r.URL.Path == "/catalog/v1/config" {
				fmt.Fprint(w, `{"defaults":{"prefix":"warehouse"}}`)
				return
			}
			fmt.Fprintf(w, `{"namespace":["isolated"],"properties":{"owner":%q}}`, label)
		}))
	}
	first, second := peer("first"), peer("second")
	defer first.Close()
	defer second.Close()
	source := func(endpoint string) iceberg.Settings {
		return iceberg.Settings{Name: "consumer", CatalogURI: endpoint + "/catalog", CatalogPrefix: "warehouse", Warehouse: "fixture", Namespace: []string{"isolated"}, Location: "s3://fixture/owned/", Plaintext: true, StorageEndpoint: endpoint, StorageRegion: "us-east-1", StorageAccessKey: "fixture", StorageSecretKey: "fixture", Permissions: iceberg.Grants{Read: true}}
	}
	one, two := source(first.URL), source(second.URL)
	policy, err := iceberg.Recommend(one)
	if err != nil {
		return err
	}
	policy.Runtime.MaxActive *= 4
	policy.Runtime.MaxWorkBytes *= 4
	policy.Evidence.Capacity *= 4
	policy.Evidence.MaxBytes *= 4
	runtime, err := framework.New(ctx, framework.Options{Operations: policy.Runtime})
	if err != nil {
		return err
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[iceberg.Result](policy.Evidence)
	if err != nil {
		return err
	}
	deps := iceberg.Dependencies{Runtime: runtime.Operations(), Evidence: inbox}
	direct, err := iceberg.Open(ctx, one, deps)
	if err != nil {
		return err
	}
	receipt, err := direct.Client().Use(ctx, func(s *iceberg.Session) error {
		values, err := s.LoadNamespaceProperties([]string{"isolated"})
		if err != nil {
			return err
		}
		if values["owner"] != "first" {
			return fmt.Errorf("direct source mismatch")
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err = receiptErr(ctx, receipt); err != nil {
		return err
	}
	if err = direct.Close(ctx); err != nil {
		return err
	}
	var follow, fixed resource.Ref[iceberg.Handle]
	for _, entry := range []struct {
		name   string
		policy resource.Policy
		target *resource.Ref[iceberg.Handle]
	}{{"follow", resource.Follow, &follow}, {"fixed", resource.Fixed, &fixed}} {
		ref, err := resource.Bind(runtime.Resources(), resource.Binding[int, iceberg.Handle]{Name: entry.name, Policy: entry.policy,
			Select: func(view settings.View) (int, error) {
				s, err := settings.As[int](view)
				if err != nil {
					return 0, err
				}
				return s.ValueCopy()
			},
			Clone: func(v int) int { return v }, Equal: func(a, b int) bool { return a == b },
			Build: func(lifetime context.Context, generation int) (*resource.Instance[iceberg.Handle], error) {
				selected := one
				if generation == 2 {
					selected = two
				}
				owner, err := iceberg.Open(lifetime, selected, deps)
				if owner == nil {
					return nil, err
				}
				return &resource.Instance[iceberg.Handle]{Value: owner.Handle(), Release: owner.Release}, err
			}})
		if err != nil {
			return err
		}
		*entry.target = ref
	}
	apply := func(version int) error {
		snapshot, err := settings.New(version, func(v int) int { return v })
		if err != nil {
			return err
		}
		update, err := runtime.Resources().Apply(ctx, snapshot.View())
		if err != nil {
			return err
		}
		return update.Wait(ctx)
	}
	if err = apply(1); err != nil {
		return err
	}
	client, err := iceberg.Using(ctx, follow, policy.Budget, deps)
	if err != nil {
		return err
	}
	receipt, err = client.Use(ctx, func(s *iceberg.Session) error {
		if err := apply(2); err != nil {
			return err
		}
		values, err := s.LoadNamespaceProperties([]string{"isolated"})
		if err != nil {
			return err
		}
		if values["owner"] != "first" {
			return fmt.Errorf("retained family retargeted")
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err = receiptErr(ctx, receipt); err != nil {
		return err
	}
	for _, entry := range []struct {
		ref  resource.Ref[iceberg.Handle]
		want string
	}{{follow, "second"}, {fixed, "first"}} {
		client, err := iceberg.Using(ctx, entry.ref, policy.Budget, deps)
		if err != nil {
			return err
		}
		receipt, err := client.Use(ctx, func(s *iceberg.Session) error {
			values, err := s.LoadNamespaceProperties([]string{"isolated"})
			if err != nil {
				return err
			}
			if values["owner"] != entry.want {
				return fmt.Errorf("generation mismatch")
			}
			return nil
		})
		if err != nil {
			return err
		}
		if err = receiptErr(ctx, receipt); err != nil {
			return err
		}
	}
	if err = runtime.Close(ctx); err != nil {
		return err
	}
	for state, _ := inbox.Inspect(); state.Outstanding > 0; state, _ = inbox.Inspect() {
		delivery, err := inbox.NextReleased(ctx)
		if err != nil {
			return err
		}
		if err = delivery.Ack(); err != nil {
			return err
		}
	}
	return nil
}

func dataConsumer(ctx context.Context, endpoint string) error {
	if endpoint == "" {
		return fmt.Errorf("data fixture endpoint missing")
	}
	source := iceberg.Settings{Name: "data", CatalogURI: endpoint + "/catalog", CatalogPrefix: "warehouse", Warehouse: "fixture", Namespace: []string{"isolated"}, Location: "s3://fixture/owned/", Plaintext: true, StorageEndpoint: endpoint, StorageRegion: "us-east-1", StorageAccessKey: "fixture", StorageSecretKey: "fixture", Permissions: iceberg.Grants{Read: true}}
	policy, err := iceberg.Recommend(source)
	if err != nil {
		return err
	}
	runtime, err := adapters.New(ctx, policy.Runtime)
	if err != nil {
		return err
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[iceberg.Result](policy.Evidence)
	if err != nil {
		return err
	}
	owner, err := iceberg.Open(ctx, source, iceberg.Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		return err
	}
	defer owner.Close(context.Background())
	var escaped *iceberg.Table
	receipt, err := owner.Client().Use(ctx, func(s *iceberg.Session) error {
		tbl, err := s.LoadTable([]string{"isolated", "historical"})
		if err != nil {
			return err
		}
		escaped = tbl
		seen := map[int64]bool{}
		visit := func(record arrow.RecordBatch) (bool, error) {
			ids, values := record.Column(0).(*array.Int64), record.Column(1).(*array.String)
			for row := range ids.Len() {
				id := ids.Value(row)
				if id < 0 || id >= 65 || seen[id] || values.IsNull(row) != (id%7 == 0) || !values.IsNull(row) && values.Value(row) != fmt.Sprintf("historical-%d", id) {
					return false, fmt.Errorf("independent exact row/null mismatch")
				}
				seen[id] = true
			}
			return true, nil
		}
		summary, err := tbl.IncrementalAppendRecords(iceberg.ScanOptions{BatchSize: 7}, iceberg.IncrementalRange{}, visit)
		if err != nil {
			return err
		}
		if !summary.Complete || len(seen) != 65 {
			return fmt.Errorf("independent incremental read incomplete")
		}
		clear(seen)
		scan, err := tbl.Scan(iceberg.ScanOptions{BatchSize: 9})
		if err != nil {
			return err
		}
		plan, err := scan.Plan()
		if err != nil {
			return err
		}
		if err = scan.Close(); err != nil {
			return err
		}
		summary, err = plan.Records(visit)
		if err != nil {
			return err
		}
		if !summary.Complete || len(seen) != 65 {
			return fmt.Errorf("independent retained plan incomplete")
		}
		return plan.Close()
	})
	if err != nil {
		return err
	}
	if err = receiptErr(ctx, receipt); err != nil {
		return err
	}
	if _, err = escaped.Metadata(); err == nil {
		return fmt.Errorf("independent escaped table remained live")
	}
	if err = owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
		return fmt.Errorf("independent data owner did not release: %w", err)
	}
	for state, _ := inbox.Inspect(); state.Outstanding > 0; state, _ = inbox.Inspect() {
		delivery, err := inbox.NextReleased(ctx)
		if err != nil {
			return err
		}
		if err = delivery.Ack(); err != nil {
			return err
		}
	}
	return nil
}

func receiptErr(ctx context.Context, receipt *adapters.Receipt[iceberg.Result]) error {
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil {
		return err
	}
	return snapshot.Err()
}
