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
 *
 */

package surf

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enetx/g"
	"github.com/enetx/surf/profiles"
	"github.com/enetx/surf/profiles/chrome"
	"github.com/frost-leo/fathomry/internal/resource"
	utls "github.com/refraction-networking/utls"
)

func reviewPrepared(t *testing.T, options OptionsV1) Prepared {
	t.Helper()
	prepared, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	return prepared
}

func TestStaticHelloProfileMemberBoundaries(t *testing.T) {
	for _, scenario := range []string{"valid", "nil-extension", "typed-nil-extension", "extension-count", "cipher-count", "compression-count", "storage"} {
		t.Run(scenario, func(t *testing.T) {
			spec := utls.ClientHelloSpec{CipherSuites: []uint16{0x1301}, CompressionMethods: []byte{0},
				Extensions: []utls.TLSExtension{&utls.SNIExtension{ServerName: "profile.invalid"}}}
			switch scenario {
			case "nil-extension":
				spec.Extensions[0] = nil
			case "typed-nil-extension":
				spec.Extensions[0] = (*utls.SNIExtension)(nil)
			case "extension-count":
				spec.Extensions = make([]utls.TLSExtension, 257)
			case "cipher-count":
				spec.CipherSuites = make([]uint16, 513)
			case "compression-count":
				spec.CompressionMethods = make([]byte, 257)
			case "storage":
				spec.Extensions = []utls.TLSExtension{&utls.GenericExtension{Id: 1234, Data: make([]byte, 2048)}}
			}
			var callbacks atomic.Int64
			profile := chrome.Desktop
			profile.HelloSpec = &spec
			profile.BuildHeaders = func(profiles.OSKey) *g.MapOrd[g.String, g.String] {
				callbacks.Add(1)
				return nil
			}
			_, err := PrepareV1(OptionsV1{Name: "static-profile", Mode: HTTP2Only, MaxProfileBytes: 1024, Native: NativeOptionsV1{Profile: &profile}})
			if (err == nil) != (scenario == "valid") || callbacks.Load() != 0 {
				t.Fatal("static profile boundary or offline authority changed", err)
			}
		})
	}
}

func reviewCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "source-copy-probe"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{"source-copy-probe.invalid"},
		KeyUsage: x509.KeyUsageDigitalSignature, ExtraExtensions: []pkix.Extension{{Id: []int{1, 3, 6, 1, 4, 1, 55555, 1}, Value: bytes.Repeat([]byte("a"), 32<<10)}}}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

func TestCopiedNativeContainersChangeSourceMetadata(t *testing.T) {
	certificate := reviewCertificate(t)
	for _, scenario := range []struct {
		name string
		edit func(*NativeOptionsV1)
	}{
		{"std-next-protos-control", func(native *NativeOptionsV1) { native.TLSConfig.NextProtos = []string{"http/1.1"} }},
		{"std-cipher-suites", func(native *NativeOptionsV1) { native.TLSConfig.CipherSuites = make([]uint16, 128) }},
		{"std-client-ca-index", func(native *NativeOptionsV1) {
			native.TLSConfig.ClientCAs = x509.NewCertPool()
			native.TLSConfig.ClientCAs.AddCert(certificate.Leaf)
		}},
		{"std-named-certificate", func(native *NativeOptionsV1) {
			native.TLSConfig.NameToCertificate = map[string]*tls.Certificate{"source-copy-probe.invalid": &certificate}
		}},
		{"std-signature-algorithms", func(native *NativeOptionsV1) {
			native.TLSConfig.Certificates = []tls.Certificate{{SupportedSignatureAlgorithms: make([]tls.SignatureScheme, 8192)}}
		}},
		{"ja-ech-bytes", func(native *NativeOptionsV1) { native.JAConfig.EncryptedClientHelloConfigList = make([]byte, 64<<10) }},
		{"ja-cipher-suites", func(native *NativeOptionsV1) { native.JAConfig.CipherSuites = make([]uint16, 8192) }},
		{"ja-curves", func(native *NativeOptionsV1) { native.JAConfig.CurvePreferences = make([]utls.CurveID, 8192) }},
		{"ja-client-ca-index", func(native *NativeOptionsV1) {
			native.JAConfig.ClientCAs = x509.NewCertPool()
			native.JAConfig.ClientCAs.AddCert(certificate.Leaf)
		}},
		{"ja-named-certificate", func(native *NativeOptionsV1) {
			native.JAConfig.NameToCertificate = map[string]*utls.Certificate{"source-copy-probe.invalid": {Certificate: certificate.Certificate, PrivateKey: certificate.PrivateKey, Leaf: certificate.Leaf}}
		}},
		{"ja-application-settings", func(native *NativeOptionsV1) {
			native.JAConfig.ApplicationSettings = map[string][]byte{"h2": bytes.Repeat([]byte("p"), 32<<10)}
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			profile := chrome.Desktop
			options := OptionsV1{Name: "native-budget", Mode: HTTP1Only, MaxRoutes: 1, MaxTCPConnections: 1,
				Native: NativeOptionsV1{Profile: &profile, TLSConfig: &tls.Config{}, JAConfig: &utls.Config{}}}
			if scenario.name == "std-signature-algorithms" {
				options.Native.TLSConfig.Certificates = []tls.Certificate{{}}
			}
			base := reviewPrepared(t, options).Metadata()
			scenario.edit(&options.Native)
			prepared := reviewPrepared(t, options)
			changed := prepared.Metadata()
			t.Logf("source delta=%d work delta=%d", changed.SourceBytes-base.SourceBytes, changed.WorkBytes-base.WorkBytes)
			if changed.SourceBytes <= base.SourceBytes || changed.WorkBytes != base.WorkBytes {
				t.Error("copied native configuration absent from source declaration or charged to work instead")
			}
			if scenario.name == "ja-application-settings" {
				options.Native.JAConfig.ApplicationSettings["h2"][0] = 'x'
				if prepared.native.JAConfig.ApplicationSettings["h2"][0] != 'p' {
					t.Fatal("prepared client ALPS setting aliases caller mutation")
				}
			}
		})
	}
}

func TestPreparedProfileIsLazyFrozenAndAuthoritative(t *testing.T) {
	var calls atomic.Int64
	profile := chrome.Desktop
	profile.HelloSpec, profile.HelloID, profile.ShuffleExtensions = nil, utls.ClientHelloID{}, false
	originalH2, originalH3, originalHeaders, originalBoundary := profile.ConfigureH2, profile.ConfigureH3, profile.BuildHeaders, profile.Boundary
	profile.ConfigureH2 = func(config profiles.H2Config) { calls.Add(1); originalH2(config) }
	profile.ConfigureH3 = func(config profiles.H3Config) { calls.Add(1); originalH3(config) }
	profile.BuildHeaders = func(os profiles.OSKey) *g.MapOrd[g.String, g.String] { calls.Add(1); return originalHeaders(os) }
	profile.Boundary = func() g.String { calls.Add(1); return originalBoundary() }
	options := OptionsV1{Name: "prepared-lazy", Mode: H2C, MaxActive: 2, Native: NativeOptionsV1{Profile: &profile}}
	prepared, err := PrepareV1(options, resource.Layer{Kind: resource.Local, Content: []byte(`{"queued_calls":2,"max_profile_bytes":65536,"max_http2_stream_bytes":4194304,"max_http3_clients":3}`)})
	if err != nil {
		t.Fatal(err)
	}
	metadata := prepared.Metadata()
	profile.HelloSpec = &utls.ClientHelloSpec{}
	if calls.Load() != 0 || prepared.native.Profile.HelloSpec != nil || metadata.ProfileBytes != 65536 || metadata.HTTP2StreamBytes != 4<<20 ||
		metadata.Limits.Bytes != 2*metadata.WorkBytes || metadata.Limits.QueuedBytes != 2*metadata.WorkBytes {
		t.Fatal("preparation changed frozen/lazy resolved authority")
	}
	selected := resource.WithLimits(prepared.Select(), metadata.Limits)
	assembly, err := resource.Assemble(testContext(t), testContext(t), "prepared-review", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(context.Background())
	source, _, err := resource.Bind(assembly, selected)
	if err != nil || !reflect.DeepEqual(source.owner.budget, metadata) || calls.Load() != 0 {
		t.Fatal("construction changed authoritative preparation or called native profile", err)
	}
	options.Native.Profile = nil
	if _, err := PrepareV1(options, resource.Layer{Kind: resource.Local, Content: []byte(`{"max_http3_clients":0}`)}); err == nil {
		t.Fatal("explicit invalid zero restored a default")
	}
	options.Mode = HTTP1Only
	options.Native.HelloSpecFactory = func(context.Context) (utls.ClientHelloSpec, error) {
		calls.Add(1)
		return utls.ClientHelloSpec{}, nil
	}
	if _, err := PrepareV1(options); err != nil || calls.Load() != 0 {
		t.Fatal("offline preparation invoked native Hello factory", err)
	}
}

func TestNativeQuotaAndProtocolBudgetOwnership(t *testing.T) {
	options := OptionsV1{Name: "quota-budget", Mode: PreferHTTP3, MaxRoutes: 1, MaxTCPConnections: 1, MaxUDPSockets: 1, MaxHTTP3Clients: 1}
	base := reviewPrepared(t, options).Metadata()
	options.MaxHTTP3Clients = 2
	clients := reviewPrepared(t, options).Metadata()
	if clients.SourceBytes-base.SourceBytes < 15<<20 || clients.WorkBytes != base.WorkBytes {
		t.Fatal("H3 cache quota not charged to source independently of UDP")
	}
	options.MaxHTTP3Clients = 1
	options.MaxUDPSockets = 2
	packets := reviewPrepared(t, options).Metadata()
	if packets.SourceBytes-base.SourceBytes != 64<<10 || packets.WorkBytes != base.WorkBytes {
		t.Fatal("physical UDP and native H3 client declarations conflated")
	}
	options.MaxUDPSockets = 1
	options.Mode, options.MaxHTTP2StreamBytes = Negotiated, 4<<20
	lower := reviewPrepared(t, options).Metadata()
	options.MaxHTTP2StreamBytes = 8 << 20
	higher := reviewPrepared(t, options).Metadata()
	if higher.WorkBytes-lower.WorkBytes != 4<<20 || higher.SourceBytes != lower.SourceBytes {
		t.Fatal("origin H2 stream credit not charged to actual work root")
	}
	profile := chrome.Desktop
	options.Mode, options.Native = HTTP1Only, NativeOptionsV1{Profile: &profile, TLSConfig: &tls.Config{}}
	uncached := reviewPrepared(t, options).Metadata()
	options.Native.TLSConfig.ClientSessionCache = tls.NewLRUClientSessionCache(1)
	if _, err := PrepareV1(options); !errors.Is(err, ErrUnsupported) {
		t.Fatal("standard cache silently treated as an explicit JA cache", err)
	}
	options.Native.JAConfig = &utls.Config{ClientSessionCache: utls.NewLRUClientSessionCache(64)}
	override := reviewPrepared(t, options).Metadata()
	if override.SourceBytes-uncached.SourceBytes >= 64*(16<<20) || override.WorkBytes != uncached.WorkBytes {
		t.Fatal("explicit borrowed JA cache incorrectly charged as a native-owned64-entry cache")
	}
}
