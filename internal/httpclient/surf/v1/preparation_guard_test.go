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

package surf

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"strconv"
	"testing"

	"github.com/enetx/surf/profiles/chrome"
	utls "github.com/refraction-networking/utls"
)

func TestNativeContainerGuardBeforeLeafClone(t *testing.T) {
	// Deliberately invalid DER distinguishes the aggregate admission guard from
	// later clone-and-parse rejection without allocating a gigabyte of input.
	leaf := &x509.Certificate{Raw: make([]byte, 1<<20)}
	certificate := tls.Certificate{Leaf: leaf}
	config := &tls.Config{Certificates: make([]tls.Certificate, 32), NameToCertificate: make(map[string]*tls.Certificate)}
	for index := range config.Certificates {
		config.Certificates[index] = certificate
		config.NameToCertificate["name-"+strconv.Itoa(index)+".invalid"] = &certificate
	}
	options := OptionsV1{Name: "pre-clone-guard", Mode: HTTP1Only, Native: NativeOptionsV1{TLSConfig: config}}
	if _, err := PrepareV1(options); !errors.Is(err, ErrLimit) {
		t.Fatal("oversized aggregate reached clone/parse before size admission", err)
	}
	config.Certificates = config.Certificates[:1]
	config.NameToCertificate = nil
	if _, err := PrepareV1(options); err == nil || errors.Is(err, ErrLimit) {
		t.Fatal("small invalid DER control did not reach native certificate parsing", err)
	}
	config.Certificates[0] = reviewCertificate(t)
	if _, err := PrepareV1(options); err != nil {
		t.Fatal("small valid certificate control rejected", err)
	}
}

func TestJAAlternateVerificationNameIsAccounted(t *testing.T) {
	profile := chrome.Desktop
	options := OptionsV1{Name: "ja-name-budget", Mode: HTTP1Only, MaxRoutes: 1, Native: NativeOptionsV1{Profile: &profile, JAConfig: &utls.Config{}}}
	base := reviewPrepared(t, options).Metadata()
	options.Native.JAConfig.InsecureServerNameToVerify = "alternate.example.invalid"
	changed := reviewPrepared(t, options).Metadata()
	if changed.SourceBytes <= base.SourceBytes || changed.WorkBytes != base.WorkBytes {
		t.Fatal("retained JA verification name missing from source accounting")
	}
}
