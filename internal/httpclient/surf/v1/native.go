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
	"reflect"
	"slices"

	sdk "github.com/enetx/surf"
	utls "github.com/refraction-networking/utls"
)

func copyNative(input NativeOptionsV1) (NativeOptionsV1, error) {
	result := input
	if nilLike(input.Jar) || len(input.RequestMiddleware) > 32 || len(input.ResponseMiddleware) > 32 ||
		!headerFits(input.Headers, 1<<20, true) {
		return result, failure(ErrInput, "native-options")
	}
	result.Headers = input.Headers.Clone()
	result.RequestMiddleware = slices.Clone(input.RequestMiddleware)
	result.ResponseMiddleware = slices.Clone(input.ResponseMiddleware)
	for _, fn := range result.RequestMiddleware {
		if fn == nil {
			return result, failure(ErrInput, "request-middleware")
		}
	}
	for _, fn := range result.ResponseMiddleware {
		if fn == nil {
			return result, failure(ErrInput, "response-middleware")
		}
	}
	if input.Profile != nil {
		if input.OS < 0 || input.OS > 4 {
			return result, failure(ErrInput, "native-os")
		}
		profile := *input.Profile
		if profile.ConfigureH2 == nil || profile.ConfigureH3 == nil || profile.BuildHeaders == nil || profile.Headers == nil || profile.Boundary == nil {
			return result, failure(ErrInput, "native-profile")
		}
		if profile.HelloSpec != nil {
			if input.HelloSpecFactory == nil {
				if err := validateSpec(*profile.HelloSpec); err != nil {
					return result, err
				}
				copy := sdk.FathomryCopyHelloSpec(*profile.HelloSpec)
				profile.HelloSpec = &copy
			} else {
				profile.HelloSpec = &utls.ClientHelloSpec{}
			}
		}
		if profile.HelloID.Seed != nil {
			seed := *profile.HelloID.Seed
			profile.HelloID.Seed = &seed
		}
		if profile.HelloID.Weights != nil {
			weights := *profile.HelloID.Weights
			profile.HelloID.Weights = &weights
		}
		result.Profile = &profile
	}
	var err error
	result.TLSConfig, err = copyTLS(input.TLSConfig)
	if err != nil {
		return result, err
	}
	result.ProxyTLSConfig, err = copyTLS(input.ProxyTLSConfig)
	if err != nil {
		return result, err
	}
	result.JAConfig, err = copyJA(input.JAConfig)
	return result, err
}
func validateNative(value settings, native NativeOptionsV1) error {
	ja := native.Profile != nil || native.HelloSpecFactory != nil
	if !headerFits(native.Headers, value.MaxHeaderBytes, true) {
		return failure(ErrLimit, "native-headers")
	}
	if native.JAConfig != nil && !ja {
		return failure(ErrInput, "unused-ja-config")
	}
	if value.Mode == PreferHTTP3 && (native.JAConfig != nil || native.HelloSpecFactory != nil) {
		return failure(ErrUnsupported, "http3-does-not-use-ja")
	}
	if ja && value.Mode != PreferHTTP3 && native.JAConfig == nil && native.TLSConfig != nil {
		config := native.TLSConfig
		if len(config.Certificates) > 0 || config.GetClientCertificate != nil || config.VerifyConnection != nil || config.ClientSessionCache != nil ||
			len(config.CipherSuites) > 0 || len(config.CurvePreferences) > 0 || len(config.EncryptedClientHelloConfigList) > 0 {
			return failure(ErrUnsupported, "native-ja-config-required")
		}
	}
	return nil
}

func validateSpec(spec utls.ClientHelloSpec) error {
	if len(spec.Extensions) > 256 || len(spec.CipherSuites) > 512 || len(spec.CompressionMethods) > 256 {
		return failure(ErrLimit, "native-spec")
	}
	for _, extension := range spec.Extensions {
		if extension == nil || nilLike(extension) {
			return failure(ErrInput, "native-extension")
		}
		kind := reflect.TypeOf(extension)
		for kind.Kind() == reflect.Pointer {
			kind = kind.Elem()
		}
		if kind.PkgPath() != "github.com/refraction-networking/utls" {
			return failure(ErrUnsupported, "opaque-extension-needs-factory")
		}
		switch value := extension.(type) {
		case *utls.SessionTicketExtension:
			if value.Session != nil {
				return failure(ErrUnsupported, "session-spec-needs-factory")
			}
		case *utls.UtlsPreSharedKeyExtension:
			if value.Session != nil {
				return failure(ErrUnsupported, "session-spec-needs-factory")
			}
		}
	}
	budget := int64(1 << 20)
	if !boundedValue(reflect.ValueOf(spec), &budget, make(map[uintptr]bool), 0) {
		return failure(ErrLimit, "native-spec-storage")
	}
	return nil
}
func boundedValue(value reflect.Value, budget *int64, active map[uintptr]bool, depth int) bool {
	if depth > 48 || *budget < 0 {
		return false
	}
	*budget -= 16
	switch value.Kind() {
	case reflect.Interface:
		if !value.IsNil() {
			return boundedValue(value.Elem(), budget, active, depth+1)
		}
	case reflect.Pointer:
		if value.IsNil() {
			return true
		}
		address := value.Pointer()
		if active[address] {
			return false
		}
		active[address] = true
		ok := boundedValue(value.Elem(), budget, active, depth+1)
		delete(active, address)
		return ok
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			if !boundedValue(value.Field(index), budget, active, depth+1) {
				return false
			}
		}
	case reflect.String:
		*budget -= int64(value.Len())
	case reflect.Slice, reflect.Array:
		if int64(value.Len()) > *budget {
			return false
		}
		if value.Type().Elem().Kind() >= reflect.Int && value.Type().Elem().Kind() <= reflect.Complex128 {
			*budget -= int64(value.Len()) * int64(value.Type().Elem().Size())
			return *budget >= 0
		}
		for index := 0; index < value.Len(); index++ {
			if !boundedValue(value.Index(index), budget, active, depth+1) {
				return false
			}
		}
	case reflect.Map:
		if value.Len() > 256 {
			return false
		}
		iter := value.MapRange()
		for iter.Next() {
			if !boundedValue(iter.Key(), budget, active, depth+1) || !boundedValue(iter.Value(), budget, active, depth+1) {
				return false
			}
		}
	case reflect.Chan, reflect.UnsafePointer:
		return value.IsNil()
	}
	return *budget >= 0
}
func copyTLS(input *tls.Config) (*tls.Config, error) {
	if input == nil {
		return nil, nil
	}
	result := input.Clone()
	if nilLike(input.Rand) || nilLike(input.KeyLogWriter) || len(input.Certificates) > 32 || len(input.NextProtos) > 32 || len(input.CipherSuites) > 512 || len(input.CurvePreferences) > 256 || len(input.NameToCertificate) > 32 {
		return nil, failure(ErrInput, "tls-config")
	}
	if input.RootCAs != nil {
		result.RootCAs = input.RootCAs.Clone()
	}
	if input.ClientCAs != nil {
		result.ClientCAs = input.ClientCAs.Clone()
	}
	result.NextProtos = slices.Clone(input.NextProtos)
	result.CipherSuites = slices.Clone(input.CipherSuites)
	result.CurvePreferences = slices.Clone(input.CurvePreferences)
	result.EncryptedClientHelloConfigList = slices.Clone(input.EncryptedClientHelloConfigList)
	result.Certificates = make([]tls.Certificate, len(input.Certificates))
	for index, certificate := range input.Certificates {
		copy, err := copyCertificate(certificate)
		if err != nil {
			return nil, err
		}
		result.Certificates[index] = copy
	}
	if input.NameToCertificate != nil {
		result.NameToCertificate = make(map[string]*tls.Certificate, len(input.NameToCertificate))
		for name, certificate := range input.NameToCertificate {
			if certificate == nil {
				return nil, failure(ErrInput, "certificate")
			}
			copy, err := copyCertificate(*certificate)
			if err != nil {
				return nil, err
			}
			result.NameToCertificate[name] = &copy
		}
	}
	return result, nil
}
func certificateBytes(input [][]byte) ([][]byte, error) {
	if len(input) > 128 {
		return nil, failure(ErrLimit, "certificate")
	}
	result := make([][]byte, len(input))
	var size int
	for index, value := range input {
		size += len(value)
		if size > 1<<20 {
			return nil, failure(ErrLimit, "certificate")
		}
		result[index] = slices.Clone(value)
	}
	return result, nil
}
func copyCertificate(input tls.Certificate) (tls.Certificate, error) {
	result := input
	var err error
	result.Certificate, err = certificateBytes(input.Certificate)
	if err != nil {
		return result, err
	}
	result.SignedCertificateTimestamps, err = certificateBytes(input.SignedCertificateTimestamps)
	if err != nil {
		return result, err
	}
	if len(input.OCSPStaple) > 1<<20 || nilLike(input.PrivateKey) {
		return result, failure(ErrInput, "certificate")
	}
	result.OCSPStaple = slices.Clone(input.OCSPStaple)
	result.SupportedSignatureAlgorithms = slices.Clone(input.SupportedSignatureAlgorithms)
	if input.Leaf != nil {
		result.Leaf, err = x509.ParseCertificate(slices.Clone(input.Leaf.Raw))
	}
	return result, err
}
func copyJA(input *utls.Config) (*utls.Config, error) {
	if input == nil {
		return nil, nil
	}
	result := input.Clone()
	if nilLike(input.Rand) || nilLike(input.KeyLogWriter) || len(input.Certificates) > 32 || len(input.NameToCertificate) > 32 {
		return nil, failure(ErrInput, "ja-config")
	}
	if input.RootCAs != nil {
		result.RootCAs = input.RootCAs.Clone()
	}
	if input.ClientCAs != nil {
		result.ClientCAs = input.ClientCAs.Clone()
	}
	result.NextProtos = slices.Clone(input.NextProtos)
	result.CipherSuites = slices.Clone(input.CipherSuites)
	result.CurvePreferences = slices.Clone(input.CurvePreferences)
	result.EncryptedClientHelloConfigList = slices.Clone(input.EncryptedClientHelloConfigList)
	result.Certificates = make([]utls.Certificate, len(input.Certificates))
	clone := func(input utls.Certificate) (utls.Certificate, error) {
		result := input
		var err error
		result.Certificate, err = certificateBytes(input.Certificate)
		if err != nil {
			return result, err
		}
		result.SignedCertificateTimestamps, err = certificateBytes(input.SignedCertificateTimestamps)
		if err != nil {
			return result, err
		}
		if len(input.OCSPStaple) > 1<<20 || nilLike(input.PrivateKey) {
			return result, failure(ErrInput, "ja-certificate")
		}
		result.OCSPStaple = slices.Clone(input.OCSPStaple)
		result.SupportedSignatureAlgorithms = slices.Clone(input.SupportedSignatureAlgorithms)
		if input.Leaf != nil {
			result.Leaf, err = x509.ParseCertificate(slices.Clone(input.Leaf.Raw))
		}
		return result, err
	}
	for index, certificate := range input.Certificates {
		value, err := clone(certificate)
		if err != nil {
			return nil, err
		}
		result.Certificates[index] = value
	}
	if input.NameToCertificate != nil {
		result.NameToCertificate = make(map[string]*utls.Certificate, len(input.NameToCertificate))
		for name, certificate := range input.NameToCertificate {
			if certificate == nil {
				return nil, failure(ErrInput, "ja-certificate")
			}
			value, err := clone(*certificate)
			if err != nil {
				return nil, err
			}
			result.NameToCertificate[name] = &value
		}
	}
	return result, nil
}
