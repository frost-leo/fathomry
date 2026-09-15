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

package httpcloak

import (
	"bytes"
	"maps"
	"net"
	"reflect"
	"strings"

	http "github.com/sardanioss/http"
	"github.com/sardanioss/httpcloak/fingerprint"
	"github.com/sardanioss/httpcloak/transport"
)

func nilLike(value any) bool {
	if value == nil {
		return true
	}
	ref := reflect.ValueOf(value)
	switch ref.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return ref.IsNil()
	}
	return false
}
func copyNative(value NativeOptionsV1) (NativeOptionsV1, error) {
	value.Preset = fingerprint.Clone(value.Preset)
	if value.Jar != nil && nilLike(value.Jar) {
		return NativeOptionsV1{}, failure(ErrInput, "jar")
	}
	if value.Transport == nil {
		value.Transport = &transport.TransportConfig{}
	} else {
		source := value.Transport
		if source.FathomryDialTCP != nil || source.FathomryMaxHeaderBytes != 0 || source.SessionCacheBackend != nil || source.SessionCacheErrorCallback != nil {
			return NativeOptionsV1{}, failure(ErrUnsupported, "native-ownership")
		}
		copied := *source
		copied.ConnectTo = maps.Clone(source.ConnectTo)
		copied.ECHConfig = bytes.Clone(source.ECHConfig)
		copied.CustomPseudoOrder = append([]string(nil), source.CustomPseudoOrder...)
		if source.CustomH2Settings != nil {
			item := *source.CustomH2Settings
			copied.CustomH2Settings = &item
		}
		if source.CustomTCPFingerprint != nil {
			item := *source.CustomTCPFingerprint
			copied.CustomTCPFingerprint = &item
		}
		if source.CustomJA3Extras != nil {
			copied.CustomJA3Extras = fingerprint.Clone(&fingerprint.Preset{JA3Extras: source.CustomJA3Extras}).JA3Extras
		}
		value.Transport = &copied
	}
	if value.Transport.LocalAddr != "" && net.ParseIP(value.Transport.LocalAddr) == nil {
		return NativeOptionsV1{}, failure(ErrInput, "local-address")
	}
	if len(value.Transport.ConnectTo) > 128 || len(value.Transport.ECHConfig) > 1<<16 || len(value.Transport.CustomJA3) > 1<<16 || len(value.Transport.CustomPseudoOrder) > 4 {
		return NativeOptionsV1{}, failure(ErrLimit, "native-options")
	}
	if len(value.Transport.CustomPseudoOrder) > 0 {
		allowed := map[string]bool{":method": true, ":scheme": true, ":authority": true, ":path": true}
		for _, name := range value.Transport.CustomPseudoOrder {
			if !allowed[name] {
				return NativeOptionsV1{}, failure(ErrInput, "pseudo-order")
			}
			delete(allowed, name)
		}
		if len(allowed) != 0 {
			return NativeOptionsV1{}, failure(ErrInput, "pseudo-order")
		}
	}
	if value.Verify != nil {
		copy := *value.Verify
		if copy.RootCAs != nil {
			copy.RootCAs = copy.RootCAs.Clone()
		}
		value.Verify = &copy
	}
	if value.ProxyVerify != nil {
		copy := *value.ProxyVerify
		if copy.RootCAs != nil {
			copy.RootCAs = copy.RootCAs.Clone()
		}
		value.ProxyVerify = &copy
	}
	if value.Transport.KeyLogWriter != nil && nilLike(value.Transport.KeyLogWriter) {
		return NativeOptionsV1{}, failure(ErrInput, "key-log-writer")
	}
	if value.Transport.QuicIdleTimeout < 0 {
		return NativeOptionsV1{}, failure(ErrInput, "quic-idle-timeout")
	}
	return value, nil
}
func choosePreset(value settings, native NativeOptionsV1) (*fingerprint.Preset, error) {
	count := 0
	if native.Preset != nil {
		count++
	}
	if value.PresetName != "" {
		count++
	}
	if value.PresetJSON != "" {
		count++
	}
	if count != 1 {
		return nil, failure(ErrInput, "preset-selection")
	}
	preset := fingerprint.Clone(native.Preset)
	if value.PresetName != "" {
		preset = fingerprint.GetStrict(value.PresetName)
	}
	if value.PresetJSON != "" {
		file, err := fingerprint.LoadPresetFromJSONStrict([]byte(value.PresetJSON))
		if err != nil {
			return nil, failure(ErrInput, "preset-json", err)
		}
		if file.Preset == nil {
			return nil, failure(ErrInput, "preset-json")
		}
		preset, err = fingerprint.BuildPreset(file.Preset)
		if err != nil {
			return nil, failure(ErrInput, "preset-json", err)
		}
	}
	if preset == nil {
		return nil, failure(ErrInput, "unknown-preset")
	}
	preset = fingerprint.Clone(preset)
	if err := presetHeaders(preset, value.MaxHeaderBytes); err != nil {
		return nil, err
	}
	if value.Protocol == HTTP3 && !preset.SupportHTTP3 || value.Protocol == HTTP2 && preset.DisableHTTP2 {
		return nil, failure(ErrUnsupported, "preset-protocol")
	}
	if native.Transport.CustomH2Settings != nil {
		preset.HTTP2Settings = *native.Transport.CustomH2Settings
	}
	if native.Transport.CustomTCPFingerprint != nil {
		value := native.Transport.CustomTCPFingerprint
		if value.TTL > 0 {
			preset.TCPFingerprint.TTL = value.TTL
		}
		if value.MSS > 0 {
			preset.TCPFingerprint.MSS = value.MSS
		}
		if value.WindowSize > 0 {
			preset.TCPFingerprint.WindowSize = value.WindowSize
		}
		if value.WindowScale > 0 {
			preset.TCPFingerprint.WindowScale = value.WindowScale
		}
		if value.DFBit {
			preset.TCPFingerprint.DFBit = true
		}
	}
	var err error
	if value.Protocol == HTTP3 {
		_, _, err = fingerprint.ResolveQUICClientHelloSpec(preset, false, 0)
	} else {
		_, _, err = fingerprint.ResolveClientHelloSpec(preset, native.Transport.CustomJA3, native.Transport.CustomJA3Extras, false, 0)
	}
	if err != nil {
		return nil, failure(ErrInput, "fingerprint-factory", err)
	}
	return preset, nil
}
func preview(request *http.Request) *http.Request {
	value := request.Clone(request.Context())
	value.Body = nil
	value.GetBody = nil
	value.Response = nil
	value.TLS = nil
	return value
}

func presetHeaders(preset *fingerprint.Preset, limit int64) error {
	check := func(key, value string) error {
		if !token(key) || !fieldValue(value) {
			return failure(ErrInput, "preset-headers")
		}
		for _, reserved := range []string{"Proxy-Authorization", "Content-Length", "Transfer-Encoding", "Upgrade"} {
			if strings.EqualFold(key, reserved) {
				return failure(ErrInput, "preset-headers")
			}
		}
		return nil
	}
	var size int64
	for key, value := range preset.Headers {
		if err := check(key, value); err != nil {
			return err
		}
		if len(preset.HeaderOrder) == 0 {
			size += int64(len(key) + len(value) + 4)
		}
	}
	for _, entry := range preset.HeaderOrder {
		if err := check(entry.Key, entry.Value); err != nil {
			return err
		}
		size += int64(len(entry.Key) + len(entry.Value) + 4)
	}
	if !fieldValue(preset.UserAgent) {
		return failure(ErrInput, "preset-user-agent")
	}
	if size+int64(len(preset.UserAgent)) > limit {
		return failure(ErrLimit, "preset-headers")
	}
	return nil
}

func seedPresetCredentials(request *http.Request, preset *fingerprint.Preset) {
	for _, key := range []string{"Authorization", "Cookie"} {
		present := false
		for name := range request.Header {
			if strings.EqualFold(name, key) {
				present = true
				break
			}
		}
		if present {
			continue
		}
		if len(preset.HeaderOrder) > 0 {
			for _, entry := range preset.HeaderOrder {
				if strings.EqualFold(entry.Key, key) {
					request.Header.Add(key, entry.Value)
				}
			}
		} else {
			for name, value := range preset.Headers {
				if strings.EqualFold(name, key) {
					request.Header.Add(key, value)
				}
			}
		}
	}
}
