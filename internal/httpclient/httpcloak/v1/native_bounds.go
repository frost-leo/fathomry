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

import "github.com/sardanioss/httpcloak/fingerprint"

func nativeDataBytes(input any, maximum int64) (int64, error) {
	count, err := fingerprint.FathomryDataBytes(input, maximum)
	if err != nil {
		return 0, failure(ErrLimit, "native-containers", err)
	}
	return count, nil
}

func nativeContainerBytes(value NativeOptionsV1) (int64, error) {
	config := value.Transport
	return nativeDataBytes(struct {
		ConnectTo                 map[string]string
		ECHConfig                 []byte
		ECHDomain, JA3, LocalAddr string
		PseudoOrder               []string
		JA3Extras                 *fingerprint.JA3Extras
		H2Settings                *fingerprint.HTTP2Settings
		TCPFingerprint            *fingerprint.TCPFingerprint
	}{config.ConnectTo, config.ECHConfig, config.ECHConfigDomain, config.CustomJA3, config.LocalAddr, config.CustomPseudoOrder, config.CustomJA3Extras, config.CustomH2Settings, config.CustomTCPFingerprint}, 1<<20)
}
