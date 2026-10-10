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

package temporal

import (
	native "github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"slices"
)

// Prepared freezes the exact selected settings/runtime containers without
// constructing a Client or invoking native extension callbacks.
type Prepared struct {
	private
	native   native.Prepared
	settings Settings
	metadata native.Metadata
	parent   *Client
}

// Prepare is offline. Runtime objects remain borrowed for every resulting source;
// mutation of those objects while in use is outside this contract.
func Prepare(value Settings, options NativeOptions) (Prepared, error) {
	prepared, err := native.PrepareV1(value.native(), options.native())
	if err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	return preparedPublic(value, prepared)
}

func preparedPublic(value Settings, prepared native.Prepared) (Prepared, error) {
	if value.MaxUses == 0 {
		value.MaxUses = 64
	}
	if value.InnerEvidenceCapacity == 0 {
		value.InnerEvidenceCapacity = 64
	}
	if value.FamilyLimit == 0 {
		value.FamilyLimit = 64
	}
	if value.MaxUses < 1 || value.MaxUses > 1024 || value.InnerEvidenceCapacity < 1 || value.InnerEvidenceCapacity > 65535 || value.FamilyLimit < 3 || value.FamilyLimit > 1024 || value.WorkerWorkBytes < 0 || value.WorkerWorkBytes > 1<<40 {
		return Prepared{}, fail(ErrInput, "prepare")
	}
	minimum := int64(value.FamilyLimit) * prepared.Metadata().WorkBytes
	if value.WorkerWorkBytes == 0 {
		value.WorkerWorkBytes = minimum
	}
	if value.WorkerWorkBytes < minimum {
		return Prepared{}, fail(ErrLimit, "worker-envelope")
	}
	effective := prepared.Options()
	value = Settings{Name: effective.Name, Version: effective.Version, Endpoint: effective.Endpoint, Namespace: effective.Namespace,
		Identity: effective.Identity, Lazy: effective.Lazy, Plaintext: effective.Plaintext,
		RootCAPEM: effective.RootCAPEM, CertificatePEM: effective.CertificatePEM, PrivateKeyPEM: effective.PrivateKeyPEM,
		ServerName: effective.ServerName, APIKey: effective.APIKey, RPCs: effective.RPCs,
		ConnectTimeout: effective.ConnectTimeout, RPCTimeout: effective.RPCTimeout, AdmissionTimeout: effective.AdmissionTimeout,
		MaxActive: effective.MaxActive, QueuedCalls: effective.QueuedCalls, MaxRequestBytes: effective.MaxRequestBytes,
		MaxResponseBytes: effective.MaxResponseBytes, MaxUses: value.MaxUses, InnerEvidenceCapacity: value.InnerEvidenceCapacity, FamilyLimit: value.FamilyLimit, WorkerWorkBytes: value.WorkerWorkBytes}
	return Prepared{native: prepared, settings: value, metadata: prepared.Metadata()}, nil
}

// PrepareFromExisting freezes an independently owned namespace derived from an
// existing retained Client. It performs no native acquisition or extension call.
// Its transport/authentication are inherited; ordinary runtime extensions remain
// explicit. Closing the parent before Open refuses construction.
func PrepareFromExisting(value Settings, options NativeOptions, parent *Client) (Prepared, error) {
	if parent == nil || parent.use == nil {
		return Prepared{}, fail(ErrInput, "prepare-shared")
	}
	nativePrepared, err := native.PrepareFromExistingV1(value.native(), options.native(), parent.raw, parent.use.owner.policy.nativeLimits.Bytes)
	if err != nil {
		return Prepared{}, translate(err, "prepare")
	}
	prepared, err := preparedPublic(value, nativePrepared)
	if err != nil {
		return Prepared{}, err
	}
	policy, err := prepared.Policy()
	if err != nil {
		return Prepared{}, err
	}
	prepared.native, err = native.PrepareFromExistingV1(value.native(), options.native(), parent.raw, policy.nativeLimits.Bytes)
	prepared.parent = parent
	return prepared, translate(err, "prepare")
}

// Settings returns deliberately sensitive, detached effective configuration.
func (prepared Prepared) Settings() Settings {
	result := prepared.settings
	result.RPCs = slices.Clone(result.RPCs)
	return result
}

// KnownRPCs is the selected generated service inventory, not a grant or a claim
// that the connected Server supports every listed method.
func KnownRPCs() []string { return native.KnownRPCs() }
