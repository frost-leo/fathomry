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
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go/encryption"
	nativeio "github.com/apache/iceberg-go/io"
)

type managedTestEncryption struct {
	startErr, closeErr        error
	inputCloses, outputCloses atomic.Int32
	key                       []byte
}
type managedTestEncryptedInput struct {
	nativeio.File
	manager *managedTestEncryption
}
type managedTestEncryptedOutput struct {
	nativeio.FileWriter
	manager *managedTestEncryption
}

func (m *managedTestEncryption) NewDecryptedInputFile(_ context.Context, file nativeio.File, metadata encryption.EncryptionKeyMetadata) (encryption.EncryptedInputFile, error) {
	if len(metadata) != 0 {
		metadata[0] = 99
	}
	return &managedTestEncryptedInput{file, m}, m.startErr
}
func (m *managedTestEncryption) NewEncryptedOutputFile(_ context.Context, writer nativeio.FileWriter, _ string) (encryption.EncryptedOutputFile, error) {
	if m.startErr != nil {
		if _, err := writer.Write([]byte("partial setup output")); err != nil {
			return nil, errors.Join(m.startErr, err)
		}
	}
	return &managedTestEncryptedOutput{writer, m}, m.startErr
}
func (f *managedTestEncryptedInput) KeyMetadata() encryption.EncryptionKeyMetadata {
	return f.manager.key
}
func (f *managedTestEncryptedOutput) KeyMetadata() encryption.EncryptionKeyMetadata {
	return f.manager.key
}
func (f *managedTestEncryptedInput) Close() error {
	f.manager.inputCloses.Add(1)
	return errors.Join(f.File.Close(), f.manager.closeErr)
}
func (f *managedTestEncryptedOutput) Close() error {
	f.manager.outputCloses.Add(1)
	return errors.Join(f.FileWriter.Close(), f.manager.closeErr)
}
func (*managedTestEncryptedInput) NativeDecryptionProperties(context.Context) (any, error) {
	return "native-input", nil
}
func (*managedTestEncryptedOutput) NativeEncryptionProperties(context.Context) (any, error) {
	return "native-output", nil
}

func TestManagedEncryptionPartialStartupAndCleanup(t *testing.T) {
	for _, write := range []bool{false, true} {
		t.Run(fmt.Sprint(write), func(t *testing.T) {
			peer, options := newPeer(t)
			peer.objects["owned/input"] = []byte("fixture")
			startErr, closeErr := errors.New("encryption setup failed"), errors.New("encryption release failed")
			manager := &managedTestEncryption{startErr: startErr, closeErr: closeErr}
			f := bindManagedNativeFixture(t, managedSettingsFor(options), ManagedNativeOptions{Encryption: manager}, 1)
			_, err := f.outcome(t, "encryption-startup", func(s *ManagedSession) error {
				if write {
					_, err := s.CreateFile(options.Location+"output", "selected-key", func(encryption.EncryptedOutputFile) error {
						return fmt.Errorf("visitor must not run after setup error")
					})
					return err
				}
				return s.ReadFile(options.Location+"input", nil, func(encryption.EncryptedInputFile) error {
					return fmt.Errorf("visitor must not run after setup error")
				})
			})
			if !errors.Is(err, startErr) || !errors.Is(err, closeErr) {
				t.Fatalf("independent setup/cleanup errors lost: %s", causes(err))
			}
			if write && manager.outputCloses.Load() != 1 || !write && manager.inputCloses.Load() != 1 {
				t.Fatal("partially constructed encryption owner not released exactly once")
			}
			peer.mu.Lock()
			_, outputExists := peer.objects["owned/output"]
			peer.mu.Unlock()
			if outputExists {
				t.Fatal("failed encryption setup published partial plaintext")
			}
		})
	}
}

func TestManagedEncryptionNativeExtensionsAndKeyCopies(t *testing.T) {
	peer, options := newPeer(t)
	manager := &managedTestEncryption{key: []byte{1, 2, 3}}
	f := bindManagedNativeFixture(t, managedSettingsFor(options), ManagedNativeOptions{Encryption: manager}, 1)
	f.run(t, "encryption-extensions", func(s *ManagedSession) error {
		key, err := s.CreateFile(options.Location+"output", "selected-key", func(file encryption.EncryptedOutputFile) error {
			native, ok := file.(encryption.NativeEncryptionOutputFile)
			if !ok {
				return fmt.Errorf("native output extension lost")
			}
			if properties, err := native.NativeEncryptionProperties(context.Background()); err != nil || properties != "native-output" {
				return fmt.Errorf("native output properties changed")
			}
			_, err := file.Write([]byte("exact owned bytes"))
			return err
		})
		if err != nil {
			return err
		}
		key[0] = 42
		if manager.key[0] != 1 {
			return fmt.Errorf("output key metadata aliases manager")
		}
		inputKey := []byte{1, 2, 3}
		err = s.ReadFile(options.Location+"output", inputKey, func(file encryption.EncryptedInputFile) error {
			if _, ok := file.(encryption.NativeEncryptionInputFile); !ok {
				return fmt.Errorf("native input extension lost")
			}
			actual, err := io.ReadAll(file)
			if err != nil || string(actual) != "exact owned bytes" {
				return fmt.Errorf("native callback byte round trip: %w", err)
			}
			return nil
		})
		if inputKey[0] != 1 {
			return fmt.Errorf("caller key metadata was mutated")
		}
		return err
	})
	if manager.outputCloses.Load() != 1 || manager.inputCloses.Load() != 1 {
		t.Fatal("encryption owner close count mismatch")
	}
	peer.mu.Lock()
	_, exists := peer.objects["owned/output"]
	peer.mu.Unlock()
	if !exists {
		t.Fatal("successful output missing")
	}
}

func TestManagedPlaintextEncryptionFailsClosed(t *testing.T) {
	peer, options := newPeer(t)
	peer.objects["owned/input"] = []byte("fixture")
	f := bindManagedFixture(t, managedSettingsFor(options), 1)
	_, err := f.outcome(t, "plaintext-output", func(s *ManagedSession) error {
		_, err := s.CreateFile(options.Location+"output", "not-plaintext", func(encryption.EncryptedOutputFile) error {
			return fmt.Errorf("plaintext visitor must not run")
		})
		return err
	})
	if !errors.Is(err, encryption.ErrKeyIDNotSupported) {
		t.Fatalf("plaintext write failed open: %s", causes(err))
	}
	_, err = f.outcome(t, "plaintext-input", func(s *ManagedSession) error {
		return s.ReadFile(options.Location+"input", []byte("encrypted-key"), func(encryption.EncryptedInputFile) error {
			return fmt.Errorf("plaintext visitor must not run")
		})
	})
	if !errors.Is(err, encryption.ErrKeyMetadataNotSupported) {
		t.Fatalf("plaintext read failed open: %s", causes(err))
	}
}
