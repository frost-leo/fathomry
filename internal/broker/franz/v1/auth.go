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

package franz

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"strconv"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func tlsConfig(value settings) (*tls.Config, error) {
	if value.Plaintext {
		if value.RootCAPEM != "" {
			return nil, failure(ErrInput, "tls")
		}
		return nil, nil
	}
	if len(value.RootCAPEM) == 0 || len(value.RootCAPEM) > 64<<10 {
		return nil, failure(ErrInput, "tls")
	}
	roots := x509.NewCertPool()
	rest := bytes.TrimSpace([]byte(value.RootCAPEM))
	count := 0
	for len(rest) > 0 {
		if !bytes.HasPrefix(rest, []byte("-----BEGIN CERTIFICATE-----")) {
			return nil, failure(ErrInput, "tls")
		}
		end := bytes.Index(rest, []byte("-----END CERTIFICATE-----"))
		if end < 0 {
			return nil, failure(ErrInput, "tls")
		}
		end += len("-----END CERTIFICATE-----")
		if bytes.Count(rest[:end], []byte("-----BEGIN")) != 1 {
			return nil, failure(ErrInput, "tls")
		}
		block, trailing := pem.Decode(rest[:end])
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, failure(ErrInput, "tls")
		}
		if len(bytes.TrimSpace(trailing)) != 0 {
			return nil, failure(ErrInput, "tls")
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !certificate.IsCA || !certificate.BasicConstraintsValid {
			return nil, failure(ErrInput, "tls", err)
		}
		roots.AddCert(certificate)
		count++
		rest = bytes.TrimSpace(rest[end:])
	}
	if count == 0 {
		return nil, failure(ErrInput, "tls")
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, nil
}

func staticSASL(value settings) sasl.Mechanism {
	switch value.SASL {
	case "PLAIN":
		return plain.Auth{User: value.User, Pass: value.Password}.AsMechanism()
	case "SCRAM-SHA-256":
		return boundedSCRAM{scram.Auth{User: value.User, Pass: value.Password}.AsSha256Mechanism()}
	case "SCRAM-SHA-512":
		return boundedSCRAM{scram.Auth{User: value.User, Pass: value.Password}.AsSha512Mechanism()}
	default:
		return nil
	}
}

// The pinned SDK bounds neither SCRAM challenge splitting nor PBKDF iterations.
// Admit those resources before delegation; the SDK still owns all proof/nonce/
// signature verification and the protocol. No caller callback is accepted.
type boundedSCRAM struct{ native sasl.Mechanism }

const maxSCRAMChallengeBytes = 8 << 10
const maxSCRAMIterations = 65536

func (mechanism boundedSCRAM) Name() string { return mechanism.native.Name() }
func (mechanism boundedSCRAM) Authenticate(ctx context.Context, host string) (sasl.Session, []byte, error) {
	if ctx.Err() != nil {
		return nil, nil, failure(ErrConnect, "sasl-context", ctx.Err(), context.Cause(ctx))
	}
	session, first, err := mechanism.native.Authenticate(ctx, host)
	if err != nil {
		return nil, nil, err
	}
	return &boundedSCRAMSession{native: session, ctx: ctx}, first, nil
}

type boundedSCRAMSession struct {
	native sasl.Session
	ctx    context.Context
	step   int
}

func (session *boundedSCRAMSession) Challenge(response []byte) (bool, []byte, error) {
	if session.ctx.Err() != nil {
		return false, nil, failure(ErrConnect, "sasl-context", session.ctx.Err(), context.Cause(session.ctx))
	}
	if len(response) > maxSCRAMChallengeBytes || session.step >= 2 {
		return false, nil, failure(ErrLimit, "sasl-challenge")
	}
	if session.step == 0 {
		fields := bytes.SplitN(response, []byte(","), 4)
		if len(fields) < 3 || !bytes.HasPrefix(fields[2], []byte("i=")) {
			return false, nil, failure(ErrInput, "sasl-challenge")
		}
		iterations, err := strconv.ParseUint(string(fields[2][2:]), 10, 32)
		if err != nil || iterations < 4096 || iterations > maxSCRAMIterations {
			return false, nil, failure(ErrLimit, "sasl-iterations", err)
		}
	}
	session.step++
	return session.native.Challenge(response)
}
