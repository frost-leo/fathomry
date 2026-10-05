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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/twmb/franz-go/pkg/kfake"
)

func testKafkaTrust(t *testing.T) (*tls.Config, string) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: private}}},
		string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

func TestStaticSASLRequiresVerifiedTLS(t *testing.T) {
	for _, mechanism := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"} {
		t.Run(mechanism, func(t *testing.T) {
			secure, roots := testKafkaTrust(t)
			const user, password = "gh-105-user", "gh-105-secret-canary"
			cluster := localCluster(t, kfake.TLS(secure), kfake.EnableSASL(), kfake.Superuser(mechanism, user, password))
			options := clusterOptions(cluster)
			options.Plaintext = false
			options.RootCAPEM = roots
			options.SASL, options.User, options.Password = mechanism, user, password
			fixture := bindFixture(t, options, 3)
			position := produce(t, fixture.client, "authenticated", Message{Topic: "records", Value: []byte("authenticated")}).WritesCopy()[0].Position
			if got := readResult(t, fixture.client, "auth-read", position); got.State != ReadFound {
				t.Fatal(got.Err)
			}
			for _, bad := range []string{"password", "trust", "plaintext", "pem-junk"} {
				wrong := options
				switch bad {
				case "password":
					wrong.Password = "denied-canary"
				case "trust":
					_, wrong.RootCAPEM = testKafkaTrust(t)
				case "plaintext":
					wrong.Plaintext = true
					wrong.RootCAPEM = ""
				case "pem-junk":
					wrong.RootCAPEM += "PRIVATE-TRAILING-CANARY"
				}
				selected, err := Select(wrong)
				if err == nil {
					selected = resource.WithLimits(selected, LimitsV1(wrong))
					assembly, openErr := resource.Assemble(deadline(t), deadline(t), "denied", selected)
					err = openErr
					if assembly != nil {
						if closeErr := assembly.Close(deadline(t)); closeErr != nil {
							t.Fatal(closeErr)
						}
					}
				}
				if err == nil {
					t.Fatal("invalid security profile accepted", bad)
				}
				if strings.Contains(fmt.Sprintf("%+v %#v", err, wrong), password) {
					t.Fatal("secret escaped formatting")
				}
				if encoded, _ := json.Marshal(wrong); bytes.Contains(encoded, []byte(password)) {
					t.Fatal("secret escaped JSON")
				}
			}
		})
	}
}

type challengeWitness struct{ calls int }

func (witness *challengeWitness) Challenge([]byte) (bool, []byte, error) {
	witness.calls++
	return false, []byte("native-delegation"), nil
}
func TestSCRAMAdmissionPrecedesNativeExpansion(t *testing.T) {
	for _, input := range [][]byte{
		[]byte("r=nonce,s=c2FsdA==,i=2147483647"),
		[]byte("r=nonce,s=c2FsdA==,i=65537"),
		[]byte("r=nonce,s=c2FsdA==,i=4095"),
		bytes.Repeat([]byte("x"), maxSCRAMChallengeBytes+1),
	} {
		witness := new(challengeWitness)
		session := &boundedSCRAMSession{native: witness, ctx: context.Background()}
		if _, _, err := session.Challenge(input); !errors.Is(err, ErrLimit) || witness.calls != 0 {
			t.Fatal("native work began before SCRAM resource admission")
		}
	}
	witness := new(challengeWitness)
	ctx, cancel := context.WithCancel(context.Background())
	session := &boundedSCRAMSession{native: witness, ctx: ctx}
	if _, _, err := session.Challenge([]byte("r=nonce,s=c2FsdA==,i=4096")); err != nil || witness.calls != 1 {
		t.Fatal("valid bounded challenge was not delegated")
	}
	cancel()
	if _, _, err := session.Challenge([]byte("v=signature")); !errors.Is(err, context.Canceled) || witness.calls != 1 {
		t.Fatal("canceled native auth resumed")
	}
}
func TestTLSBundleRefusesSkippedMalformedBlocks(t *testing.T) {
	_, roots := testKafkaTrust(t)
	for _, value := range []string{"-----BEGIN CERTIFICATE-----\ninvalid\n" + roots, roots + "trailing", "unrelated\n" + roots} {
		if _, err := tlsConfig(settings{RootCAPEM: value}); !errors.Is(err, ErrInput) {
			t.Fatal("partial PEM acceptance bypassed the trust policy")
		}
	}
}
