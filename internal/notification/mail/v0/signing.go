/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package mail

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"io"
	"mime"
	"mime/multipart"
	stdmail "net/mail"
	"net/textproto"
	"slices"
	"strings"
	"time"

	"github.com/frost-leo/fathomry/internal/notification/mail/v0/internal/pkcs7"
	sdk "github.com/wneessen/go-mail"
)

// DKIMOptions are copied by NewDKIM. Canonicalization empty uses native relaxed.
// BodyLength zero signs the full body; a positive length deliberately leaves an
// unsigned suffix and is not recommended. Time zero selects signing time.
type DKIMOptions struct {
	private
	Domain, Selector, AUID                       string
	HeaderCanonicalization, BodyCanonicalization string
	SignHeaders, OversignHeaders                 []string
	BodyLength                                   int64
	ExpiresIn                                    time.Duration
	Time                                         time.Time
}

// DKIM owns parsed RSA/Ed25519 key material and immutable options. A fresh native
// signer is constructed for every use; neither keys nor mutable signers escape.
// The selected SDK rejects arbitrary crypto.Signer implementations.
type DKIM struct {
	private
	options DKIMOptions
	key     crypto.Signer
}

func NewDKIM(options DKIMOptions, keyPEM []byte) (*DKIM, error) {
	if !hostValid(options.Domain) || !token(options.Selector, 253) || options.Selector == "" || !line(options.AUID, 320) ||
		len(options.SignHeaders) > 64 || len(options.OversignHeaders) > 64 || options.BodyLength < 0 || options.BodyLength > 32<<20 ||
		options.ExpiresIn < 0 || options.ExpiresIn > 365*24*time.Hour || len(keyPEM) > 64<<10 {
		return nil, failure(ErrInput, "dkim")
	}
	if options.AUID != "" {
		at := strings.LastIndexByte(options.AUID, '@')
		if at < 0 || strings.HasPrefix(options.AUID, "<") {
			return nil, failure(ErrInput, "dkim-auid")
		}
		local, domain := options.AUID[:at], options.AUID[at+1:]
		if local != "" {
			parsed, err := stdmail.ParseAddress(options.AUID)
			if err != nil || parsed.Name != "" {
				return nil, failure(ErrInput, "dkim-auid", err)
			}
		}
		if !hostValid(domain) {
			return nil, failure(ErrInput, "dkim-auid-domain")
		}
		domain, signingDomain := strings.ToLower(domain), strings.ToLower(options.Domain)
		if domain != signingDomain && !strings.HasSuffix(domain, "."+signingDomain) {
			return nil, failure(ErrInput, "dkim-auid-domain")
		}
		var encoded strings.Builder
		const digits = "0123456789ABCDEF"
		for _, char := range []byte(local) {
			if char < 33 || char > 126 || char == ';' || char == '=' {
				encoded.WriteByte('=')
				encoded.WriteByte(digits[char>>4])
				encoded.WriteByte(digits[char&15])
			} else {
				encoded.WriteByte(char)
			}
		}
		options.AUID = encoded.String() + "@" + domain
	}
	for _, mode := range []string{options.HeaderCanonicalization, options.BodyCanonicalization} {
		if mode != "" && mode != "simple" && mode != "relaxed" {
			return nil, failure(ErrInput, "dkim-canonicalization")
		}
	}
	for _, names := range [][]string{options.SignHeaders, options.OversignHeaders} {
		for _, name := range names {
			if !token(name, 64) || name == "" {
				return nil, failure(ErrInput, "dkim-header")
			}
		}
	}
	key, err := sdk.PrivKeyFromPEM(bytes.Clone(keyPEM))
	if err != nil {
		return nil, failure(ErrInput, "dkim-key", err)
	}
	if rsaKey, ok := key.(*rsa.PrivateKey); ok && (rsaKey.N.BitLen() < 1024 || rsaKey.N.BitLen() > 8192) {
		return nil, failure(ErrLimit, "dkim-key")
	}
	options.SignHeaders, options.OversignHeaders = slices.Clone(options.SignHeaders), slices.Clone(options.OversignHeaders)
	value := &DKIM{options: options, key: key}
	if err := value.signer().ValidateConfig(); err != nil {
		return nil, failure(ErrInput, "dkim", err)
	}
	return value, nil
}
func (value *DKIM) signer() *sdk.DKIMSigner {
	input := value.options
	signer := sdk.NewDKIMSigner(input.Domain, input.Selector, value.key)
	signer.AUID(input.AUID)
	signer.Bodylength(input.BodyLength)
	signer.ExpiresIn(input.ExpiresIn)
	signer.HeaderCanonicalization(sdk.Canonicalization(input.HeaderCanonicalization))
	signer.BodyCanonicalization(sdk.Canonicalization(input.BodyCanonicalization))
	signer.SignHeaders(slices.Clone(input.SignHeaders)...)
	signer.OversignHeaders(slices.Clone(input.OversignHeaders)...)
	if !input.Time.IsZero() {
		signer.NowFunc = func() time.Time { return input.Time }
	}
	return signer
}
func (value *DKIM) sign(ctx context.Context, wire []byte, limit int) ([]byte, error) {
	if value == nil || value.key == nil {
		return nil, failure(ErrInput, "dkim")
	}
	if len(wire) > limit {
		return nil, failure(ErrLimit, "dkim-input")
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	headers, body, ok := bytes.Cut(wire, []byte("\r\n\r\n"))
	if !ok {
		return nil, failure(ErrInput, "dkim-mime")
	}
	signature, err := value.signer().Sign(headers, body)
	if err != nil {
		return nil, failure(ErrInput, "dkim", err)
	}
	if len(signature) > limit-len(wire) {
		return nil, failure(ErrLimit, "dkim-output")
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	result := append([]byte(signature), wire...)
	if err := checkMIME(result); err != nil {
		return nil, err
	}
	return result, nil
}

// SMIME owns a parsed RSA/ECDSA certificate/key pair. Only the first intermediate
// is used by go-mail v0.8.1; certificate trust or recipient verification is not
// established by signing. PEM inputs are copied/parsed and never retained.
type SMIME struct {
	private
	certificate tls.Certificate
}

func NewSMIME(certificatePEM, keyPEM []byte) (*SMIME, error) {
	if len(certificatePEM) > 128<<10 || len(keyPEM) > 64<<10 {
		return nil, failure(ErrLimit, "smime")
	}
	certificate, err := tls.X509KeyPair(bytes.Clone(certificatePEM), bytes.Clone(keyPEM))
	if err != nil {
		return nil, failure(ErrInput, "smime", err)
	}
	if key, ok := certificate.PrivateKey.(*rsa.PrivateKey); ok && (key.N.BitLen() < 1024 || key.N.BitLen() > 8192) {
		return nil, failure(ErrLimit, "smime-key")
	}
	value := &SMIME{certificate: certificate}
	if err := value.apply(sdk.NewMsg()); err != nil {
		return nil, err
	}
	return value, nil
}
func (value *SMIME) apply(message *sdk.Msg) error {
	if value == nil || len(value.certificate.Certificate) == 0 {
		return failure(ErrInput, "smime")
	}
	if err := message.SignWithTLSCertificate(&value.certificate); err != nil {
		return failure(ErrInput, "smime", err)
	}
	return nil
}

// sign uses the selected SDK's unchanged CMS primitive over the exact corrected
// MIME entity. Neither signing nor emission re-enters its defective 7bit writer.
func (value *SMIME) sign(ctx context.Context, wire []byte, limit int, boundary string) ([]byte, error) {
	if value == nil || len(value.certificate.Certificate) == 0 {
		return nil, failure(ErrInput, "smime")
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	parsed, err := stdmail.ReadMessage(bytes.NewReader(wire))
	if err != nil {
		return nil, err
	}
	outer, content := textproto.MIMEHeader{}, textproto.MIMEHeader{}
	for key, values := range parsed.Header {
		if strings.HasPrefix(strings.ToLower(key), "content-") {
			content[key] = values
		} else {
			outer[key] = values
		}
	}
	entity := &boundedWriter{ctx: ctx, limit: limit}
	if err = writeHeaders(entity, content); err != nil {
		return nil, err
	}
	if _, err = io.Copy(entity, parsed.Body); err != nil {
		return nil, err
	}
	certificateBytes := 0
	for _, certificate := range value.certificate.Certificate[:min(2, len(value.certificate.Certificate))] {
		certificateBytes += len(certificate)
	}
	if certificateBytes > limit {
		return nil, failure(ErrLimit, "smime-certificate")
	}
	signed, err := pkcs7.NewSignedData(entity.buffer.Bytes())
	if err != nil {
		return nil, err
	}
	leaf, err := x509.ParseCertificate(value.certificate.Certificate[0])
	if err != nil {
		return nil, err
	}
	if err = signed.AddSigner(leaf, value.certificate.PrivateKey, pkcs7.SignerInfoConfig{}); err != nil {
		return nil, err
	}
	if len(value.certificate.Certificate) > 1 {
		intermediate, err := x509.ParseCertificate(value.certificate.Certificate[1])
		if err != nil {
			return nil, err
		}
		signed.AddCertificate(intermediate)
	}
	signed.Detach()
	signature, err := signed.Finish()
	if err != nil {
		return nil, err
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if boundary == "" {
		boundary = multipart.NewWriter(io.Discard).Boundary()
	}
	if bytes.Contains(entity.buffer.Bytes(), []byte("\r\n--"+boundary)) {
		return nil, failure(ErrInput, "boundary-collision")
	}
	outer.Set("Content-Type", mime.FormatMediaType("multipart/signed", map[string]string{"boundary": boundary, "protocol": "application/pkcs7-signature", "micalg": "sha-256"}))
	writer := &boundedWriter{ctx: ctx, limit: limit}
	if err = writeHeaders(writer, outer); err != nil {
		return nil, err
	}
	if _, err = io.WriteString(writer, "--"+boundary+"\r\n"); err != nil {
		return nil, err
	}
	if _, err = writer.Write(entity.buffer.Bytes()); err != nil {
		return nil, err
	}
	if _, err = io.WriteString(writer, "\r\n--"+boundary+"\r\nContent-Type: application/pkcs7-signature; name=smime.p7s\r\nContent-Transfer-Encoding: base64\r\n\r\n"); err != nil {
		return nil, err
	}
	encoded := base64.StdEncoding.EncodeToString(signature)
	for encoded != "" {
		length := min(76, len(encoded))
		if _, err = io.WriteString(writer, encoded[:length]+"\r\n"); err != nil {
			return nil, err
		}
		encoded = encoded[length:]
	}
	if _, err = io.WriteString(writer, "\r\n--"+boundary+"--\r\n"); err != nil {
		return nil, err
	}
	if err = checkMIME(writer.buffer.Bytes()); err != nil {
		return nil, err
	}
	return writer.buffer.Bytes(), nil
}

// Unsubscribe constructs standard List-Unsubscribe fields using the native URI
// validation for one-click HTTPS. It makes no network request.
func Unsubscribe(oneClick string, additional ...string) ([]Header, error) {
	if len(additional) > 16 || !line(oneClick, 2048) {
		return nil, failure(ErrInput, "unsubscribe")
	}
	for _, uri := range additional {
		if !line(uri, 2048) || strings.ContainsAny(uri, "<>") {
			return nil, failure(ErrInput, "unsubscribe")
		}
	}
	message := sdk.NewMsg()
	if oneClick != "" {
		if err := message.SetListUnsubscribeOneClick(oneClick, additional...); err != nil {
			return nil, failure(ErrInput, "unsubscribe", err)
		}
	} else {
		message.SetListUnsubscribe(additional...)
	}
	// Construct the same preformatted native representation without exposing Msg.
	values := additional
	if oneClick != "" {
		values = append([]string{oneClick}, additional...)
	}
	wrapped := make([]string, len(values))
	for index, value := range values {
		wrapped[index] = "<" + value + ">"
	}
	result := []Header{{Name: "List-Unsubscribe", Values: []string{strings.Join(wrapped, ", ")}, Preformatted: true}}
	if oneClick != "" {
		result = append(result, Header{Name: "List-Unsubscribe-Post", Values: []string{"List-Unsubscribe=One-Click"}, Preformatted: true})
	}
	return result, nil
}
