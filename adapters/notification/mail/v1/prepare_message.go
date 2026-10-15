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
	"context"
	ht "html/template"
	"io"
	"io/fs"
	tt "text/template"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/notification/mail/v0"
)

// Input selects exactly one Bytes, borrowed Reader, absolute Path, or FS/Name.
// Files acquired here are always closed; borrowed readers are never closed.
// An arbitrary Reader cannot be forcibly terminated by a Go context.
type Input struct {
	private
	Bytes  []byte
	Reader io.Reader
	Write  func(context.Context, io.Writer) error
	Path   string
	FS     fs.FS
	Name   string
}
type FileInput struct {
	private
	Input  Input
	File   Attachment
	Inline bool
}

type PartInput struct {
	private
	Input Input
	Part  Part
}

// TemplateInput executes a supplied text/HTML template under admission. Name
// selects a named template, File selects attachment/CID instead of a body part.
// Template/data/functions remain borrowed until actual return.
type TemplateInput struct {
	private
	Text   *tt.Template
	HTML   *ht.Template
	Name   string
	Data   any
	Part   Part
	File   *Attachment
	Inline bool
}
type Transform struct {
	private
	Name  string
	Apply func(context.Context, Content) (Content, error)
}

// Preparation is admitted message work, unlike offline source Prepare. Keys and
// message data are immutable; borrowed callbacks must cooperate and not panic.
type Preparation struct {
	private
	Files         []FileInput
	Parts         []PartInput
	Templates     []TemplateInput
	Transforms    []Transform
	SkipTransform string
	DKIM          *DKIM
	SMIME         *SMIME
}
type DKIMOptions struct {
	private
	Domain, Selector, AUID                       string
	HeaderCanonicalization, BodyCanonicalization string
	SignHeaders, OversignHeaders                 []string
	BodyLength                                   int64
	ExpiresIn                                    time.Duration
	Time                                         time.Time
}
type DKIM struct {
	private
	native *native.DKIM
}
type SMIME struct {
	private
	native *native.SMIME
}

// NewDKIM parses/copies bounded RSA/Ed25519 PEM into immutable custody. There is no
// production verification API. BodyLength > 0 leaves an unsigned suffix.
func NewDKIM(value DKIMOptions, keyPEM []byte) (*DKIM, error) {
	result, err := native.NewDKIM(native.DKIMOptions{Domain: value.Domain, Selector: value.Selector, AUID: value.AUID,
		HeaderCanonicalization: value.HeaderCanonicalization, BodyCanonicalization: value.BodyCanonicalization,
		SignHeaders: value.SignHeaders, OversignHeaders: value.OversignHeaders, BodyLength: value.BodyLength, ExpiresIn: value.ExpiresIn, Time: value.Time}, keyPEM)
	if err != nil {
		return nil, translate(err, "dkim")
	}
	return &DKIM{native: result}, nil
}

// NewSMIME parses a copied RSA/ECDSA certificate/key pair. Native uses only the
// first intermediate certificate. Signing does not certify the trust chain.
func NewSMIME(certificatePEM, keyPEM []byte) (*SMIME, error) {
	result, err := native.NewSMIME(certificatePEM, keyPEM)
	if err != nil {
		return nil, translate(err, "smime")
	}
	return &SMIME{native: result}, nil
}
func nativeDKIM(value *DKIM) *native.DKIM {
	if value == nil {
		return nil
	}
	if value.native == nil {
		return &native.DKIM{}
	}
	return value.native
}
func nativeSMIME(value *SMIME) *native.SMIME {
	if value == nil {
		return nil
	}
	if value.native == nil {
		return &native.SMIME{}
	}
	return value.native
}
func nativeInput(value Input) native.Input {
	return native.Input{Bytes: value.Bytes, Reader: value.Reader, Write: value.Write, Path: value.Path, FS: value.FS, Name: value.Name}
}

// Unsubscribe prepares native one-click HTTPS or ordinary list fields offline.
func Unsubscribe(oneClick string, additional ...string) ([]Header, error) {
	values, err := native.Unsubscribe(oneClick, additional...)
	if err != nil {
		return nil, translate(err, "unsubscribe")
	}
	result := make([]Header, len(values))
	for i, value := range values {
		result[i] = Header{Name: value.Name, Values: value.Values, Preformatted: value.Preformatted}
	}
	return result, nil
}

// PrepareMessage executes bounded acquisition/templates/transforms/signing, with
// independent evidence before callbacks. It does not send a message.
func (client *Client) PrepareMessage(ctx context.Context, message Message, plan Preparation) (Result, error) {
	if len(plan.Files) > 32 || len(plan.Parts) > 32 || len(plan.Templates) > 32 || len(plan.Transforms) > 32 {
		return Result{}, fail(ErrLimit, "prepare-message")
	}
	input := native.Preparation{SkipTransform: plan.SkipTransform, DKIM: nativeDKIM(plan.DKIM), SMIME: nativeSMIME(plan.SMIME)}
	for _, value := range plan.Parts {
		input.Parts = append(input.Parts, native.PartInput{Input: nativeInput(value.Input), Part: nativePart(value.Part)})
	}
	for _, value := range plan.Files {
		if len(value.File.Fields) > 32 {
			return Result{}, fail(ErrLimit, "file-fields")
		}
		input.Files = append(input.Files, native.FileInput{Input: nativeInput(value.Input), File: nativeAttachment(value.File), Inline: value.Inline})
	}
	for _, value := range plan.Templates {
		item := native.TemplateInput{Text: value.Text, HTML: value.HTML, Name: value.Name, Data: value.Data, Part: nativePart(value.Part), Inline: value.Inline}
		if value.File != nil {
			if len(value.File.Fields) > 32 {
				return Result{}, fail(ErrLimit, "file-fields")
			}
			file := nativeAttachment(*value.File)
			item.File = &file
		}
		input.Templates = append(input.Templates, item)
	}
	for _, value := range plan.Transforms {
		item := native.Transform{Name: value.Name}
		if value.Apply != nil {
			hook := value.Apply
			item.Apply = func(work context.Context, content native.Content) (native.Content, error) {
				transformed, err := hook(work, publicContent(content))
				if err != nil {
					return native.Content{}, err
				}
				return nativeContent(transformed)
			}
		}
		input.Transforms = append(input.Transforms, item)
	}
	return client.dispatch(ctx, "prepare-message", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.PrepareMessage(work, id, message.native, input)
	})
}

type ImportOptions struct {
	private
	From       string
	Recipients []string
	DKIM       *DKIM
	SMIME      *SMIME
}

func (client *Client) Import(ctx context.Context, input Input, options ImportOptions) (Result, error) {
	return client.dispatch(ctx, "import", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.Import(work, id, nativeInput(input), native.ImportOptions{From: options.From, Recipients: options.Recipients, DKIM: nativeDKIM(options.DKIM), SMIME: nativeSMIME(options.SMIME)})
	})
}

// Export borrows Writer through its actual return, reports partial bytes, and
// never closes it. Prepared EMLCopy is the independent in-memory export route.
func (client *Client) Export(ctx context.Context, message Message, writer io.Writer) (Result, error) {
	return client.dispatch(ctx, "export", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.Export(work, id, message.native, writer)
	})
}
