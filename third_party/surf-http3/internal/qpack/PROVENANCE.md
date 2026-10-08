# Private Surf QPACK decoder

Table/index routines are adapted from the already-selected source
github.com/nukilabs/qpack v0.7.0, origin8b61eff997c65a046f1134614e6ee9a118158313,
with its original MIT notice retained in LICENSE.md. This is private decoder code
inside the Surf H3 correction, not a dependency on the Nuki provider/module.
Selected github.com/quic-go/qpack v0.6.0 still supplies the static table/types and
request encoder; selected x/net/hpack supplies Huffman decoding. No shared module
replacement or upstream version was changed.

The local implementation adds checked integer/reference arithmetic, actual
advertised blocked-stream enforcement, bounded literals/fields, and connection-local
feedback/lifecycle integration. Technical tests are not license/release clearance.
