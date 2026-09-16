module github.com/nukilabs/quic-go

go 1.27

require (
	github.com/nukilabs/http v1.3.1
	github.com/nukilabs/qpack v0.7.0
	github.com/nukilabs/utls v1.3.3
	github.com/quic-go/go-ossfuzz-seeds v0.1.0
	github.com/stretchr/testify v1.12.1
	go.uber.org/mock v0.5.2
	golang.org/x/crypto v0.55.0
	golang.org/x/net v0.57.0
	golang.org/x/sync v0.22.0
	golang.org/x/sys v0.47.0
)

require (
	github.com/andybalholm/brotli v1.2.3 // indirect
	github.com/jordanlewis/gcassert v0.0.0-20250430164644-389ef753e22e // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/mod v0.38.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	golang.org/x/tools v0.48.0 // indirect
)

tool (
	github.com/jordanlewis/gcassert/cmd/gcassert
	go.uber.org/mock/mockgen
)
