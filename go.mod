module github.com/filecoin-project/go-hamt-ipld/v3

require (
	github.com/ipfs/go-block-format v0.1.2
	github.com/ipfs/go-cid v0.4.1
	github.com/ipfs/go-ipld-cbor v0.1.0
	github.com/spaolacci/murmur3 v1.1.0
	github.com/stretchr/testify v1.7.0
	github.com/whyrusleeping/cbor-gen v0.0.0-20230818171029-f91ae536ca25
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.12.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/gopherjs/gopherjs v1.17.2 // indirect
	github.com/ipfs/go-ipfs-util v0.0.2 // indirect
	github.com/ipfs/go-ipld-format v0.5.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.5 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/minio/sha256-simd v1.0.1 // indirect
	github.com/mr-tron/base58 v1.2.0 // indirect
	github.com/multiformats/go-base32 v0.1.0 // indirect
	github.com/multiformats/go-base36 v0.2.0 // indirect
	github.com/multiformats/go-multibase v0.2.0 // indirect
	github.com/multiformats/go-multihash v0.2.3 // indirect
	github.com/multiformats/go-varint v0.0.7 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/polydawn/refmt v0.89.0 // indirect
	golang.org/x/crypto v0.12.0 // indirect
	gopkg.in/yaml.v3 v3.0.0 // indirect
	lukechampine.com/blake3 v1.2.1 // indirect
)

go 1.20

replace github.com/ipfs/go-ipld-cbor => github.com/vulcanize/go-ipld-cbor v0.1.1-internal-0.0.1
