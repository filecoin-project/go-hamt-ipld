module github.com/ipfs/go-hamt-ipld

require (
	github.com/ipfs/go-cid v0.0.3
	github.com/ipfs/go-datastore v0.1.1
	github.com/ipfs/go-ipfs-blockstore v0.1.1
	github.com/ipfs/go-ipld-cbor v0.0.3
	github.com/smartystreets/goconvey v1.6.4 // indirect
	github.com/spaolacci/murmur3 v1.1.0
	github.com/warpfork/go-wish v0.0.0-20200122115046-b9ea61034e4a // indirect
	github.com/whyrusleeping/cbor-gen v0.0.0-20200123233031-1cdf64d27158
	golang.org/x/xerrors v0.0.0-20191204190536-9bdfabe68543
)

replace github.com/ipfs/go-ipld-cbor => ../go-ipld-cbor

go 1.13
