package hamt

// test utilities
// from https://github.com/polydawn/refmt/blob/3d65705ee9f12dc0dfcc0dc6cf9666e97b93f339/cbor/cborFixtures_test.go#L81-L93

func bcat(bss ...[]byte) []byte {
	l := 0
	for _, bs := range bss {
		l += len(bs)
	}
	rbs := make([]byte, 0, l)
	for _, bs := range bss {
		rbs = append(rbs, bs...)
	}
	return rbs
}

func b(b byte) []byte { return []byte{b} }
