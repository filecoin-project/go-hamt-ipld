package hamt

import "fmt"

const bucketSize = 3
const defaultBitWidth = 8

type config struct {
	bitWidth int
	hashFn   HashFunc
}

func defaultConfig() *config {
	return &config{
		bitWidth: defaultBitWidth,
		hashFn:   defaultHashFunction,
	}
}

// Option is a function that configures a HAMT.
type Option func(*config) error

// UseTreeBitWidth allows you to set a custom bitWidth of the HAMT in bits
// (from 1-8).
//
// Passing in the returned Option to NewNode will generate a new HAMT that uses
// the specified bitWidth.
//
// The default bitWidth is 8.
func UseTreeBitWidth(bitWidth int) Option {
	return func(c *config) error {
		if bitWidth < 1 {
			return fmt.Errorf("configured bitwidth %d below minimum of 1", bitWidth)
		} else if bitWidth > 8 {
			return fmt.Errorf("configured bitwidth %d exceeds maximum of 8", bitWidth)
		}
		c.bitWidth = bitWidth
		return nil
	}
}

// UseHashFunction allows you to set the hash function used for internal
// indexing by the HAMT.
//
// Passing in the returned Option to NewNode will generate a new HAMT that uses
// the specified hash function.
//
// The default hash function is murmur3-x64 but you should use a
// cryptographically secure function such as SHA2-256 if an attacker may be
// able to pick the keys in order to avoid potential hash collision (tree
// explosion) attacks.
func UseHashFunction(hash HashFunc) Option {
	return func(c *config) error {
		if hash == nil {
			return fmt.Errorf("configured hash function was nil")
		}
		c.hashFn = hash
		return nil
	}
}
