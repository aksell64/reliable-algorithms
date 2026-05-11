package dkg

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
)

// Params holds the public parameters for the DKG protocol.
// p — safe prime, q — prime order of subgroup, g and h — generators of the subgroup of order q in Z*p.
// It must hold: g^q ≡ 1 (mod p), h^q ≡ 1 (mod p), and nobody knows log_g(h).
type Params struct {
	P *big.Int // safe prime modulus
	Q *big.Int // prime order of subgroup, p = 2q + 1
	G *big.Int // generator of subgroup of order q
	H *big.Int // second generator, discrete log relation to G unknown
}

// ZpElement represents an element of the quotient ring Z/pZ (ring of residues modulo p).
// In the general case this is a ring; when p is prime, Z/pZ is a field.
// The caller is responsible for ensuring that operations used are valid
// for the algebraic structure implied by the choice of modulus.
//
// ZpElement stores a value reduced modulo P.
type ZpElement struct {
	Value *big.Int
	P     *big.Int
}

// NewZpElement creates a ZpElement, reducing value mod p.
func NewZpElement(value *big.Int, p *big.Int) *ZpElement {
	v := new(big.Int).Mod(value, p)
	return &ZpElement{Value: v, P: p}
}

// RandomZp generates a cryptographically random element in Z/pZ \ {0}.
func RandomZp(p *big.Int) (*ZpElement, error) {
	for {
		v, err := rand.Int(rand.Reader, p)
		if err != nil {
			return nil, fmt.Errorf("random generation failed: %w", err)
		}
		if v.Sign() != 0 {
			return &ZpElement{Value: v, P: p}, nil
		}
	}
}

// ZpZero returns the zero element in Z/pZ.
func ZpZero(p *big.Int) *ZpElement {
	return &ZpElement{Value: big.NewInt(0), P: p}
}

// ZpOne returns the multiplicative identity in Z/pZ.
func ZpOne(p *big.Int) *ZpElement {
	return &ZpElement{Value: big.NewInt(1), P: p}
}

// ZpFromInt64 creates a ZpElement from an int64 value.
func ZpFromInt64(val int64, p *big.Int) *ZpElement {
	v := new(big.Int).SetInt64(val)
	v.Mod(v, p)
	return &ZpElement{Value: v, P: p}
}

// Clone returns a deep copy.
func (a *ZpElement) Clone() *ZpElement {
	return &ZpElement{
		Value: new(big.Int).Set(a.Value),
		P:     a.P,
	}
}

// Add returns (a + b) mod p.
func (a *ZpElement) Add(b *ZpElement) *ZpElement {
	r := new(big.Int).Add(a.Value, b.Value)
	r.Mod(r, a.P)
	return &ZpElement{Value: r, P: a.P}
}

// Sub returns (a - b) mod p.
func (a *ZpElement) Sub(b *ZpElement) *ZpElement {
	r := new(big.Int).Sub(a.Value, b.Value)
	r.Mod(r, a.P)
	return &ZpElement{Value: r, P: a.P}
}

// Mul returns (a * b) mod p.
func (a *ZpElement) Mul(b *ZpElement) *ZpElement {
	r := new(big.Int).Mul(a.Value, b.Value)
	r.Mod(r, a.P)
	return &ZpElement{Value: r, P: a.P}
}

// Neg returns (-a) mod p.
func (a *ZpElement) Neg() *ZpElement {
	r := new(big.Int).Neg(a.Value)
	r.Mod(r, a.P)
	return &ZpElement{Value: r, P: a.P}
}

func (a *ZpElement) Equal(b *ZpElement) bool {
	return a.Value.Cmp(b.Value) == 0 && a.P.Cmp(b.P) == 0
}

// Inv returns the multiplicative inverse a^{-1} mod p.
// Returns error if the inverse does not exist (a == 0 or gcd(a, p) != 1).
func (a *ZpElement) Inv() (*ZpElement, error) {
	if a.Value.Sign() == 0 {
		return nil, errors.New("cannot invert zero element in Zp")
	}
	r := new(big.Int).ModInverse(a.Value, a.P)
	if r == nil {
		return nil, errors.New("modular inverse does not exist")
	}
	return &ZpElement{Value: r, P: a.P}, nil
}

// Div returns a * b^{-1} mod p.
func (a *ZpElement) Div(b *ZpElement) (*ZpElement, error) {
	bInv, err := b.Inv()
	if err != nil {
		return nil, err
	}
	return a.Mul(bInv), nil
}

// Exp returns a^exp mod p, where exp is a raw *big.Int.
func (a *ZpElement) Exp(exp *big.Int) *ZpElement {
	r := new(big.Int).Exp(a.Value, exp, a.P)
	return &ZpElement{Value: r, P: a.P}
}

// IsZero checks if the element is zero.
func (a *ZpElement) IsZero() bool {
	return a.Value.Sign() == 0
}

// BigInt returns the underlying *big.Int value (not a copy — be careful).
func (a *ZpElement) BigInt() *big.Int {
	return a.Value
}

func (a *ZpElement) String() string {
	return a.Value.String()
}
