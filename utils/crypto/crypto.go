package crypto

import (
	"crypto/sha256"
	"math/big"

	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
)

// =============================================================
// Хэширование произвольного входа на кривую G1
// =============================================================

// HashToG1 — хэширует произвольные байты в точку на G1.
// Использует стандартный метод hash-to-curve (RFC 9380, suite BLS12381G1_XMD:SHA-256_SSWU_RO_).
// dst — domain separation tag, уникальный для протокола.
func HashToG1(msg []byte, dst []byte) (bls12381.G1Affine, error) {
	// gnark-crypto реализует полный hash-to-curve по RFC 9380
	point, err := bls12381.HashToG1(msg, dst)
	if err != nil {
		return bls12381.G1Affine{}, err
	}
	return point, nil
}

// HashToG1Simple — упрощённая обёртка с дефолтным DST для Common Coin.
func HashToG1Simple(roundID []byte) (bls12381.G1Affine, error) {
	dst := []byte("COMMON-COIN-BLS12381-G1_XMD:SHA-256_SSWU_RO_")
	return HashToG1(roundID, dst)
}

// =============================================================
// Операции над скалярами (поле Fr — порядок подгруппы)
// =============================================================

// ScalarRandom — генерирует случайный скаляр из Fr.
func ScalarRandom() (fr.Element, error) {
	var s fr.Element
	_, err := s.SetRandom()
	return s, err
}

// ScalarFromInt — создаёт скаляр из int64.
func ScalarFromInt(v int64) fr.Element {
	var s fr.Element
	s.SetInt64(v)
	return s
}

// ScalarInverse — мультипликативный обратный: s^{-1} mod q.
func ScalarInverse(s fr.Element) fr.Element {
	var inv fr.Element
	inv.Inverse(&s)
	return inv
}

// ScalarMul — умножение двух скаляров: a * b mod q.
func ScalarMul(a, b fr.Element) fr.Element {
	var result fr.Element
	result.Mul(&a, &b)
	return result
}

// ScalarAdd — сложение двух скаляров: a + b mod q.
func ScalarAdd(a, b fr.Element) fr.Element {
	var result fr.Element
	result.Add(&a, &b)
	return result
}

// ScalarSub — вычитание: a - b mod q.
func ScalarSub(a, b fr.Element) fr.Element {
	var result fr.Element
	result.Sub(&a, &b)
	return result
}

// ScalarNeg — отрицание: -s mod q.
func ScalarNeg(s fr.Element) fr.Element {
	var neg fr.Element
	neg.Neg(&s)
	return neg
}

// =============================================================
// Операции над точками G1
// =============================================================

// G1Generator — возвращает генератор группы G1.
func G1Generator() bls12381.G1Affine {
	_, _, g1, _ := bls12381.Generators()
	return g1
}

// G1ScalarMul — умножение точки на скаляр: [s]P.
func G1ScalarMul(point bls12381.G1Affine, scalar fr.Element) bls12381.G1Affine {
	var result bls12381.G1Affine
	var sBigInt big.Int
	scalar.BigInt(&sBigInt)
	result.ScalarMultiplication(&point, &sBigInt)
	return result
}

// G1Add — сложение двух точек на G1.
func G1Add(p1, p2 bls12381.G1Affine) bls12381.G1Affine {
	var p1Jac bls12381.G1Jac
	p1Jac.FromAffine(&p1)

	var p2Jac bls12381.G1Jac
	p2Jac.FromAffine(&p2)

	p1Jac.AddAssign(&p2Jac)

	var result bls12381.G1Affine
	result.FromJacobian(&p1Jac)
	return result
}

// G1Neg — отрицание точки (отражение по оси X).
func G1Neg(p bls12381.G1Affine) bls12381.G1Affine {
	var neg bls12381.G1Affine
	neg.Neg(&p)
	return neg
}

// G1IsOnCurve — проверка что точка лежит на кривой.
func G1IsOnCurve(p bls12381.G1Affine) bool {
	return p.IsOnCurve()
}

// G1IsInSubgroup — проверка что точка в правильной подгруппе порядка q.
func G1IsInSubgroup(p bls12381.G1Affine) bool {
	return p.IsInSubGroup()
}

// =============================================================
// Операции над точками G2
// =============================================================

// G2Generator — возвращает генератор группы G2.
func G2Generator() bls12381.G2Affine {
	_, _, _, g2 := bls12381.Generators()
	return g2
}

// G2ScalarMul — умножение точки G2 на скаляр: [s]Q.
func G2ScalarMul(point bls12381.G2Affine, scalar fr.Element) bls12381.G2Affine {
	var result bls12381.G2Affine
	var sBigInt big.Int
	scalar.BigInt(&sBigInt)
	result.ScalarMultiplication(&point, &sBigInt)
	return result
}

// =============================================================
// Pairing — проверка подписей BLS
// =============================================================

// VerifyPairing проверяет: e(signature, g2) == e(messagePoint, publicKey)
// Это стандартная проверка BLS-подписи.
// signature ∈ G1, publicKey ∈ G2, messagePoint ∈ G1.
func VerifyPairing(
	signature bls12381.G1Affine,
	publicKey bls12381.G2Affine,
	messagePoint bls12381.G1Affine,
) (bool, error) {
	// Проверяем: e(sig, g2) == e(msg, pk)
	// Эквивалентно: e(sig, g2) * e(-msg, pk) == 1 (identity в GT)
	g2Gen := G2Generator()

	negMsg := G1Neg(messagePoint)

	// Multi-pairing: e(sig, g2) * e(-msg, pk) == 1?
	ok, err := bls12381.PairingCheck(
		[]bls12381.G1Affine{signature, negMsg},
		[]bls12381.G2Affine{g2Gen, publicKey},
	)
	if err != nil {
		return false, err
	}
	return ok, nil
}

// =============================================================
// Утилиты
// =============================================================

// CoinFromSignature — извлекает бит монетки из агрегированной подписи.
// Берёт SHA-256 от сериализованной подписи, возвращает младший бит.
func CoinFromSignature(signature bls12381.G1Affine) byte {
	bytes := signature.Marshal()
	hash := sha256.Sum256(bytes)
	return hash[0] & 1
}

// SerializeG1 — сериализация точки G1 в байты (сжатый формат, 48 байт).
func SerializeG1(p bls12381.G1Affine) []byte {
	// Marshal возвращает несжатый формат (96 байт: X || Y)
	// Для сжатого используем RawBytes (48 байт с флагом)
	compressed := p.Bytes()
	return compressed[:]
}

// DeserializeG1 — десериализация точки G1 из байтов.
func DeserializeG1(data []byte) (bls12381.G1Affine, error) {
	var p bls12381.G1Affine
	_, err := p.SetBytes(data)
	return p, err
}
