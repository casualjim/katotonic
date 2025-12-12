package katotonic

import "github.com/google/uuid"

const (
	termBits    uint32 = 48
	seqBits     uint32 = 74
	payloadBits uint32 = termBits + seqBits // 122

	fixedVersionStart uint32 = 48
	fixedVersionEnd   uint32 = 51
	fixedVariant0     uint32 = 64
	fixedVariant1     uint32 = 65

	seqHiBits uint32 = seqBits - 64 // 10
)

const (
	termMask  uint64 = (1 << termBits) - 1
	seqHiMask uint64 = (1 << seqHiBits) - 1
)

func isFixedBit(bit uint32) bool {
	return (bit >= fixedVersionStart && bit <= fixedVersionEnd) || bit == fixedVariant0 || bit == fixedVariant1
}

func setUUIDBit(b *[16]byte, bit uint32) {
	byteIndex := bit / 8
	bitInByte := 7 - (bit % 8)
	b[byteIndex] |= 1 << bitInByte
}

func getUUIDBit(b *[16]byte, bit uint32) uint8 {
	byteIndex := bit / 8
	bitInByte := 7 - (bit % 8)
	if b[byteIndex]&(1<<bitInByte) != 0 {
		return 1
	}
	return 0
}

func payloadBit(term uint64, seqHi uint64, seqLo uint64, payloadIndex uint32) uint8 {
	if payloadIndex < termBits {
		shift := termBits - 1 - payloadIndex
		if (term>>shift)&1 == 1 {
			return 1
		}
		return 0
	}

	seqIndex := payloadIndex - termBits
	shift := seqBits - 1 - seqIndex
	if shift >= 64 {
		if (seqHi>>(shift-64))&1 == 1 {
			return 1
		}
		return 0
	}
	if (seqLo>>shift)&1 == 1 {
		return 1
	}
	return 0
}

// EncodeV8 encodes (term, seqHi, seqLo) into a Katotonic UUIDv8.
// seqHi carries the high 10 bits of the 74-bit seq, seqLo the low 64 bits.
func EncodeV8(term uint64, seqHi uint64, seqLo uint64) uuid.UUID {
	term &= termMask
	seqHi &= seqHiMask

	var out [16]byte
	var payloadIndex uint32

	for uuidBit := uint32(0); uuidBit < 128; uuidBit++ {
		if isFixedBit(uuidBit) {
			continue
		}
		if payloadBit(term, seqHi, seqLo, payloadIndex) == 1 {
			setUUIDBit(&out, uuidBit)
		}
		payloadIndex++
	}

	// Set version (bits 48–51) to 1000.
	out[6] = (out[6] & 0x0F) | 0x80
	// Set variant (bits 64–65) to 10.
	out[8] = (out[8] & 0x3F) | 0x80

	return uuid.UUID(out)
}

// DecodeV8 decodes a Katotonic UUIDv8 into (term, seqHi, seqLo).
func DecodeV8(id uuid.UUID) (uint64, uint64, uint64) {
	bytes := [16]byte(id)
	var term uint64
	var seqHi uint64
	var seqLo uint64

	var payloadIndex uint32
	for uuidBit := uint32(0); uuidBit < 128; uuidBit++ {
		if isFixedBit(uuidBit) {
			continue
		}
		bit := getUUIDBit(&bytes, uuidBit)
		if payloadIndex < termBits {
			term = (term << 1) | uint64(bit)
		} else {
			seqIndex := payloadIndex - termBits
			if seqIndex < seqHiBits {
				seqHi = (seqHi << 1) | uint64(bit)
			} else {
				seqLo = (seqLo << 1) | uint64(bit)
			}
		}
		payloadIndex++
	}

	return term, seqHi, seqLo
}

