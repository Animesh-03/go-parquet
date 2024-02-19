package encoding

import (
	"bytes"
	"math"
)

// Return the parsed Variable Length Int and the number of bytes parsed
func DecodeVInt(reader *bytes.Reader) int64 {
	moreBit := true
	value := uint64(0)

	for i := 0; moreBit; i++ {
		b, err := reader.ReadByte()
		if err != nil {
			panic(err)
		}
		// Check if more bytes are to be read
		moreBit = (b & 128) != 0
		// Get the last 7 bits of current byte
		valBytes := b & 0x7f

		value |= uint64(valBytes) << (i * 7)
	}

	return DecodeZigZag(uint64(value))
}

// Return decoded int
func DecodeZigZag(val uint64) int64 {
	if uint64(val)>>63 == 1 {
		return -1 * int64((uint64(val)+1)/2)
	} else {
		return int64((uint64(val) + 1) / 2)
	}
}

// Return zig-zag encoded int
func EncodeZigZag(val int64) uint64 {
	if val < 0 {
		return uint64(math.Abs(float64(val))*2 - 1)
	} else {
		return uint64(val * 2)
	}
}

// Return byte array of a Variable Length Zig-Zag encoded int
func EncodeVInt(val int64) *[]byte {
	vIntBytes := make([]byte, 0)

	// Get the zig-zag encoded value
	valBytes := uint64(EncodeZigZag(val))

	for valBytes > 0 {
		// Get the last 7 bits
		lowerBits := valBytes & 0x7f
		valBytes = valBytes >> 7

		// Set the more bit if needed
		moreBit := uint64(0)
		if valBytes > 0 {
			moreBit = 1
		}

		// Append a byte with moreBit as MSB and the val as the remaining bits
		vIntBytes = append(vIntBytes, byte((moreBit<<7)|lowerBits))
	}

	return &vIntBytes
}
