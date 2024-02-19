package plain

import (
	"bytes"
	"encoding/binary"
)

func DecodeString(reader *bytes.Reader, numValues int32) []string {
	val := make([]string, numValues)

	for i := range numValues {
		sLen := DecodeInt32(reader, 1)
		s := make([]byte, sLen[0])

		if _, err := reader.Read(s); err != nil {
			panic(err)
		}

		val[i] = string(s)
	}

	return val
}

func DecodeInt32(reader *bytes.Reader, numValues int32) []int32 {
	val := make([]int32, numValues)

	for i := range numValues {
		buf := make([]byte, 4)
		if _, err := reader.Read(buf); err != nil {
			panic(err)
		}

		val[i] = int32(binary.LittleEndian.Uint32(buf))
	}

	return val
}
