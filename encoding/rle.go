package encoding

import (
	"bytes"
	"encoding/binary"

	"github.com/animesh-03/go-parquet/encoding/plain"
)

func ReadRLEData(reader *bytes.Reader, bitWidth int32) []int32 {
	length := plain.DecodeInt32(reader, 1)
	buf := make([]byte, length[0])

	if _, err := reader.Read(buf); err != nil {
		panic(err)
	}

	rleReader := bytes.NewReader(buf)

	rleData := make([]int32, 0)

	for rleReader.Len() > 0 {
		header := DecodeVInt(rleReader) >> 1

		vals := DecodeRLE(rleReader, header, bitWidth)

		rleData = append(rleData, vals...)
	}

	return rleData

}

func DecodeRLE(reader *bytes.Reader, header int64, bitWidth int32) []int32 {
	width := (bitWidth + 7) / 8
	buf := make([]byte, width)

	if _, err := reader.Read(buf); err != nil {
		panic(err)
	}
	for len(buf) < 4 {
		buf = append(buf, byte(0))
	}

	val := binary.LittleEndian.Uint32(buf)

	vals := make([]int32, header)
	for i := range header {
		vals[i] = int32(val)
	}

	return vals
}
