package main

import (
	"fmt"

	"github.com/animesh-03/go-parquet/reader"
)

// func (pr *ParquetReader) GetFooterSize() (uint32, error) {
// 	var err error
// 	buf := make([]byte, 4)
// 	if _, err = pr.PFile.Seek(-8, io.SeekEnd); err != nil {
// 		return 0, err
// 	}
// 	if _, err = io.ReadFull(pr.PFile, buf); err != nil {
// 		return 0, err
// 	}
// 	size := binary.LittleEndian.Uint32(buf)
// 	return size, err
// }

func main() {
	pr := reader.NewParquetReader("data.parquet")
	fmt.Println(pr.Metadata.NumRows)
	fmt.Println(pr.Metadata.GetSchema()[1].GetNumChildren())
}
