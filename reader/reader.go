package reader

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/animesh-03/go-parquet/encoding"
	"github.com/animesh-03/go-parquet/parquet"
	"github.com/animesh-03/go-parquet/schema"
	"github.com/apache/thrift/lib/go/thrift"
)

type ParquetReader struct {
	File     io.Reader
	Metadata *parquet.FileMetaData
	Schema   schema.Schema
}

func NewParquetReader(path string) *ParquetReader {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}

	fStat, err := f.Stat()
	if err != nil {
		panic(err)
	}

	buf := make([]byte, 4)
	if _, err = f.Seek(-4, io.SeekEnd); err != nil {
		panic(err)
	}
	if _, err := f.Read(buf); err != nil {
		panic(err)
	}
	if string(buf) != "PAR1" {
		panic("invalid magic number")
	}

	if _, err = f.Seek(-8, io.SeekEnd); err != nil {
		panic(err)
	}
	if _, err = io.ReadFull(f, buf); err != nil {
		panic(err)
	}
	size := binary.LittleEndian.Uint32(buf)

	if _, err = f.Seek(-(int64)(8+size), io.SeekEnd); err != nil {
		panic(err)
	}
	pf := thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{})
	thriftReader := thrift.NewStreamTransportR(f)
	bufferReader := thrift.NewTBufferedTransport(thriftReader, int(fStat.Size()))
	protocol := pf.GetProtocol(bufferReader)

	metadata := parquet.NewFileMetaData()
	metadata.Read(context.TODO(), protocol)

	schema := schema.NewSchemaFromMetadata(metadata)

	dataPageOffset := metadata.GetRowGroups()[0].GetColumns()[1].GetMetaData().DataPageOffset
	if _, err = f.Seek(dataPageOffset, io.SeekStart); err != nil {
		panic(err)
	}
	bufferReader = thrift.NewTBufferedTransport(thriftReader, int(fStat.Size()))
	protocol = pf.GetProtocol(bufferReader)
	pageHeader := parquet.NewPageHeader()
	pageHeader.Read(context.TODO(), protocol)

	fmt.Println(pageHeader)

	colBuf := make([]byte, pageHeader.CompressedPageSize)
	if _, err := bufferReader.Read(colBuf); err != nil {
		panic(err)
	}

	fmt.Println(colBuf)

	gr, _ := gzip.NewReader(bytes.NewReader(colBuf))
	output, e2 := io.ReadAll(gr)
	if e2 != nil {
		fmt.Println(e2)
	}

	fmt.Println(output)
	fmt.Println(len(output))

	colReader := bytes.NewReader(output)

	if schema.MaxRL > 0 {
		rls := encoding.ReadRLEData(colReader, 1)
		fmt.Println("Repetition Levels: ", rls)
	}

	if schema.MaxDL > 0 {
		dls := encoding.ReadRLEData(colReader, 1)
		fmt.Println("Definition Levels: ", dls)
	}

	return &ParquetReader{
		File:     f,
		Metadata: metadata,
		Schema:   schema,
	}
}
