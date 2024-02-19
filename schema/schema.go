package schema

import (
	"fmt"

	"github.com/animesh-03/go-parquet/parquet"
)

type Schema struct {
	Elements []*parquet.SchemaElement

	MaxRL int64
	MaxDL int64

	PathMap map[string]*parquet.SchemaElement
}

func NewSchemaFromMetadata(metadata *parquet.FileMetaData) Schema {
	schema := Schema{}

	schema.Elements = metadata.GetSchema()

	schema.computeMaxRLDL()

	return schema
}

func (s *Schema) computeMaxRLDL() {
	mrl, mdl := 0, 0
	var dfs func(index, rl, dl int)
	dfs = func(index, rl, dl int) {
		mrl = max(mrl, rl)
		mdl = max(mdl, dl)

		ele := s.Elements[index]

		for i := index + 1; i < index+int(ele.GetNumChildren()); i++ {
			nrl, ndl := rl, dl
			child := s.Elements[i]
			if *child.RepetitionType == parquet.FieldRepetitionType_REPEATED {
				nrl++
			}
			if *child.RepetitionType == parquet.FieldRepetitionType_OPTIONAL {
				ndl++
			}

			dfs(i, nrl, ndl)
		}
	}

	dfs(0, 0, 0)

	fmt.Println(mrl, mdl)

	s.MaxRL, s.MaxDL = int64(mrl), int64(mdl)
}

func (s *Schema) GetMaxRepetitionLevel() int64 {
	return s.MaxRL
}

func (s *Schema) GetMaxDefinitionLevel() int64 {
	return s.MaxDL
}
