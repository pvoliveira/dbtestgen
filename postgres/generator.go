package postgres

import (
	"database/sql"

	"github.com/pvoliveira/dbtestgen"
)

type GeneratorService struct {
	ConnString string
	db         *sql.DB
}

func (g *GeneratorService) Open() error {

}

func (g *GeneratorService) DDL() (string, error) {

}

func NewGeneratorService() (*GeneratorService, error) {

}

func DDLTable(schema, name string) (string, error) {
	var table *dbtestgen.Table

	gen, err := NewGeneratorService()

	table = dbtestgen.NewTable(schema, name, gen)

	ddl, err := table.DDL()
	if err != nil {
		return "", err
	}

	return ddl, nil
}

func DDLConstraint(schemaTable, nameTable string) (string, error) {

}

func DDLProcedure(schema, name string) (string, error) {

}
