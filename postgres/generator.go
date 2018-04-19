package postgres

import (
	"database/sql"
	"errors"

	"github.com/pvoliveira/dbtestgen"

	_ "github.com/lib/pq"
)

var _ dbtestgen.Executor = &Executor{}

type Executor struct {
	db         *sql.DB
	contraints []*dbtestgen.Constraint
	procedures []*dbtestgen.Procedure
	statements []*dbtestgen.Statement
	tables     []*dbtestgen.Table
}

func (e *Executor) RegisterTables(tables []*dbtestgen.Table) error {
	return errors.New("not implemented")
}

func (e *Executor) RegisterProcedures(tables []*dbtestgen.Procedure) error {
	return errors.New("not implemented")
}

func (e *Executor) ReturnScript() (string, error) {
	return "", errors.New("not implemented")
}

func NewExecutor(db *sql.DB) (*Executor, error) {
	gen := &Executor{db: db}
	err := gen.db.Ping()
	if err != nil {
		return nil, err
	}
	return gen, nil
}
