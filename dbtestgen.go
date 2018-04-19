package dbtestgen

// TypeConstraint type of constraint.
type TypeConstraint int

// Defines types of constraints.
const (
	CONSTRAINTPK TypeConstraint = iota // primary key
	CONSTRAINTFK                       // foreign key
	CONSTRAINTUN                       // unique
)

type Executor interface {
	RegisterTables(tables []*Table) error
	RegisterProcedures(procs []*Procedure) error
	ReturnScript() (string, error)
}

type Generator interface {
	CommandSQL() (string, error)
}

type Table struct {
	Schema string
	Name   string
	Where  string
	Generator
}

type Constraint struct {
	Schema       string
	Name         string
	TableRelated string
	TypeConst    TypeConstraint
	Generator
}

type Procedure struct {
	Schema string
	Name   string
	Generator
}

type Statement struct {
	Schema      string
	TableTarget string
	Generator
}
