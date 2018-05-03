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
	registerConstraints(tables []*Table) (map[TypeConstraint][]*Constraint, error)
	RegisterProcedures(procs []*Procedure) error
	RegisterTables(tables []*Table) error
	ReturnScript() (string, error)
}

type SQLGenerator interface {
	CommandSQL() (string, error)
}

type Table struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	Where  string `json:"where"`
	SQLGenerator
}

type Constraint struct {
	Schema       string         `json:"schema"`
	Name         string         `json:"name"`
	TableRelated string         `json:"tableRelated"`
	TypeConst    TypeConstraint `json:"typeConst"`
	SQLGenerator
}

type Procedure struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	SQLGenerator
}

type Statement struct {
	Schema      string `json:"schema"`
	TableTarget string `json:"tableTarget"`
	SQLGenerator
}
