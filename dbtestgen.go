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
	registerConstraints(tables []*Table) error
	RegisterProcedures(procs []*Procedure) error
	RegisterTables(tables []*Table) error
	ReturnScript() (string, error)
}

type SQLGenerator interface {
	CommandSQL() (string, error)
}

// Table haves data about tables from database like schema and name,
// which are used to retrive metadata to build DDL commands.
type Table struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	Where  string `json:"where"`
	SQLGenerator
}

// Constraint haves data about tables from database like schema, name and table related
// which are used to retrive metadata to build DDL commands.
type Constraint struct {
	Schema       string         `json:"schema"`
	Name         string         `json:"name"`
	TableRelated string         `json:"tableRelated"`
	TypeConst    TypeConstraint `json:"typeConst"`
	SQLGenerator
}

// Procedure haves data about tables from database like schema and name,
// which are used to retrive metadata to build DDL commands.
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
