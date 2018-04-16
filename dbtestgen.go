package dbtestgen

// TypeConstraint type of constraint.
type TypeConstraint int

// Defines types of constraint.
const (
	CONSTRAINTPK TypeConstraint = iota // primary key
	CONSTRAINTFK                       // foreign key
	CONSTRAINTUN                       // unique
)

type DBObject interface {
	DDL() (string, error)
}

type Table struct {
	Schema string
	Name   string
	DBObject
}

type Constraint struct {
	Schema       string
	Name         string
	TableRelated string
	TypeConst    TypeConstraint
	DBObject
}

type Procedure struct {
	Schema string
	Name   string
	DBObject
}

func NewTable(schema, name string, dbObj DBObject) *Table {
	return &Table{Schema: schema, Name: name, DBObject: dbObj}
}

func NewConstraint(schema, name string, dbObj DBObject) *Constraint {
	return &Constraint{Schema: schema, Name: name, DBObject: dbObj}
}

func NewProcedure(schema, name string, dbObj DBObject) *Procedure {
	return &Procedure{Schema: schema, Name: name, DBObject: dbObj}
}
