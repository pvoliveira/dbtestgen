// Package postgres implements postgres integration
package postgres

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"text/template"

	_ "github.com/lib/pq"
	"github.com/pvoliveira/dbtestgen"
)

//var _ dbtestgen.Executor = &Executor{}

// Executor stores objects to invoke the creation of DDL at the end
type Executor struct {
	db         *sql.DB
	contraints map[dbtestgen.TypeConstraint][]*dbtestgen.Constraint
	procedures []*dbtestgen.Procedure
	statements []*dbtestgen.Statement
	tables     []*dbtestgen.Table
}

// FuncDDL function used to return the code of data definition language (DDL)
type FuncDDL func() (string, error)

// SQLGenerator implements SQLGenerator interface
type SQLGenerator struct {
	fn *FuncDDL
}

// CommandSQL returns the DDL of each type
func (s SQLGenerator) CommandSQL() (string, error) {
	if s.fn == nil {
		return "", fmt.Errorf("function to build DDL not defined")
	}

	fnddl := *s.fn
	cmdsql, err := fnddl()

	return cmdsql, err
}

func newSQLGenerator(fn FuncDDL) (SQLGenerator, error) {
	return SQLGenerator{&fn}, nil
}

func (e *Executor) registerConstraints(tables []*dbtestgen.Table) error {
	if len(tables) == 0 {
		return errors.New("tables must be passed")
	}

	inputTables := make(map[string]bool)
	for _, t := range tables {
		if t.Schema == "public" || t.Schema == "PUBLIC" {
			inputTables[t.Name] = true
		} else {
			inputTables[t.Schema+"."+t.Name] = true
		}
	}

	for i, tbl := range tables {
		if len(tbl.Schema) == 0 || len(tbl.Name) == 0 {
			return fmt.Errorf("schema and name must be filled in table: item %v", i)
		}

		constrs, err := returnConstraints(e.db, tbl.Schema, tbl.Name)
		if err != nil {
			return err
		}

		for _, cstr := range constrs {
			for _, c := range cstr {
				if _, ok := inputTables[c.TableRelated]; ok {
					e.contraints[c.TypeConst] = append(e.contraints[c.TypeConst], c)
				}
			}
		}

	}

	return nil
}

func returnConstraints(db *sql.DB, schemaName, tableName string) (map[string][]*dbtestgen.Constraint, error) {
	rows, err := db.Query(`SELECT distinct
		r.conname as name, 
		pg_catalog.pg_get_constraintdef(r.oid, true) as def, 
		case when r.confrelid::regclass::varchar = '-' then
			r.conrelid::regclass
		else 
			r.confrelid::regclass end as related,
		r.contype as type
	FROM pg_catalog.pg_constraint r 
	WHERE r.conrelid = '` + schemaName + `.` + tableName + `'::regclass ORDER BY r.contype DESC`)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	constraintsDefinitions := make(map[string][]*dbtestgen.Constraint, 0)

	for rows.Next() {
		var constr = &dbtestgen.Constraint{}
		var definition string
		var typeConst string
		if err := rows.Scan(&constr.Name, &definition, &constr.TableRelated, &typeConst); err != nil {
			return nil, fmt.Errorf("error to return constraints related with table %v: %v", tableName, err)
		}

		switch typeConst {
		case "p":
			constr.TypeConst = dbtestgen.CONSTRAINTPK
			break
		case "f":
			constr.TypeConst = dbtestgen.CONSTRAINTFK
			break
		}

		fnDDLConstr := func() (string, error) {
			return "ALTER TABLE " + schemaName + "." + tableName + " ADD CONSTRAINT " + constr.Name + " " + definition + ";", nil
		}

		constr.SQLGenerator, err = newSQLGenerator(fnDDLConstr)

		constraintsDefinitions[schemaName+`.`+tableName] = append(constraintsDefinitions[schemaName+`.`+tableName], constr)
	}

	return constraintsDefinitions, nil
}

// RegisterTables assigns the function that return the DDL to each table
func (e *Executor) RegisterTables(tables []*dbtestgen.Table) error {
	if tables == nil || len(tables) == 0 {
		return errors.New("tables must be passed")
	}

	e.tables = make([]*dbtestgen.Table, len(tables))

	for i, tbl := range tables {
		if len(tbl.Schema) == 0 || len(tbl.Name) == 0 {
			return fmt.Errorf("schema and name must be filled in table: item %v", i)
		}

		// configure function that returns the ddl of table
		fnTblDDL := func(tableSchema, tableName string) func() (string, error) {
			return func() (string, error) {
				rows, errFn := e.db.Query("SELECT * FROM " + tableSchema + "." + tableName + " WHERE 1=2")
				if errFn != nil {
					return "", errFn
				}
				defer rows.Close()

				cols, errFn := rows.ColumnTypes()
				if errFn != nil {
					return "", errFn
				}

				columnsDefinitions := make([]string, 0)

				for _, col := range cols {
					columnsDefinitions = append(columnsDefinitions, sqlColumnDefinition(*col))
				}

				return buildDDLCreateTable(tableSchema, tableName, columnsDefinitions)
			}
		}(tbl.Schema, tbl.Name)

		gen, err := newSQLGenerator(fnTblDDL)
		if err != nil {
			return err
		}

		tbl.SQLGenerator = gen

		e.tables[i] = tbl
	}

	err := e.registerConstraints(tables)
	if err != nil {
		return err
	}

	return nil
}

func sqlColumnDefinition(col sql.ColumnType) string {
	var ddl string

	ddl = col.Name() + " " + col.DatabaseTypeName()

	if precision, scale, ok := col.DecimalSize(); ok {
		if precision > 0 {
			ddl += "(" + strconv.Itoa(int(precision))
		}
		if scale > 0 {
			ddl += ", " + strconv.Itoa(int(scale))
		}
		ddl += ")"
	}

	if length, ok := col.Length(); ok {
		if length > 0 {
			ddl += fmt.Sprintf("(%d)", length)
		}
	}

	if nullable, ok := col.Nullable(); ok {
		if nullable {
			ddl += " NULL"
		} else {
			ddl += " NOT NULL"
		}
	}

	return ddl
}

func buildDDLCreateTable(schema, name string, columns []string) (string, error) {
	createTableTemplate := `CREATE TABLE {{.Schema}}.{{.Name}} ( 
 {{.Columns}} );`

	if len(schema) == 0 {
		return "", errors.New("method needs a table's schema")
	}

	if len(name) == 0 {
		return "", errors.New("method needs a table's name")
	}

	if len(columns) == 0 {
		return "", errors.New("method needs columns definitions")
	}

	tmplMain, err := template.New("tabletmpl").Parse(createTableTemplate)
	if err != nil {
		return "", err
	}

	// type to fit with template model
	data := struct {
		Name, Schema, Columns string
	}{name, schema, strings.Join(columns, ",\n")}

	buf := new(bytes.Buffer)
	err = tmplMain.Execute(buf, data)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

// RegisterProcedures assigns the function that return the DDL of procedures Postgres
func (e *Executor) RegisterProcedures(procs []*dbtestgen.Procedure) error {
	if procs == nil {
		return errors.New("procedures must be passed")
	}

	for i, p := range procs {
		if len(p.Schema) == 0 || len(p.Name) == 0 {
			return fmt.Errorf("schema and name must be filled in procedure: item %v", i)
		}

		// configure function that returns the ddl of table
		fnProcDDL := func() (string, error) {
			rows, err := e.db.Query(`SELECT /*n.nspname || '.' || proname AS fname,*/ pg_get_functiondef(p.oid) as definition
			FROM pg_proc p
			JOIN pg_type t
			ON p.prorettype = t.oid
			LEFT OUTER
			JOIN pg_description d
			ON p.oid = d.objoid
			LEFT OUTER
			JOIN pg_namespace n
			ON n.oid = p.pronamespace
			WHERE n.nspname~'` + p.Schema + `'
			AND proname~'` + p.Name + `'`)

			if err != nil {
				return "", err
			}
			defer rows.Close()

			var definition string
			if !rows.Next() {
				return "", nil
			}

			if err := rows.Scan(&definition); err != nil {
				return "", err
			}

			return definition, nil
		}

		gen, err := newSQLGenerator(fnProcDDL)
		if err != nil {
			return err
		}

		p.SQLGenerator = gen

		e.procedures = append(e.procedures, p)
	}

	return nil
}

// ReturnScript joins all scripts DDL of object as result
func (e *Executor) ReturnScript() (string, error) {
	if e.db == nil {
		return "", errors.New("connection not defined")
	}

	if err := e.db.Ping(); err != nil {
		return "", fmt.Errorf("connection can't be established")
	}

	if e.tables == nil || len(e.tables) == 0 {
		return "", errors.New("no tables registereds")
	}

	var buffer bytes.Buffer

	for _, t := range e.tables {
		ddl, err := t.SQLGenerator.CommandSQL()

		if err != nil {
			return "", err
		}

		buffer.WriteString(ddl + "\n\n")
	}

	if e.contraints != nil && len(e.contraints) > 0 {
		for _, typeCons := range []dbtestgen.TypeConstraint{dbtestgen.CONSTRAINTPK, dbtestgen.CONSTRAINTFK, dbtestgen.CONSTRAINTUN} {
			for _, t := range e.contraints[typeCons] {
				ddl, err := t.SQLGenerator.CommandSQL()

				if err != nil {
					return "", err
				}

				buffer.WriteString(ddl + "\n\n")
			}
		}
	}

	if e.procedures != nil && len(e.procedures) > 0 {
		for _, t := range e.procedures {
			ddl, err := t.SQLGenerator.CommandSQL()

			if err != nil {
				return "", err
			}

			buffer.WriteString(ddl + "\n\n")
		}
	}

	return buffer.String(), nil
}

// NewExecutor constructor of Executor
func NewExecutor(connStr string) (*Executor, error) {
	ex := Executor{}
	ex.tables = make([]*dbtestgen.Table, 0)
	ex.contraints = make(map[dbtestgen.TypeConstraint][]*dbtestgen.Constraint, 0)
	ex.procedures = make([]*dbtestgen.Procedure, 0)
	ex.statements = make([]*dbtestgen.Statement, 0)

	var err error
	if ex.db, err = sql.Open("postgres", connStr); err != nil {
		return nil, err
	}

	return &ex, nil
}
