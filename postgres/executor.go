package postgres

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"text/template"

	"github.com/pvoliveira/dbtestgen"
)

//var _ dbtestgen.Executor = &Executor{}

type Executor struct {
	db         *sql.DB
	contraints map[dbtestgen.TypeConstraint][]*dbtestgen.Constraint
	procedures []*dbtestgen.Procedure
	statements []*dbtestgen.Statement
	tables     []*dbtestgen.Table
}

type SQLGenerator struct {
	fn func() (string, error)
}

func (s *SQLGenerator) CommandSQL() (string, error) {
	if s.fn == nil {
		return "", fmt.Errorf("function to build DDL not defined")
	}

	return s.fn()
}

func newSQLGenerator(fn func() (string, error)) (*SQLGenerator, error) {
	return &SQLGenerator{fn}, nil
}

func (e *Executor) registerConstraints(tables []*dbtestgen.Table) error {
	if len(tables) == 0 {
		return errors.New("tables must be passed")
	}

	inputTables := make(map[string]bool)
	for _, t := range tables {
		inputTables[t.Name] = true
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

func (e *Executor) RegisterTables(tables []*dbtestgen.Table) error {
	if tables == nil {
		return errors.New("tables must be passed")
	}

	for i, tbl := range tables {
		if len(tbl.Schema) == 0 || len(tbl.Name) == 0 {
			return fmt.Errorf("schema and name must be filled in table: item %v", i)
		}

		// configure function that returns the ddl of table
		fnTblDDL := func() (string, error) {
			rows, errFn := e.db.Query("SELECT * FROM " + tbl.Schema + "." + tbl.Name + " WHERE 1=2")
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
				def := sqlColumnDefinition(*col)
				columnsDefinitions = append(columnsDefinitions, def)
			}

			return buildDDLCreateTable(tbl.Schema, tbl.Name, columnsDefinitions)
		}

		gen, err := newSQLGenerator(fnTblDDL)
		if err != nil {
			return err
		}

		tbl.SQLGenerator = gen
	}

	e.tables = tables

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
	createTableTemplate := `CREATE TABLE {{.Schema}}.{{.Name}}\n( {{.Columns}} );`

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
