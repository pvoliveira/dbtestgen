package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
	"github.com/pvoliveira/dbtestgen"
)

type parserPostgres struct{}

// ParseColumns - Returns array of sql.ColumnType according to columns of table.
func (p parserPostgres) ParseColumns(db *sql.DB, schemaName, tableName string) (columnsDefinitions []sql.ColumnType, err error) {
	rows, err := db.Query("SELECT * FROM " + schemaName + "." + tableName + " WHERE 1=2")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	columnsDefinitions = make([]sql.ColumnType, 0)

	for _, col := range cols {
		columnsDefinitions = append(columnsDefinitions, *col)
	}

	return columnsDefinitions, err
}

// ParseConstraints - Returns DDL statement of constraints like primary key, foreign key, uniques, etc.
func (p parserPostgres) ParseConstraints(db *sql.DB, schemaName, tableName string) (constraintsDefinitions map[string]dbtestgen.ConstraintMetadata, err error) {
	type constraint struct{ name, def, related string }

	rows, err := db.Query(`SELECT distinct
		r.conname as name, 
		pg_catalog.pg_get_constraintdef(r.oid, true) as def, 
		r.confrelid::regclass as related,
		r.contype as type
	FROM pg_catalog.pg_constraint r 
	WHERE r.conrelid = '` + schemaName + `.` + tableName + `'::regclass /*AND r.contype = 'f'*/ ORDER BY r.contype DESC`)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	constraintsDefinitions = make(map[string]dbtestgen.ConstraintMetadata)

	for rows.Next() {
		var constr = dbtestgen.ConstraintMetadata{}
		var definition string
		if err := rows.Scan(&constr.Name, &definition, &constr.TableNameRelated); err != nil {
			return nil, err
		}

		constr.DDL = "ALTER TABLE " + schemaName + "." + tableName + " ADD CONSTRAINT " + constr.Name + " " + definition + ";"
		constraintsDefinitions[constr.Name] = constr
	}

	fmt.Printf("constraints founded:\n%+v\n", constraintsDefinitions)

	return constraintsDefinitions, nil
}

// RawColumnDefinition - Returns the DDL block on a create table command, like:
func (p parserPostgres) RawColumnDefinition(col sql.ColumnType) (sqlType string, err error) {
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
		ddl += "(" + strconv.Itoa(int(length)) + ")"
	}

	if nullable, ok := col.Nullable(); ok {
		if nullable {
			ddl += " NULL"
		} else {
			ddl += " NOT NULL"
		}
	}

	return ddl, nil
}

func (p parserPostgres) ParseProcedures(db *sql.DB, schemaName, procedureName string) (funcsPropsDefinitions map[string]string, err error) {

	_, err = db.Query(`SELECT n.nspname AS schema
		,proname AS fname
		,proargnames AS args
		,t.typname AS return_type
		,d.description
		,pg_get_functiondef(p.oid) as definition
	FROM pg_proc p
	JOIN pg_type t
	  ON p.prorettype = t.oid
	LEFT OUTER
	JOIN pg_description d
	  ON p.oid = d.objoid
	LEFT OUTER
	JOIN pg_namespace n
	  ON n.oid = p.pronamespace
   WHERE n.nspname~'` + schemaName + `'
	 AND proname~'` + procedureName + `';`)

	return nil, nil
}

func main() {
	var connStrInput string
	var tables string
	flag.StringVar(&connStrInput, "input", "{dialect}://{user}:{password}@{host}/{databasename}[?{parameters=value}]", "connectionstring to input database")
	flag.StringVar(&tables, "tables", "schema.tableone[,schema.tabletwo]", "tables with respectives schemas")

	flag.Parse()

	// if flag.NArg() < 2 {
	// 	fmt.Fprintln(os.Stderr, "missing subcommand: input and tables")
	// 	os.Exit(1)
	// }

	openConnInput := func(config *dbtestgen.ConfigDB) error {
		//dbInstance, err := sql.Open("postgres", "postgres://postgres:senha@10.20.11.119/input?sslmode=disable")
		//dbInstance, err := sql.Open("postgres", "postgres://pagoufacil:pagoufacilw3b@10.20.11.106/pagoufacildb?sslmode=disable")
		dbInstance, err := sql.Open("postgres", connStrInput)
		if err != nil {
			return err
		}
		config.DB = dbInstance
		return nil
	}

	// set tables to input configuration that generate the DDL
	configInputTables := func(config *dbtestgen.ConfigDB) error {
		paramtables := strings.Split(tables, ",")
		for _, tablename := range paramtables {
			schematable := strings.Split(tablename, ".")
			config.Tables = append(config.Tables, &dbtestgen.ConfigTable{Schema: schematable[0], Name: schematable[1]})
		}

		return nil
	}

	configInput, err := dbtestgen.AddConfigDB("entrada", dbtestgen.Input, openConnInput, configInputTables)
	if err != nil {
		fmt.Printf("Error: %v", err)
		os.Exit(1)
	}

	parser := parserPostgres{}

	dbtestgen.RegisterParser(parser)

	sql, err := configInput.GenerateDDLScript()
	if err != nil {
		fmt.Printf("Error:\n%v\n", err)
		os.Exit(1)
	}

	fmt.Fprint(os.Stdout, sql)
}
