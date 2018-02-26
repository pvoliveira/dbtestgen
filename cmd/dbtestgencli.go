package main

import (
	"database/sql"
	"fmt"
	"strconv"

	_ "github.com/lib/pq"
	"github.com/pvoliveira/dbtestgen/src"
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
func (p parserPostgres) ParseConstraints(db *sql.DB, schemaName, tableName string) (constraintsDefinitions map[string]string, err error) {
	type constraint struct{ name, def string }

	rows, err := db.Query(`SELECT 
		conname, pg_catalog.pg_get_constraintdef(r.oid, true) as condef 
		FROM pg_catalog.pg_constraint r 
		WHERE r.conrelid = '` + schemaName + `.` + tableName + `'::regclass AND r.contype = 'f' ORDER BY 1`)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	constraintsDefinitions = make(map[string]string)

	for rows.Next() {
		var constr = constraint{}
		if err := rows.Scan(&constr.name, &constr.def); err != nil {
			return nil, err
		}

		constraintsDefinitions[constr.name] = "ALTER TABLE " + schemaName + "." + tableName + " ADD CONSTRAINT " + constr.name + " " + constr.def
	}

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

func main() {
	openConnInput := func(config *dbtestgen.ConfigDB) error {
		//dbInstance, err := sql.Open("postgres", "postgres://postgres:senha@10.20.11.119/input?sslmode=disable")
		dbInstance, err := sql.Open("postgres", "postgres://pagoufacil:pagoufacilw3b@10.20.11.106/pagoufacildb?sslmode=disable")
		config.DB = dbInstance
		return err
	}

	openConnOutput := func(config *dbtestgen.ConfigDB) error {
		dbInstance, err := sql.Open("postgres", "postgres://postgres:senha@10.20.11.119/output?sslmode=disable")
		config.DB = dbInstance
		return err
	}

	configInput, err := dbtestgen.NewConfigDB("entrada", dbtestgen.Input, openConnInput)
	if err != nil {
		fmt.Printf("Error: %v", err)
		return
	}

	_, err = dbtestgen.NewConfigDB("saida", dbtestgen.Output, openConnOutput)
	if err != nil {
		fmt.Printf("Error: %v", err)
		return
	}

	// TODO: read config from yaml file
	configInput.Tables = []*dbtestgen.ConfigTable{&dbtestgen.ConfigTable{Schema: "banco", Name: "pessoaemail"}}

	parser := parserPostgres{}

	dbtestgen.RegisterParser(parser)

	err = dbtestgen.RecoverMetadata(configInput)
	if err != nil {
		fmt.Printf("Error:\n%v\n", err)
		return
	}

	for _, tbl := range configInput.Tables {
		ddl, err := tbl.ReturnTableDDL()
		if err != nil {
			fmt.Printf("Error ReturnTableDDL:\n%v\n", err)
			return
		}
		fmt.Printf("Table:\n%s\n", ddl)
	}
}
