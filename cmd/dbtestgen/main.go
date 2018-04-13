package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"

	_ "github.com/lib/pq"
	"github.com/pvoliveira/dbtestgen"
	"gopkg.in/yaml.v2"
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

	constraintsDefinitions = make(map[string]dbtestgen.ConstraintMetadata)

	for rows.Next() {
		var constr = dbtestgen.ConstraintMetadata{}
		var definition string
		if err := rows.Scan(&constr.Name, &definition, &constr.TableNameRelated, &constr.Type); err != nil {
			return nil, err
		}

		constr.DDL = "ALTER TABLE " + schemaName + "." + tableName + " ADD CONSTRAINT " + constr.Name + " " + definition + ";"
		constraintsDefinitions[constr.Name] = constr
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

	return ddl, nil
}

func (p parserPostgres) ParseProcedures(db *sql.DB, schemaName, procedureName string) (string, error) {

	rows, err := db.Query(`SELECT /*n.nspname || '.' || proname AS fname,*/ pg_get_functiondef(p.oid) as definition
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
	 AND proname~'` + procedureName + `'`)

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

type configFileProcs struct {
	Schema, Name string
}

type configFileTables struct {
	Schema, Name, Where string
}

type configFile struct {
	Procs  []configFileProcs
	Tables []configFileTables
}

func main() {
	var connStrInput string
	var tablesConfig string
	flag.StringVar(&connStrInput, "inputdb", "", "connectionstring to input database ('{dialect}://{user}:{password}@{host}/{databasename}[?{parameters=value}]')")
	flag.StringVar(&tablesConfig, "tables", "", "tables with respectives schemas ('schema.tableone[,schema.tabletwo]')")

	flag.Parse()

	if flag.NFlag() < 2 {
		fmt.Fprintln(os.Stderr, "missing subcommands: inputdb and tables")

		flag.PrintDefaults()

		os.Exit(1)
	}

	// set the connection string to connect with input database
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
		fileConfig, err := os.Open(tablesConfig)
		if err != nil {
			return err
		}
		defer fileConfig.Close()

		fileContent, err := ioutil.ReadAll(fileConfig)
		if err != nil {
			return err
		}

		parameters := configFile{}
		err = yaml.Unmarshal(fileContent, &parameters)
		if err != nil {
			return err
		}

		fmt.Printf("config file converted:\n%+v\n\n", parameters)

		for _, tbl := range parameters.Tables {
			config.Tables = append(config.Tables, &dbtestgen.ConfigTable{Schema: tbl.Schema, Name: tbl.Name, Where: tbl.Where})
		}

		for _, prc := range parameters.Procs {
			config.Procs = append(config.Procs, &dbtestgen.ConfigProc{Schema: prc.Schema, Name: prc.Name})
		}

		return nil
	}

	configInput, err := dbtestgen.AddConfigDB("entrada", dbtestgen.Input, openConnInput, configInputTables)
	if err != nil {
		fmt.Printf("Error: %v", err)
		os.Exit(1)
	}

	dbtestgen.RegisterParser(parserPostgres{})

	sql, err := configInput.GenerateScript()
	if err != nil {
		fmt.Fprint(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Fprint(os.Stdout, sql)
}
