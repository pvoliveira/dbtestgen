// Package dbtestgen expose methods and a interface to create dialect
// used to return metadata around tables and constraint from a database
package dbtestgen

import (
	"bytes"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"text/template"
)

const (
	// Input database origin of data
	Input DBTarget = iota
	// Output database target to the data
	Output
)

var (
	dbsMu               sync.RWMutex
	dbs                 = make(map[string]*ConfigDB)
	parserDDL           Parser
	createTableTemplate = `CREATE TABLE {{.Schema}}.{{.Name}} ( {{.Columns}} );`
	funcJoinString      = template.FuncMap{"join": strings.Join}
)

// DBTarget Tell us if the database is the input target or output target
type DBTarget int

// Parser defines the parser that implements queries to returns the DDL statement.
type Parser interface {
	// ParseColumns Returns array of sql.ColumnType according to columns of table.
	ParseColumns(db *sql.DB, schemaName, tableName string) (columnsDefinitions []sql.ColumnType, err error)

	// ParseConstraints Returns DDL statement of constraints like primary key, foreign key, uniques, etc.
	// Examples (PostgreSQL):
	// `ALTER TABLE distributors ADD CONSTRAINT dist_id_zipcode_key UNIQUE (dist_id, zipcode);`
	// `ALTER TABLE distributors ADD CONSTRAINT distfk FOREIGN KEY (address) REFERENCES addresses (address) MATCH FULL;`
	ParseConstraints(db *sql.DB, schemaName, tableName string) (constraintsDefinitions map[string]string, err error)

	// RawColumnDefinition Returns the DDL block on a create table command, like:
	// `ID UUID NOT NULL`
	// `DESCRIPTION VARCHAR(200) NOT NULL`
	// `CREATED DATE NULL DEFAULT CURRENT_DATE`
	// examples on PostgreSQL.
	RawColumnDefinition(col sql.ColumnType) (sqlType string, err error)
}

// RegisterParser function to register the parser according to the Driver
func RegisterParser(parser Parser) {
	if parserDDL = parser; parserDDL == nil {
		panic("The parser can't be nil.")
	}
}

// ConfigDB Stores the configuration to connect to databases
type ConfigDB struct {
	DB     *sql.DB
	Name   string
	Type   DBTarget
	Tables []*ConfigTable
}

func (cfg *ConfigDB) checkConn() error {
	return cfg.DB.Ping()
}

// ConstraintMetadata Define metadata of constraints
type ConstraintMetadata struct {
	Name, DDL string
}

// ColumnMetadata Define metadata of columns
type ColumnMetadata struct {
	SQLColumnType      sql.ColumnType
	Name, DDL, Default string
	HasConstrain       bool
}

// ConfigTable Define metadata of table
type ConfigTable struct {
	Name, DDL, Schema string
	columns           []*ColumnMetadata
	constraints       []*ConstraintMetadata
}

func addConfig(db *ConfigDB) error {
	dbsMu.Lock()
	defer dbsMu.Unlock()

	if db == nil {
		panic("The db parameter can't be nil")
	}

	if _, ok := dbs[db.Name]; ok {
		return errors.New("Configuration already exists")
	}

	var hasInput bool
	for _, dbConfig := range dbs {
		if !hasInput {
			hasInput = (dbConfig.Type == Input)
		} else {
			if dbConfig.Type == Input {
				return errors.New("Input configuration already exists")
			}
		}
	}

	dbs[db.Name] = db

	return nil
}

// ReturnConfigDBs Returns all ConfigDB created
func ReturnConfigDBs() map[string]*ConfigDB {
	dbsMu.Lock()
	defer dbsMu.Unlock()
	return dbs
}

// ClearConfigDBs Clears all ConfigDB created
func ClearConfigDBs() {
	dbsMu.Lock()
	defer dbsMu.Unlock()

	dbs = make(map[string]*ConfigDB)
}

// AddConfigDB Configures a new instance of ConfigDB and return it
func AddConfigDB(name string, target DBTarget, cfgs ...func(*ConfigDB) error) (c *ConfigDB, err error) {
	c = &ConfigDB{Name: name, Type: target, DB: new(sql.DB)}
	for _, fn := range cfgs {
		if err = fn(c); err != nil {
			return nil, err
		}
	}

	if err = c.checkConn(); err != nil {
		return nil, err
	}

	addConfig(c)

	return c, nil
}

// RecoverMetadata Process tables configurated to get from database the DDL scripts
func RecoverMetadata(cfg *ConfigDB) (err error) {
	if parserDDL == nil {
		panic("The parser wasn't configured. Call RegisterParser before start.")
	}

	if cfg == nil || cfg.Type != Input {
		return errors.New("Any configuration is input type")
	}

	for _, tbl := range cfg.Tables {
		tbl.columns, err = recoverColumnsMetadata(cfg.DB, tbl.Schema, tbl.Name)
		//tbl.Constraints, err = recoverConstraintsMetadata(cfg.DB, tbl.Schema, tbl.Name)
	}

	return err
}

func recoverColumnsMetadata(db *sql.DB, schemaName, tableName string) (metadata []*ColumnMetadata, err error) {
	if parserDDL == nil {
		panic("The parser wasn't configured. Call RegisterParser before start.")
	}

	if db == nil {
		return nil, errors.New("Any configuration is input type")
	}

	if cols, err := parserDDL.ParseColumns(db, schemaName, tableName); err == nil {
		for _, col := range cols {
			var colDefinition string
			colName := col.Name()
			colDefinition, err = parserDDL.RawColumnDefinition(col)
			metadata = append(metadata, &ColumnMetadata{Name: colName, DDL: colDefinition, SQLColumnType: col})
		}
	} else {
		return nil, err
	}

	return metadata, nil
}

func recoverConstraintsMetadata(db *sql.DB, schemaName, tableName string) (metadata []*ConstraintMetadata, err error) {
	if parserDDL == nil {
		panic("The parser wasn't configured. Call RegisterParser before start.")
	}

	if db == nil {
		return nil, errors.New("Any configuration is input type")
	}

	if cons, err := parserDDL.ParseConstraints(db, schemaName, tableName); err == nil {
		for name, constraint := range cons {
			metadata = append(metadata, &ConstraintMetadata{Name: name, DDL: constraint})
		}
	} else {
		return nil, err
	}

	return metadata, nil
}

// ReturnTableDDL Returns the DDL string of table
func (cfg *ConfigTable) ReturnTableDDL() (string, error) {
	if cfg.columns == nil || len(cfg.columns) == 0 {
		return "", errors.New("Table haven't columns")
	}

	tmplMain, err := template.New("tabletmpl").Parse(createTableTemplate)
	if err != nil {
		return "", err
	}

	// gets just the ddl of columns
	columns := make([]string, 0)
	for _, meta := range cfg.columns {
		columns = append(columns, meta.DDL)
	}

	// type to fit with template model
	data := struct {
		Name, Schema, Columns string
	}{cfg.Name, cfg.Schema, strings.Join(columns, ",\n")}

	buf := new(bytes.Buffer)
	err = tmplMain.Execute(buf, data)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

func joinTablesCreateDDL(tables ...*ConfigTable) (string, error) {
	if tables == nil {
		return "", errors.New("some ConfigTable needed")
	}

	ddl := make([]string, 0)

	for _, t := range tables {
		sql, err := t.ReturnTableDDL()
		if err != nil {
			return "", err
		}
		ddl = append(ddl, sql)
	}

	return strings.Join(ddl, "\n"), nil
}
