// Package old expose methods and a interface to create dialect
// used to return metadata around tables and constraint from a database
package old

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
	ParseColumns(db *sql.DB, schemaName, tableName string) ([]sql.ColumnType, error)

	// ParseConstraints Returns DDL statement of constraints like primary key, foreign key, uniques, etc.
	// Examples (PostgreSQL):
	// `ALTER TABLE distributors ADD CONSTRAINT dist_id_zipcode_key UNIQUE (dist_id, zipcode);`
	// `ALTER TABLE distributors ADD CONSTRAINT distfk FOREIGN KEY (address) REFERENCES addresses (address) MATCH FULL;`
	ParseConstraints(db *sql.DB, schemaName, tableName string) (map[string]ConstraintMetadata, error)

	// RawColumnDefinition Returns the DDL block on a create table command, like:
	// `id UUID NOT NULL`
	// `description VARCHAR(200) NOT NULL`
	// `created DATE NULL DEFAULT CURRENT_DATE`
	// examples on PostgreSQL.
	RawColumnDefinition(col sql.ColumnType) (string, error)

	// ParseProcedures returns DDL statement of a procedure identify by schema and name
	ParseProcedures(db *sql.DB, schemaName, procedureName string) (string, error)
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
	Procs  []*ConfigProc
}

func (cfg *ConfigDB) checkConn() error {
	return cfg.DB.Ping()
}

// GenerateScript Returns the script SQL to generate tables and relationship constraints
func (cfg *ConfigDB) GenerateScript() (string, error) {
	if cfg.Type != Input {
		return "", errors.New("configuration must be Input type")
	}

	err := recoverMetadata(cfg)
	if err != nil {
		return "", err
	}

	sql, err := joinTablesCreateDDL(cfg.Tables...)
	if err != nil {
		return "", err
	}

	// DDL to create procedures
	procs, err := joinProceduresCreateDDL(cfg.Procs...)
	if err != nil {
		return "", err
	}

	return strings.Join([]string{sql, procs}, "\n\n"), nil
}

// ConstraintMetadata Define metadata of constraints
type ConstraintMetadata struct {
	Name, DDL, TableNameRelated, Type string
}

// ColumnMetadata Define metadata of columns
type ColumnMetadata struct {
	SQLColumnType      sql.ColumnType
	Name, DDL, Default string
	HasConstrain       bool
}

// ConfigTable Define metadata of table
type ConfigTable struct {
	Name, Schema, Where string
	columns             []*ColumnMetadata
	constraints         []*ConstraintMetadata
}

// returnTableDDL Returns the DDL string of table
func (cfg *ConfigTable) returnTableDDL() (string, error) {
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

func (cfg *ConfigTable) generateInsertStatements(db *ConfigDB) (string, error) {
	if cfg.columns == nil || len(cfg.columns) == 0 {
		return "", errors.New("table doesn't have columns")
	}

	if len(cfg.Where) == 0 {
		return "", errors.New("where condition needed to generate inserts")
	}

	columnNames := make([]string, 0)
	for _, col := range cfg.columns {
		columnNames = append(columnNames, col.Name)
	}

	rows, err := db.DB.Query(`SELECT `+strings.Join(columnNames, ", ")+` FROM `+cfg.Schema+`.`+cfg.Name+` WHERE `+cfg.Where, nil)
	if err != nil {
		return "", errors.New("error on try select data from " + cfg.Schema + "." + cfg.Name + ":\n" + err.Error())
	}

	insertsStatements := make([]string, 0)

	// I'm not happy about that
	for rows.Next() {
		results := make([]interface{}, len(columnNames))
		if err := rows.Scan(results...); err != nil {
			return "", errors.New("error on try read the results of query:\n" + err.Error())
		}

		resultsParsed := make([]string, len(columnNames))

		for i := range results {
			resultsParsed[i] = ""
		}

		insertsStatements = append(insertsStatements, `INSERT INTO `+cfg.Schema+`.`+cfg.Name+` (`+strings.Join(columnNames, ", ")+`) VALUES (`+strings.Join(resultsParsed, ", ")+`);`)
	}

	return strings.Join(insertsStatements, "\n"), nil
}

// ConfigProc defines metadata of procedures
type ConfigProc struct {
	Name, Schema, DDL string
}

func addConfig(db *ConfigDB) error {
	dbsMu.Lock()
	defer dbsMu.Unlock()

	if db == nil {
		panic("db parameter can't be nil")
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

	err = addConfig(c)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// recoverMetadata Process tables configurated to get from database the DDL scripts
func recoverMetadata(cfg *ConfigDB) (err error) {
	if parserDDL == nil {
		panic("parser wasn't configured\ncall RegisterParser before start")
	}

	if cfg == nil || cfg.Type != Input {
		return errors.New("can't recover metadata from output configurations")
	}

	inputTables := make(map[string]bool)
	for _, t := range cfg.Tables {
		inputTables[t.Name] = true
	}

	for _, tbl := range cfg.Tables {
		tbl.columns, err = recoverColumnsMetadata(cfg.DB, tbl.Schema, tbl.Name)
		if err != nil {
			return err
		}

		// constraints related with input tables
		constraints, err := recoverConstraintsMetadata(cfg.DB, tbl.Schema, tbl.Name)
		if err != nil {
			return err
		}

		tbl.constraints = make([]*ConstraintMetadata, 0)

		for _, typecontraint := range []rune{'p', 'f'} {
			for _, cstr := range constraints {
				if strings.ContainsRune(cstr.Type, typecontraint) {
					tablename := cstr.TableNameRelated
					if strings.ContainsAny(tablename, ".") {
						tablename = strings.Split(tablename, ".")[1]
					}

					if _, ok := inputTables[tablename]; ok {
						tbl.constraints = append(tbl.constraints, cstr)
					}
				}
			}
		}
	}

	for _, prc := range cfg.Procs {
		if len(prc.Schema) > 0 && len(prc.Name) > 0 {
			procDDL, err := recoverProceduresMetadata(cfg.DB, prc.Schema, prc.Name)
			if err != nil {
				return err
			}

			if len(procDDL) > 0 {
				prc.DDL = procDDL
			}
		}
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
		panic("the parser wasn't configured, call RegisterParser before start")
	}

	if db == nil {
		return nil, errors.New("db aren't passed")
	}

	var cons map[string]ConstraintMetadata
	if cons, err = parserDDL.ParseConstraints(db, schemaName, tableName); err != nil {
		return nil, err
	}

	metadata = make([]*ConstraintMetadata, 0)
	for _, v := range cons {
		constraint := v
		metadata = append(metadata, &constraint)
	}

	return metadata, nil
}

func recoverProceduresMetadata(db *sql.DB, schemaName, procName string) (string, error) {
	if parserDDL == nil {
		panic("the parser wasn't configured, call RegisterParser before start")
	}

	if db == nil {
		return "", errors.New("db aren't passed")
	}

	ddl, err := parserDDL.ParseProcedures(db, schemaName, procName)
	if err != nil {
		return "", err
	}

	return ddl, nil
}

func joinTablesCreateDDL(tables ...*ConfigTable) (string, error) {
	if tables == nil {
		return "", errors.New("some config tables are needed")
	}

	ddl := make([]string, 0)
	allContraints := make([]*ConstraintMetadata, 0)

	// add 'create table' to script
	for _, t := range tables {
		sql, err := t.returnTableDDL()
		if err != nil {
			return "", err
		}
		ddl = append(ddl, sql)
		allContraints = append(allContraints, t.constraints...)
	}

	ddlTable := strings.Join(ddl, "\n\n")

	// add 'alter table add constraint' to script
	sortedConstraints := make([]*ConstraintMetadata, 0)
	for _, typeconstraint := range []rune{'p', 'f'} {
		for _, constraint := range allContraints {
			if strings.ContainsRune(constraint.Type, typeconstraint) {
				sortedConstraints = append(sortedConstraints, constraint)
			}
		}
	}

	ddlConstraints, err := joinConstraintsCreateDDL(sortedConstraints...)
	if err != nil {
		return "", err
	}

	return strings.Join([]string{ddlTable, ddlConstraints}, "\n\n"), nil
}

func joinConstraintsCreateDDL(constraints ...*ConstraintMetadata) (string, error) {
	if constraints == nil {
		return "", errors.New("some constraints metadata are needed")
	}

	ddl := make([]string, 0)

	for _, c := range constraints {
		ddl = append(ddl, c.DDL)
	}

	return strings.Join(ddl, "\n\n"), nil
}

func joinProceduresCreateDDL(procs ...*ConfigProc) (string, error) {
	if procs == nil {
		return "", errors.New("some procs metadata are needed")
	}

	ddl := make([]string, 0)

	for _, prc := range procs {
		ddl = append(ddl, prc.DDL)
	}

	return strings.Join(ddl, ";\n\n"), nil
}
