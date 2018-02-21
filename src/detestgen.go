package dbtestgen

import (
	"database/sql"
	"errors"
)

// DBTarget - Tell us if the database is the input target or output target
type DBTarget int

const (
	// Input - database origin of data
	Input DBTarget = iota
	// Output - database target to the data
	Output
)

var (
	parserDDL Parser
)

// Parser - defines the parser that implements queries to returns the DDL
type Parser interface {
	ParseColumns(db *sql.DB, schemaName, tableName string) (columnsDefinitions []sql.ColumnType, err error)
	ParseConstraints(db *sql.DB, schemaName, tableName string) (constraintsDefinitions map[string]string, err error)
	RawColumnType(col sql.ColumnType) (sqlType string, err error)
}

// RegisterParser - function to register the parser according to the Driver
func RegisterParser(parser Parser) {
	if parserDDL = parser; parserDDL != nil {
		panic("The parser can't be nil.")
	}
}

// ConfigDB - Stores the configuration to connect to databases
type ConfigDB struct {
	DB     *sql.DB
	Name   string
	Type   DBTarget
	Tables []*ConfigTable
}

func (cfg *ConfigDB) checkConn() error {
	return cfg.DB.Ping()
}

type ConstraintMetadata struct {
	Name, DDL string
}

type ColumnMetadata struct {
	SQLTypeColoumn                    sql.ColumnType
	Name, DDL, Default, RawTypeColumn string
	HasConstrain                      bool
}

type ConfigTable struct {
	Name, DDL, Schema string
	Columns           []*ColumnMetadata
	Constraints       []*ConstraintMetadata
}

// NewConfigDB - Returns a new instance of **dbtestgen**.ConfigDB
func NewConfigDB(name string, target DBTarget, cfgs ...func(*sql.DB) error) (c *ConfigDB, err error) {
	c = &ConfigDB{Name: name, Type: target}
	for _, fn := range cfgs {
		if err = fn(c.DB); err != nil {
			return nil, err
		}
	}

	if err = c.checkConn(); err != nil {
		return nil, err
	}

	return c, nil
}

func recoverTableMetadata(cfg *ConfigDB) (err error) {
	if parserDDL == nil {
		panic("The parser wasn't configured. Call RegisterParser before start.")
	}

	if cfg == nil || cfg.Type != Input {
		return errors.New("Any configuration is input type")
	}

	for _, tbl := range cfg.Tables {
		tbl.Columns, err = recoverColumnsMetadata(cfg, tbl)
		tbl.Constraints, err = recoverConstraintsMetadata(cfg, tbl)
	}

	return err
}

func recoverColumnsMetadata(cfg *ConfigDB, tbl *ConfigTable) ([]*ColumnMetadata, error) {
	return nil, nil
}

func recoverConstraintsMetadata(cfg *ConfigDB, tbl *ConfigTable) ([]*ConstraintMetadata, error) {
	return nil, nil
}
