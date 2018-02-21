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

type ColumnMetadata struct {
	Name, DDL, Default, SQLTypeColumn string
	HasConstrain                      bool
}

type ConfigTable struct {
	Name, DDL, Schema string
	Columns           []*ColumnMetadata
}

var (
	dbs []*ConfigDB
)

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

	dbs = append(dbs, c)

	return c, nil
}

func recoverTableMetadata(cfg *ConfigDB) (err error) {
	if cfg == nil || cfg.Type != Input {
		return errors.New("Any configuration is input type")
	}

	for _, tbl := range cfg.Tables {
		tbl.Columns, err = recoverColumnMetadata(tbl, cfg.DB)
	}

	return nil
}

func recoverColumnMetadata(table *ConfigTable, dbConn *sql.DB) ([]*ColumnMetadata, error) {
	return nil, nil
}
