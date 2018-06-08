# dbtestgen - Database Test Generator

CLI to copy partially relational models from a SQL database to generate others databases. Including primary keys, constraints, uniques and procedures.

__Tested with PostgreSQL 9.4+__

## Motivation

I start with this goal: a CLI that just copy some parts of a relational database to test specific areas of a legacy software. I want update with some features, like:

- generate automatically the other database.
- transfer to new database the data automatically.
- to indicate how much of data in tables you wish.

## Installation

```bash
go get github.com/pvoliveira/dbtestgen
```

or

```bash
go install github.com/pvoliveira/dbtestgen
```

## Using

### Configuration file

A example of configuration file:

```yml
procs:
  -
    schema: public
    name: func1
tables:
  -
    schema: public
    name: table1
  -
    schema: public
    name: table2
```

### CLI

The connection string follows the pattern of _database/sql_ package:

```bash
$ dbtestgen
missing subcommands: inputdb and tables
  -config string
        file configuring tables to use as input (default "config.yml")
  -db string
        connectionstring to input database ('{dialect}://{user}:{password}@{host}/{databasename}[?{parameters=value}]')
```

Running the cli, it will return a SQL script with DDL commands to create tables, contraints and procedures passed in configuration file.

This it's the first version, as I said above, more feature are coming. If you have any problem to report or sugestion, please open a issue. Thanks!