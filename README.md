# dbtestgen

CLI to copy partially relational models from a SQL database to generate others databases.

# Description

The goal with this CLI is copy just some parts of a relational database to test specifics areas of a legacy software. I want update with some features, by example: to indicate how much of data in tables you wish; generate automatically the other database; transfer to new database the data automatically.

# Installation

```
go get github.com/pvoliveira/dbtestgen

go install github.com/pvoliveira/dbtestgen
```

# Using



## Configuration file

```yml
procs:
  - schema: public
    name: func1
tables:
  - schema: public
    name: table1
    where: | fieldString = "string" and fieldNumber = 123

  - schema: public
    name: table2
    where: 
```

# CLI

## TODO

- [X] Pass configuration to the CLI;
- [ ] Execute DDL on output DBs;
- [X] Extract data from table too;
- [X] _Where_ parameter to filter data from tables;
- [ ] Tests;
- [ ] Get better API;
- [ ] SemVer;