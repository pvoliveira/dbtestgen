package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/pvoliveira/dbtestgen"
	"github.com/pvoliveira/dbtestgen/postgres"
	"gopkg.in/yaml.v2"
)

type configFile struct {
	Procs  []*dbtestgen.Procedure `json:"procs"`
	Tables []*dbtestgen.Table     `json:"tables"`
}

func main() {
	var db string
	var config string
	flag.StringVar(&db, "db", "", "connectionstring to input database ('{dialect}://{user}:{password}@{host}/{databasename}[?{parameters=value}]')")
	flag.StringVar(&config, "config", "config.yml", "file configuring tables to use as input")

	flag.Parse()

	if flag.NFlag() < 2 {
		fmt.Fprintln(os.Stderr, "missing subcommands: inputdb and tables")

		flag.PrintDefaults()

		os.Exit(1)
	}

	//dbInstance, err := sql.Open("postgres", "postgres://postgres:senha@10.20.11.119/input?sslmode=disable")
	//dbInstance, err := sql.Open("postgres", "postgres://pagoufacil:pagoufacilw3b@10.20.11.106/pagoufacildb?sslmode=disable")

	fileConfig, err := os.Open(config)
	if err != nil {
		panic(err)
	}
	defer fileConfig.Close()

	fileContent, err := ioutil.ReadAll(fileConfig)
	if err != nil {
		panic(err)
	}

	var parameters configFile
	err = yaml.Unmarshal(fileContent, &parameters)
	if err != nil {
		panic(err)
	}

	exec, err := postgres.NewExecutor(db)
	if err != nil {
		panic(err)
	}

	err = exec.RegisterProcedures(parameters.Procs)
	if err != nil {
		panic(err)
	}

	err = exec.RegisterTables(parameters.Tables)
	if err != nil {
		panic(err)
	}

	sql, err := exec.ReturnScript()
	if err != nil {
		fmt.Fprint(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Fprint(os.Stdout, sql)
}
