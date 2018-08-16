package main

import (
	"database/sql"
	"fmt"

	_ "github.com/pchatsu/go-spanner-driver"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

type dto struct {
	key   string
	value string
}

func main() {
	exec("mysql", "root:password@/test")
	exec("spanner", "root")
}

func exec(dn string, dsn string) error {
	db, err := sql.Open(dn, dsn)
	if err != nil {
		log.Fatal(err)
		return err
	}

	rows, err := db.Query("SELECT t.key `key`, t.value `value` FROM t_mock t")
	if err != nil {
		log.Fatal(err)
		return err
	}

	for rows.Next() {
		var dto dto
		err := rows.Scan(&dto.key, &dto.value)
		if err != nil {
			log.Fatal(err)
			return err
		}

		fmt.Println(dto.key)
		fmt.Println(dto.value)
	}

	defer db.Close()
	return nil
}
