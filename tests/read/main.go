package main

import (
	"database/sql"
	"io"
	"log"
	"os"

	"github.com/ejholmes/logstream"
	_ "github.com/lib/pq"
)

func main() {
	db, err := sql.Open("postgres", "postgres://localhost/logstream?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	r := logstream.New("abcd", db)

	if _, err := io.Copy(os.Stdout, r); err != nil {
		log.Fatal(err)
	}
}
