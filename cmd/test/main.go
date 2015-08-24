// Just a small CLI application that writes to Stdout whatever it reads from
// Stdin, using a database stream.
package main

import (
	"database/sql"
	"io"
	"log"
	"os"

	"github.com/ejholmes/pgstream"
	_ "github.com/lib/pq"
)

func main() {
	db, err := sql.Open("postgres", "postgres://localhost/pgstream?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	rw := pgstream.Open(db).Stream("abcd")

	go func() {
		if _, err := io.Copy(rw, os.Stdin); err != nil {
			log.Fatal(err)
		}

		if err := rw.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	if _, err := io.Copy(os.Stdout, rw); err != nil {
		log.Fatal(err)
	}
}
