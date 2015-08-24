package pgstream_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"strings"

	"github.com/ejholmes/pgstream"
	_ "github.com/lib/pq"
)

const poem = `Let us take the river
path near Fall Hill.

There we will negotiate
an outcrop with its silvered
initials & other bits of graffiti,

all the way to the broken edge
that overlooks the bend,
& hold hands until

we can no longer tell
where the river ends.`

func Example() {
	db := newDB()
	stream := db.Stream("stream")

	// Start writing the poem to stream in a separate goroutine.
	go write(stream)

	print(stream)
	// Output:
	// Let us take the river
	// path near Fall Hill.
	//
	// There we will negotiate
	// an outcrop with its silvered
	// initials & other bits of graffiti,
	//
	// all the way to the broken edge
	// that overlooks the bend,
	// & hold hands until
	//
	// we can no longer tell
	// where the river ends.
}

func write(w io.WriteCloser) error {
	if _, err := io.Copy(w, strings.NewReader(poem)); err != nil {
		return err
	}

	return w.Close()
}

func print(r io.Reader) error {
	b := new(bytes.Buffer)
	if _, err := io.Copy(b, r); err != nil {
		return err
	}
	fmt.Print(b)
	return nil
}

func newDB() *pgstream.DB {
	db, err := sql.Open("postgres", "postgres://localhost/pgstream?sslmode=disable")
	if err != nil {
		panic(err)
	}
	db.Exec("TRUNCATE TABLE logs")

	return pgstream.Open(db)
}
