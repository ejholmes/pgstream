package logstream

import (
	"bytes"
	"database/sql"
	"io"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

const logs = `Let us take the river
path near Fall Hill.

There we will negotiate
an outcrop with its silvered
initials & other bits of graffiti,

all the way to the broken edge
that overlooks the bend,
& hold hands until

we can no longer tell
where the river ends.`

func TestWriter(t *testing.T) {
	const stream = "1234"

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	db.Exec(`CREATE TABLE logs (id integer not null primary key, stream text, text text)`)

	rw := NewStream(db)
	rw.Name = stream

	if _, err := io.Copy(rw, strings.NewReader(logs)); err != nil {
		t.Fatal(err)
	}

	b := new(bytes.Buffer)
	if _, err := io.Copy(b, rw); err != nil {
		t.Fatal(err)
	}

	if b.String() != logs {
		t.Fatalf("Logs => %q", b.String())
	}
}
