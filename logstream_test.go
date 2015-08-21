package logstream

import (
	"bytes"
	"database/sql"
	"io"
	"strings"
	"testing"
	"time"

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

func TestStream(t *testing.T) {
	const stream = "1234"

	db := newDB(t)

	rw := NewStream(db)
	rw.Name = stream

	if _, err := io.Copy(rw, strings.NewReader(logs)); err != nil {
		t.Fatal(err)
	}
	if err := rw.Close(); err != nil {
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

func TestStream_ReadUntilClose(t *testing.T) {
	const stream = "1234"

	db := newDB(t)

	rw := NewStream(db)
	rw.Name = stream

	if _, err := io.Copy(rw, strings.NewReader(logs)); err != nil {
		t.Fatal(err)
	}

	b := new(bytes.Buffer)
	done := make(chan struct{})
	go func() {
		if _, err := io.Copy(b, rw); err != nil {
			t.Fatal(err)
		}
		close(done)
	}()

	<-time.After(3 * time.Second)

	io.WriteString(rw, "Foo")

	if err := rw.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	if b.String() != logs+"Foo" {
		t.Fatalf("Logs => %q", b.String())
	}
}

func newDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`CREATE TABLE logs (id integer not null primary key, stream text, text text, closed boolean not null default false)`); err != nil {
		t.Fatal(err)
	}

	return db
}
