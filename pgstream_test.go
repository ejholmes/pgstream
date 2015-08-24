package pgstream

import (
	"database/sql"
	"reflect"
	"testing"
)

// Name of the test stream
const stream = "stream"

func TestStream_Write(t *testing.T) {
	tests := []struct {
		in  []byte
		n   int
		out []string
	}{
		{[]byte("hello world"), 11, []string{"hello world"}},
		{[]byte("hello\nworld"), 11, []string{"hello\n", "world"}},
	}

	for _, tt := range tests {
		db := newDB(t)
		rw := db.Stream(stream)

		n, err := rw.Write(tt.in)
		if err != nil {
			t.Fatal(err)
		}

		if got, want := n, tt.n; got != want {
			t.Fatalf("n => %d; want %d", got, want)
		}

		lines, err := logLines(rw)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := lines, tt.out; !reflect.DeepEqual(got, want) {
			t.Fatalf("lines => %v; want %v", got, want)
		}
	}
}

func TestStream_Read(t *testing.T) {
	tests := []struct {
		lines [][]byte
		out   string
		n     int
	}{
		{[][]byte{[]byte("hello world")}, "hello world", 11},
		{[][]byte{[]byte("hello\n"), []byte("world")}, "hello\nworld", 11},
	}

	for _, tt := range tests {
		db := newDB(t)
		rw := db.Stream(stream)

		b := make([]byte, 32*1024)

		for _, l := range tt.lines {
			if err := rw.CreateLine(l); err != nil {
				t.Fatal(err)
			}
		}

		n, err := rw.Read(b)
		if err != nil {
			t.Fatal(err)
		}

		if got, want := n, tt.n; got != want {
			t.Fatalf("n => %d; want %d", got, want)
		}
	}
}

func newDB(t *testing.T) *DB {
	db, err := sql.Open("postgres", "postgres://localhost/pgstream?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("TRUNCATE TABLE logs"); err != nil {
		t.Fatal(err)
	}

	return Open(db)
}

func logLines(s *Stream) ([]string, error) {
	var lines []string

	rows, err := s.Lines(0)
	if err != nil {
		return lines, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id   int
			text string
		)
		if err := rows.Scan(&id, &text); err != nil {
			return lines, err
		}
		lines = append(lines, text)
	}

	return lines, nil
}

func String(s string) *string {
	return &s
}
