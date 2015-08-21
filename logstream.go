package logstream

import (
	"bufio"
	"bytes"
	"database/sql"
	"io"
	"strings"
)

const DefaultTable = "logs"

type Stream struct {
	// Table to write the log lines to.
	Table string
	Name  string

	db *sql.DB
}

func NewStream(db *sql.DB) *Stream {
	return &Stream{
		db: db,
	}
}

func (r *Stream) Read(p []byte) (int, error) {
	var (
		// Number of bytes read
		n int
		// Current index into p
		idx int
	)

	rows, err := r.db.Query(`SELECT text FROM `+r.table()+` WHERE stream = ?`, r.stream())
	if err != nil {
		return n, err
	}
	defer rows.Close()

	for rows.Next() {
		var text string
		if err := rows.Scan(&text); err != nil {
			return n, err
		}

		l := len(text)

		copy(p[idx:idx+l], []byte(text))
		n = n + l
		idx += l
	}

	return n, io.EOF
}

func (w *Stream) Write(p []byte) (int, error) {
	r := bufio.NewReader(bytes.NewReader(p))

	createLine := func(text string) error {
		q := `INSERT INTO ` + w.table() + `(stream, text) VALUES (?, ?)`
		_, err := w.db.Exec(q, w.stream(), text)
		return err
	}

	read := len(p)

	for {
		b, err := r.ReadBytes('\n')

		// Heroku may send a null character as a heartbeat signal. We
		// want to strip out any null characters, as inserting them into
		// postgres will cause an error.
		line := strings.Replace(string(b), "\x00", "", -1)

		if err != nil {
			if err == io.EOF {
				return read, createLine(line)
			} else {
				return read, err
			}
		}

		if err := createLine(line); err != nil {
			return read, err
		}
	}

	return read, nil
}

func (rw *Stream) table() string {
	if rw.Table == "" {
		return DefaultTable
	}

	return rw.Table
}

func (rw *Stream) stream() string {
	if rw.Name == "" {
		panic("No stream provided")
	}

	return rw.Name
}
