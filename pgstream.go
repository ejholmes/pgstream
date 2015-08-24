package pgstream

import (
	"bufio"
	"bytes"
	"database/sql"
	"io"
	"time"
)

const DefaultTable = "logs"

type Stream struct {
	// Table to write the log lines to.
	Table string

	// Unique identifier for the stream.
	name string

	db    *sql.DB
	id    int
	calls int

	// Controls the amount of time to wait before making the next query when
	// reading. This provides exponential backoff when there are no new
	// records.
	timeout time.Duration
}

func New(name string, db *sql.DB) *Stream {
	return &Stream{
		name: name,
		db:   db,
	}
}

func (r *Stream) Read(p []byte) (n int, err error) {
	// Current index into p
	var idx int

	r.calls += 1
	// This means we're on atleast the second Read. We'll wait for the
	// current timeout before making another query.
	if r.calls > 0 {
		<-time.After(r.timeout)
	}

	rows, err := r.db.Query(`SELECT id, text FROM `+r.table()+` WHERE id > $1 and stream = $2`, r.id, r.stream())
	if err != nil {
		return n, err
	}
	defer rows.Close()

	// Data about the log line.
	var (
		id   int
		ts   *[]byte
		text []byte
	)

	for rows.Next() {
		if err = rows.Scan(&id, &ts); err != nil {
			break
		}

		// When the text is null, we're at the last line. Return
		// io.EOF to indicate the error.
		if ts == nil {
			err = io.EOF
			break
		}

		text = *ts

		// If we don't have enough space in p to copy the text, return
		// what we have so Read can be called again.
		if idx+len(text) > len(p) {
			break
		}

		// Set r.id so that calling Read again will only read new lines.
		r.id = id

		// Copy the text into the buffer.
		copy(p[idx:idx+len(text)], text)
		n += len(text)
		idx += len(text)
	}

	// This means the query didn't return any rows. Increase the timeout.
	if id == 0 {
		r.timeout = time.Second
	}

	return
}

func (w *Stream) Write(p []byte) (int, error) {
	r := bufio.NewReader(bytes.NewReader(p))

	createLine := func(text []byte) error {
		q := `INSERT INTO ` + w.table() + `(stream, text) VALUES ($1, $2)`
		_, err := w.db.Exec(q, w.stream(), text)
		return err
	}

	read := len(p)

	for {
		b, err := r.ReadBytes('\n')

		if err != nil {
			if err == io.EOF {
				return read, createLine(b)
			} else {
				return read, err
			}
		}

		if err := createLine(b); err != nil {
			return read, err
		}
	}

	return read, nil
}

func (rw *Stream) Close() error {
	_, err := rw.db.Exec(`INSERT INTO `+rw.table()+`(stream, text) VALUES ($1, NULL)`, rw.stream())
	return err
}

func (rw *Stream) table() string {
	if rw.Table == "" {
		return DefaultTable
	}

	return rw.Table
}

func (rw *Stream) stream() string {
	if rw.name == "" {
		panic("No stream provided")
	}

	return rw.name
}
