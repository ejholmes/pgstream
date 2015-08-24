package pgstream

import (
	"bufio"
	"bytes"
	"database/sql"
	"io"
	"time"
)

const DefaultTable = "logs"

// DB wraps a sql.DB to provide a Stream method to create io.ReadWriter streams.
type DB struct {
	*sql.DB
}

// Open returns a new DB instance.
func Open(db *sql.DB) *DB {
	return &DB{DB: db}
}

// Stream returns a new Stream instance, which implements the io.ReadWriter
// interface.
func (db *DB) Stream(name string) *Stream {
	return &Stream{
		name: name,
		db:   db,
	}
}

type Stream struct {
	Table string

	// Unique identifier for the stream.
	name string

	db    *DB
	id    int
	calls int

	// Controls the amount of time to wait before making the next query when
	// reading. This provides exponential backoff when there are no new
	// records.
	timeout time.Duration
}

// Reads len(p) data from the stream into p.
func (r *Stream) Read(p []byte) (n int, err error) {
	// Current index into p
	var idx int

	r.calls += 1
	// This means we're on atleast the second Read. We'll wait for the
	// current timeout before making another query.
	if r.calls > 0 {
		<-time.After(r.timeout)
	}

	rows, err := r.Lines(r.id)
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

// Writes the stream of data to the database.
func (w *Stream) Write(p []byte) (n int, err error) {
	r := bufio.NewReader(bytes.NewReader(p))

	var (
		b   []byte
		eof bool
	)

	// Reads out each line until eof, creating a log line in the database
	// for each line.
	// TODO(ejholmes): Do a bulk insert.
	for !eof {
		b, err = r.ReadBytes('\n')
		n += len(b)

		if err != nil {
			if err == io.EOF {
				eof = true
			} else {
				break
			}
		}

		if err = w.CreateLine(b); err != nil {
			break
		}
	}

	return
}

func (rw *Stream) Close() error {
	_, err := rw.db.Exec(`INSERT INTO `+rw.table()+`(stream, text) VALUES ($1, NULL)`, rw.stream())
	return err
}

// CreateLine adds a single line of text to this stream.
func (rw *Stream) CreateLine(text []byte) error {
	q := `INSERT INTO ` + rw.table() + `(stream, text) VALUES ($1, $2)`
	_, err := rw.db.Exec(q, rw.stream(), text)
	return err
}

// Lines returns sql.Rows containing all of the log lines for this stream since
// start.
func (rw *Stream) Lines(start int) (*sql.Rows, error) {
	q := `SELECT id, text FROM ` + rw.table() + ` WHERE id > $1 and stream = $2`
	return rw.db.Query(q, start, rw.stream())
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
