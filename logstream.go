package logstream

import (
	"bufio"
	"bytes"
	"database/sql"
	"io"
	"strings"
	"time"
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
		id  int
		n   int
		nn  int
		err error
	)

	for err == nil {
		<-time.After(time.Second)
		id, nn, err = r.read(p[n:len(p)], id)
		n = n + nn
	}

	return n, err
}

func (r *Stream) read(p []byte, start int) (int, int, error) {
	var (
		// Number of bytes read
		n int
		// Current index into p
		idx int
	)

	rows, err := r.db.Query(`SELECT id, text, closed FROM `+r.table()+` WHERE id > ? and stream = ?`, start, r.stream())
	if err != nil {
		return start, n, err
	}
	defer rows.Close()

	var (
		id     = start
		text   string
		closed bool
	)
	for rows.Next() {
		if err := rows.Scan(&id, &text, &closed); err != nil {
			return id, n, err
		}

		l := len(text)

		copy(p[idx:idx+l], []byte(text))
		n = n + l
		idx += l

		if closed {
			return id, n, io.EOF
		}
	}

	return id, n, nil
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

func (rw *Stream) Close() error {
	_, err := rw.db.Exec(`UPDATE `+rw.table()+` SET closed = 1 WHERE id = (SELECT id FROM logs where stream = ? order by id desc limit 1)`, rw.stream())
	return err
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
