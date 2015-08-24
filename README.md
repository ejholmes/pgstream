# PGStream

PGStream is a Go package that implements the [io.ReadWriter](http://golang.org/pkg/io/#ReadWriter) interface backed by posgres and [database/sql](http://golang.org/pkg/database/sql/).

This allows you to pass around an `io.Reader` or `io.Writer` that can be shared across processes.

## Usage

First, create a table with the following schema:

```sql
CREATE TABLE logs (
  id SERIAL,
  stream text not null,
  text text
);

CREATE INDEX index_stream_on_logs ON logs USING btree (stream);
```

Then initialize a new pgstream engine:

```go
rw := pgstream.New("mylogs", db)

io.WriteString(rw, "Log line")

io.Copy(os.Stdout, rw)
```
