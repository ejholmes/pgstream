# LogStream

LogStream is a Go package for streaming logs to and from a database.

## Usage

First, create a table with the following schema:

```sql
CREATE TABLE logs (
  id SERIAL,
  stream text,
  text text,
  closed boolean not null default false
);

CREATE INDEX index_stream_on_logs ON logs USING btree (stream);
```

Then initialize a new logstream engine:

```go
rw := logstream.New(db)
rw.Name = "1234"

io.WriteString(rw, "Log line")

io.Copy(os.Stdout, rw)
```
