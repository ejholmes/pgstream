# LogStream

LogStream is a Go package for streaming logs to and from a database.

## Usage

First, create a table with the following schema:

```sql
CREATE TABLE logs (
  id SERIAL,
  stream string,
  text text
);
```

Then initialize a new logstream engine:

```go
engine := logstream.NewEngine(db)
engine.Table = "logs" // Default

stream := engine.Stream("id")
io.WriteString(stream, "Log line")

io.Copy(os.Stdout, stream)
```
