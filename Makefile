.PHONY: db cmd

cmd:
	go build -o build/pgstream ./cmd/test

db:
	dropdb pgstream || true
	createdb pgstream
	psql -f schema.sql pgstream
