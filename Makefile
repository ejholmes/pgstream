.PHONY: db

db:
	dropdb pgstream || true
	createdb pgstream
	psql -f schema.sql pgstream
