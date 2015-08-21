.PHONY: db

db:
	dropdb logstream
	createdb logstream
	psql -f schema.sql logstream
