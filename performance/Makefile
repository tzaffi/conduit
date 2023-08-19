random := $(shell echo $$RANDOM)
PGCONT = performance_pg
PGUSER = algorand
PGDB = performance_db # postgres
PGCONN = postgresql://algorand:algorand@localhost:65432/performance_db
PGLOGS = pg.log
CONDUIT = ../cmd/conduit/conduit

define QUERY_TOTAL_TIME
-c "SELECT substring(trim(regexp_replace(regexp_replace(query, '--.*?$$', '', 'gn'), '\\s+', ' ', 'g')), 1, 50) as query, \
round(total_exec_time::numeric, 2) AS total_time, \
calls, \
round(mean_exec_time::numeric, 2) AS mean, \
round((100 * total_exec_time / sum(total_exec_time::numeric) OVER ())::numeric, 2) AS percentage_cpu \
FROM pg_stat_statements \
ORDER BY total_exec_time DESC \
LIMIT 10;"
endef

define QUERY_SLOWEST
-c "SELECT userid::regrole, dbid, \
    substring(trim(regexp_replace(regexp_replace(query, '--.*?$$', '', 'gn'), '\\s+', ' ', 'g')), 1, 50) as query, \
    calls, total_exec_time/1000 as total_time_seconds, min_exec_time/1000 as min_time_seconds, \
    max_exec_time/1000 as max_time_seconds, mean_exec_time/1000 as mean_time_seconds \
FROM pg_stat_statements \
ORDER BY mean_exec_time DESC \
LIMIT 10;"
endef

define QUERY_MEMHOG
-c "SELECT userid::regrole, dbid, \
	substring(trim(regexp_replace(regexp_replace(query, '--.*?$$', '', 'gn'), '\\s+', ' ', 'g')), 1, 50) as query, \
    (shared_blks_hit+shared_blks_dirtied) as memory_usage  \
FROM pg_stat_statements \
ORDER BY (shared_blks_hit+shared_blks_dirtied) DESC \
LIMIT 10;"
endef


perf-run: clean pg-up run-conduit

clean: # pg-down should be called manually
	rm conduit_data/metadata.json || true

pg-up: save-logs
	docker-compose up -d
	sleep 5
	make pg-logs-tail
	docker exec -it $(PGCONT) psql -U algorand -d postgres -c "create database $(PGDB);"
	make pg-query QUERY="-c \"CREATE EXTENSION pg_stat_statements;\""

pg-logs-tail:
	docker-compose logs -f > $(PGLOGS) &

save-logs:
	mv $(PGLOGS) "$(PGLOGS)_$(random)" || true
	mv conduit.log "conduit_$(random).log" || true

QUERY := -c "SELECT * FROM pg_stat_statements LIMIT 0;"
pg-query:
	psql $(PGCONN) $(QUERY)

pg-conn: QUERY=
pg-conn: pg-query

pg-stats:
	make pg-stats-1
	make pg-stats-2
	make pg-stats-3

pg-stats-1: QUERY=$(QUERY_TOTAL_TIME)
pg-stats-1: pg-query

pg-stats-2: QUERY=$(QUERY_SLOWEST)
pg-stats-2: pg-query

pg-stats-3: QUERY=$(QUERY_MEMHOG)
pg-stats-3: pg-query


pg-enter:
	docker exec -it $(PGCONT) psql -U algorand -d postgres

pg-down:
	docker-compose down

build:
	cd .. && make

run-conduit: build
	$(CONDUIT) -d conduit_data

run-file-exporter:
	make run-runner TEMPLATE="--template file-exporter" TIMES=1 RESETDB=

clean-reports:
	rm -rf $(REPORTS)

pre-git-push:
	mv _go.mod go.mod
	mv _go.sum go.sum
	cd ../../ && make tidy

post-git-push:
	mv go.mod _go.mod
	mv go.sum _go.sum
	cd ../../ && make tidy && go get github.com/lib/pq
