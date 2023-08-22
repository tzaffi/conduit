random := $(shell echo $$RANDOM)
PGCONT = performance_pg
PGUSER = algorand
PGDB = performance_db # postgres
PGLOGS = pg.log
CONDUIT = ../cmd/conduit/conduit
PGCONN = postgresql://$(PGUSER):$(PGUSER)@localhost:65432/$(PGDB)

echo:
	@echo "random--->$(random)"
	@echo "PGCONT--->$(PGCONT)"
	@echo "PGUSER--->$(PGUSER)"
	@echo "PGDB--->$(PGDB)"
	@echo "PGLOGS--->$(PGLOGS)"
	@echo "CONDUIT--->$(CONDUIT)"
	@echo "PGCONN--->$(PGCONN)"

nuke: pg-down clean

nuke-and-run: nuke perf-run

nuke-and-debug: nuke pg-up
	mv ../cmd/conduit/conduit.log "../cmd/conduit/conduit_$(random).log" || true

perf-run: pg-up run-conduit

clean: # pg-down should be called manually
	rm conduit_data/metadata.json || true

clean-go-cache:
	cd .. && go clean -cache -testcache -modcache


pg-up: save-logs
	docker-compose up -d
	sleep 5
	make pg-logs-tail
	docker exec -it $(PGCONT) psql -U algorand -d postgres -c "create database $(PGDB);"
	make pg-query QUERY="-c \"CREATE EXTENSION pg_stat_statements;\""

pg-down:
	docker-compose down

pg-logs-tail:
	docker-compose logs -f > $(PGLOGS) &

save-logs:
	mv $(PGLOGS) "$(PGLOGS)_$(random)" || true
	mv conduit.log "conduit_$(random).log" || true

build:
	cd .. && make

pg-enter:
	docker exec -it $(PGCONT) psql -U algorand -d postgres

run-conduit: build
	$(CONDUIT) -d conduit_data

# - for query hackery... prefer pg_stats.ipynb

QUERY_COL = substring(trim(regexp_replace(regexp_replace(query, '--.*?$$', '', 'gn'), '\\s+', ' ', 'g')), 1, 100) AS query
TOTAL_SECS_COL = round((total_exec_time/1000)::numeric, 3) AS tot_s
MEAN_SECS_COL = round((mean_exec_time/1000)::numeric, 3) AS mean_s
MIN_SECS_COL = round((min_exec_time/1000)::numeric, 3) AS min_s
MAX_SECS_COL = round((max_exec_time/1000)::numeric, 3) AS max_s
CPU_COL = round((100 * total_exec_time / sum(total_exec_time::numeric) OVER ())::numeric, 2) AS \"cpu%\"
LIMIT = 15

define QUERY_TOTAL_TIME
-c "SELECT dbid, $(QUERY_COL), $(TOTAL_SECS_COL), calls, $(MEAN_SECS_COL), $(CPU_COL) \
FROM pg_stat_statements \
ORDER BY total_exec_time DESC \
LIMIT $(LIMIT);"
endef

define QUERY_SLOWEST
-c "SELECT dbid, $(QUERY_COL), calls, $(TOTAL_SECS_COL), $(MIN_SECS_COL), $(MAX_SECS_COL), $(MEAN_SECS_COL) \
FROM pg_stat_statements \
ORDER BY mean_exec_time DESC \
LIMIT $(LIMIT);"
endef

define QUERY_MEMHOG
-c "SELECT dbid, $(QUERY_COL), (shared_blks_hit+shared_blks_dirtied) as mem  \
FROM pg_stat_statements \
ORDER BY (shared_blks_hit+shared_blks_dirtied) DESC \
LIMIT $(LIMIT);"
endef

QUERY := -c "SELECT * FROM pg_stat_statements LIMIT 0;"
pg-query:
	psql $(PGCONN) $(QUERY)


pg-txn-stats: QUERY=-c "SELECT max(round) AS max_round, count(*) AS txn_count from txn;"
pg-txn-stats: pg-query

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

pg-blocking-vac: QUERY=-c "VACUUM FULL ANALYZE;"
pg-blocking-vac: pg-query

pg-vac: QUERY=-c "VACUUM;"
pg-vac: pg-query