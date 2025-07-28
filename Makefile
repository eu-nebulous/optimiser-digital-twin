.DEFAULT_GOAL := help
.PHONY: help
help:				## Output this help message
	@grep -E '^[a-zA-Z_%.-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

config.db: trace.db		## Create dummy config.db from component names in trace.db
	sqlite3 config.db "CREATE TABLE IF NOT EXISTS components (name STRING, replicas INTEGER, cores FLOAT, memory INTEGER)"
	# minimum nebulous node configuration is 3 cores, 2GB memory
	sqlite3 config.db "ATTACH DATABASE 'trace.db' AS source; INSERT INTO components(name, replicas, cores, memory) SELECT local_name, COUNT(DISTINCT local_id), 3, 2048 FROM trace_events GROUP BY local_name UNION SELECT DISTINCT remote_name, 1, 3, 2048 FROM trace_events WHERE remote_name NOT IN (SELECT local_name FROM trace_events);"

.PHONY: clean
clean:
	rm -f config.db
