#!/bin/sh
# Starts the DuckDB Local UI and bridges it to the host.
#
# DuckDB's UI server binds ONLY to the IPv6 loopback [::1]:4213 (no
# bind-address setting exists), so it is unreachable from outside the
# container without a relay. We start the UI on a persistent database
# and use socat to expose it on 0.0.0.0:4213.
#
# Note: the UI *web app* is fetched from ui.duckdb.org (DuckDB forwards
# GET requests there). Your DATA never leaves MinIO/the container, but
# the container needs outbound HTTPS for the UI shell to load.

DB=/data/lake.duckdb
mkdir -p /data

# DuckDB CLI exits after running -c; keep it alive by feeding a stdin
# pipe that never closes, so the UI server thread keeps running.
( echo "CALL start_ui_server();"; tail -f /dev/null ) | duckdb -init /init.sql "$DB" &

# Wait until the UI server actually listens on the IPv6 loopback.
i=0
while [ "$i" -lt 30 ]; do
  if socat -T1 - "TCP6:[::1]:4213" </dev/null >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 1
done

echo "DuckDB UI bridged -> http://localhost:4213/  (DB: $DB)"

# Foreground relay: container stays alive as long as the bridge runs.
exec socat TCP4-LISTEN:4213,fork,reuseaddr "TCP6:[::1]:4213"
