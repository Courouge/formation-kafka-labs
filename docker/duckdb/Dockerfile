# Official DuckDB CLI, pinned. The community davidgasquez/duckdb image is
# stuck on v0.10.3 whose `delta` extension cannot talk to a custom S3
# endpoint (MinIO) — InvalidAccessKeyId / IMDS fallback. A recent CLI
# fixes delta_scan over MinIO via the named S3 SECRET, and ships the
# Local UI (`ui` extension, since v1.2.1).
FROM debian:bookworm-slim

ARG DUCKDB_VERSION=v1.5.2

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl unzip socat \
 && rm -rf /var/lib/apt/lists/* \
 && curl -fsSL -o /tmp/duckdb.zip \
      "https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip" \
 && unzip /tmp/duckdb.zip -d /usr/local/bin \
 && rm /tmp/duckdb.zip \
 && chmod +x /usr/local/bin/duckdb \
 && duckdb -c "INSTALL httpfs; INSTALL delta; INSTALL parquet; INSTALL ui;"

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /workspace
EXPOSE 4213
# Starts the Local UI + socat bridge, stays in foreground.
# CLI access is unaffected:  docker exec -it duckdb duckdb -init /init.sql
ENTRYPOINT ["/entrypoint.sh"]
