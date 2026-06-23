-- DuckDB init script for the Kafka formation
-- Pre-configures S3 access to the local MinIO instance.
-- Loaded automatically when running:  duckdb -init /init.sql

INSTALL httpfs;
LOAD httpfs;

INSTALL delta;
LOAD delta;

INSTALL parquet;
LOAD parquet;

-- Secret used by httpfs (read_parquet, glob, ...)
CREATE OR REPLACE SECRET minio (
    TYPE S3,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    REGION 'us-east-1',
    ENDPOINT 'minio:9000',
    URL_STYLE 'path',
    USE_SSL false
);

-- Legacy global S3 settings: the `delta` extension (delta-kernel FFI)
-- does NOT read the named SECRET above, it reads these variables.
SET s3_endpoint='minio:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_region='us-east-1';
SET s3_use_ssl=false;
SET s3_url_style='path';

.print ''
.print '==================================================================='
.print ' DuckDB ready - MinIO S3 access configured (endpoint: minio:9000)'
.print '==================================================================='
.print ''
.print ' Quick examples:'
.print ''
.print "   SELECT * FROM read_parquet('s3://bronze/**/*.parquet') LIMIT 10;"
.print "   SELECT * FROM delta_scan('s3://bronze/orders') LIMIT 10;"
.print "   SELECT count(*) FROM read_parquet('s3://silver/**/*.parquet');"
.print ''
.print " List buckets via mc CLI:  docker exec mc mc ls local/"
.print ''
.print " Notebook web (Local UI) : http://localhost:4213"
.print ''
