"""Liste et lit un fichier Parquet écrit par le S3 Sink dans MinIO.

Usage :
    python3 scripts/read-minio.py <bucket> <topic-name> [--rows 20]

Exemples :
    python3 scripts/read-minio.py bronze ecommerce.public.customers
    python3 scripts/read-minio.py bronze ecommerce.public.orders --rows 50

Dépendances :
    pip install boto3 pyarrow

Variables d'environnement :
    MINIO_ENDPOINT      (défaut: http://localhost:9000)
    MINIO_ACCESS_KEY    (défaut: minioadmin)
    MINIO_SECRET_KEY    (défaut: minioadmin)
"""

from __future__ import annotations

import argparse
import io
import os
import sys
from typing import Any

try:
    import boto3
    import pyarrow.parquet as pq
except ImportError as exc:  # pragma: no cover
    print(f"Dépendance manquante : {exc}. Installer via pip install boto3 pyarrow", file=sys.stderr)
    sys.exit(1)


def make_s3_client() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        region_name="us-east-1",
    )


def list_parquet_files(s3: Any, bucket: str, prefix: str) -> list[dict[str, Any]]:
    """Liste tous les Parquet sous le préfixe (paginé)."""
    paginator = s3.get_paginator("list_objects_v2")
    files: list[dict[str, Any]] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                files.append(obj)
    return files


def read_parquet(s3: Any, bucket: str, key: str) -> pq.ParquetFile:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pq.ParquetFile(io.BytesIO(body))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("bucket", help="Bucket MinIO (ex: bronze)")
    parser.add_argument("topic", help="Nom du topic Kafka (ex: ecommerce.public.customers)")
    parser.add_argument("--rows", type=int, default=20, help="Nombre de lignes à afficher (défaut: 20)")
    args = parser.parse_args()

    s3 = make_s3_client()
    prefix = f"topics/{args.topic}/"

    files = list_parquet_files(s3, args.bucket, prefix)
    if not files:
        print(f"Aucun Parquet trouvé sous s3://{args.bucket}/{prefix}")
        print("Vérifier que le S3 sink tourne et que flush.size ou rotate.interval.ms ont été atteints.")
        return 1

    files.sort(key=lambda o: o["LastModified"], reverse=True)
    most_recent = files[0]

    print(f"=> {len(files)} fichier(s) Parquet sous s3://{args.bucket}/{prefix}")
    for f in files[:5]:
        print(f"   {f['LastModified'].isoformat()}  {f['Size']:>10} B  {f['Key']}")

    print()
    print(f"Lecture du plus récent : {most_recent['Key']}")
    pf = read_parquet(s3, args.bucket, most_recent["Key"])
    print(f"Schema :\n{pf.schema_arrow}")
    print(f"Nombre de lignes : {pf.metadata.num_rows}")
    print(f"Nombre de row groups : {pf.metadata.num_row_groups}")
    print()

    table = pf.read()
    df = table.to_pandas() if hasattr(table, "to_pandas") else table
    if hasattr(df, "head"):
        print(df.head(args.rows).to_string(index=False))
    else:
        for batch in table.to_batches(max_chunksize=args.rows):
            print(batch.to_pydict())
            break
    return 0


if __name__ == "__main__":
    sys.exit(main())
