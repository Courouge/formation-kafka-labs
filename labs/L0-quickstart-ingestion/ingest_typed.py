"""Variante typée du pipeline d'ingestion. Stdlib uniquement (Python 3.10+).

On utilise typing pour documenter les contrats internes et permettre
à mypy / pyright de relever les erreurs avant l'exécution.

Comportement identique à ingest.py — c'est juste la forme qui change.
"""

from __future__ import annotations

import json
import sys
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Literal

REQUIRED_FIELDS: tuple[str, ...] = (
    "order_id", "customer_id", "amount", "currency", "created_at",
)

Status = Literal["OK", "WARN", "EMPTY", "KO"]


@dataclass(frozen=True)
class IngestionResult:
    """Résultat d'un run, prêt à être loggué."""
    batch_id: str
    source: str
    started: str
    finished: str
    n_read: int
    n_valid: int
    n_rejected: int
    n_empty_lines: int
    status: Status

    def to_log_line(self) -> str:
        return json.dumps(asdict(self))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def validate(record: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for f in REQUIRED_FIELDS:
        if f not in record or record[f] is None:
            errors.append(f"missing_or_null_field:{f}")
    amount = record.get("amount")
    if amount is not None and (
        isinstance(amount, bool) or not isinstance(amount, (int, float))
    ):
        errors.append("amount_not_numeric")
    return errors


def iter_lines(path: Path) -> Iterator[tuple[int, str]]:
    """Itère sur (lineno, raw_line) en sautant silencieusement les vides."""
    with path.open(encoding="utf-8", errors="replace") as f:
        for lineno, raw in enumerate(f, 1):
            if raw.strip():
                yield lineno, raw


def compute_status(n_read: int, n_valid: int, n_rejected: int) -> Status:
    if n_read == 0:
        return "EMPTY"
    if n_valid == 0:
        return "WARN"
    if n_rejected / n_read > 0.10:
        return "WARN"
    return "OK"


def run(input_path: Path, bronze_dir: Path, log_path: Path) -> IngestionResult:
    if not input_path.exists():
        raise FileNotFoundError(f"source file not found: {input_path}")

    batch_id = str(uuid.uuid4())
    valid_path = bronze_dir / "valid" / f"{batch_id}.jsonl"
    rejected_path = bronze_dir / "rejected" / f"{batch_id}.jsonl"
    for p in (valid_path.parent, rejected_path.parent, log_path.parent):
        p.mkdir(parents=True, exist_ok=True)

    n_read = n_valid = n_rejected = n_empty = 0
    started = now_iso()

    with valid_path.open("w", encoding="utf-8") as fv, \
         rejected_path.open("w", encoding="utf-8") as fr:

        # On compte les lignes vides en parallèle de l'itération métier
        with input_path.open(encoding="utf-8", errors="replace") as f0:
            for raw in f0:
                if not raw.strip():
                    n_empty += 1

        for lineno, raw in iter_lines(input_path):
            n_read += 1
            common: dict[str, Any] = {
                "_ingestion_timestamp": now_iso(),
                "_batch_id": batch_id,
                "_source_file": str(input_path),
            }
            try:
                rec = json.loads(raw)
            except json.JSONDecodeError as e:
                fr.write(json.dumps({
                    "_lineno": lineno,
                    "_raw": raw.rstrip("\n"),
                    "_errors": [f"json_decode:{e.msg}"],
                    **common,
                }) + "\n")
                n_rejected += 1
                continue

            if not isinstance(rec, dict):
                fr.write(json.dumps({
                    "_lineno": lineno, "_raw": raw.rstrip("\n"),
                    "_errors": ["not_an_object"], **common,
                }) + "\n")
                n_rejected += 1
                continue

            errors = validate(rec)
            enriched = {**rec, **common}
            if errors:
                enriched["_errors"] = errors
                fr.write(json.dumps(enriched) + "\n")
                n_rejected += 1
            else:
                fv.write(json.dumps(enriched) + "\n")
                n_valid += 1

    result = IngestionResult(
        batch_id=batch_id,
        source=str(input_path),
        started=started,
        finished=now_iso(),
        n_read=n_read,
        n_valid=n_valid,
        n_rejected=n_rejected,
        n_empty_lines=n_empty,
        status=compute_status(n_read, n_valid, n_rejected),
    )
    with log_path.open("a", encoding="utf-8") as fl:
        fl.write(result.to_log_line() + "\n")
    print(json.dumps(asdict(result), indent=2))
    return result


if __name__ == "__main__":
    try:
        run(
            Path(sys.argv[1] if len(sys.argv) > 1 else "data/orders.jsonl"),
            Path("bronze"),
            Path("logs/ingest.log"),
        )
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
