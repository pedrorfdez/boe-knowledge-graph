import io
import json
import logging
import os
import time
from datetime import date as Date
from pathlib import Path

import httpx
from dagster import asset, AssetDep, AssetExecutionContext

from pipeline.adapters.loader import load_adapter
from pipeline.assets.minio_init import legislation_lake_bucket
from pipeline.fetchers.rest_json import fetch_index_ids, fetch_document_raw
from pipeline.partitions import boe_partitions
from pipeline.resources import MinioResource

logger = logging.getLogger(__name__)

_LEGISLATION_ROOT = Path(os.environ.get("LEGISLATION_ROOT", Path(__file__).parent.parent.parent.parent))
_CANONICAL_PATH = _LEGISLATION_ROOT / "ontology" / "canonical.yml"
_ADAPTER_PATHS = {
    "es": _LEGISLATION_ROOT / "adapters" / "es" / "boe.yml",
}


@asset(
    partitions_def=boe_partitions,
    deps=[AssetDep(legislation_lake_bucket)],
    description="Fetch BOE norms for a (country, date) partition and store raw JSON in MinIO Bronze.",
)
def bronze_boe_norms(context: AssetExecutionContext, minio: MinioResource) -> None:
    partition_keys = context.partition_key.keys_by_dimension
    country = partition_keys["country"]
    date_str_iso = partition_keys["date"]
    pub_date = Date.fromisoformat(date_str_iso)
    date_str_src = pub_date.strftime("%Y%m%d")
    year, month, day = pub_date.strftime("%Y"), pub_date.strftime("%m"), pub_date.strftime("%d")

    adapter = load_adapter(_ADAPTER_PATHS[country], _CANONICAL_PATH)
    minio_client = minio.get_client()
    min_delay = 1.0 / adapter.fetch.rate_limit_rps

    stored = 0
    skipped = 0

    with httpx.Client(headers={"Accept": "application/json"}) as http:
        doc_ids = fetch_index_ids(http, adapter.fetch, date_str_src)
        context.log.info(f"Found {len(doc_ids)} documents for {date_str_iso}")

        for doc_id in doc_ids:
            object_name = f"bronze/{country}/{adapter.source}/year={year}/month={month}/day={day}/{doc_id}.json"
            try:
                minio_client.stat_object(minio.bucket, object_name)
                skipped += 1
                continue
            except Exception:
                pass

            time.sleep(min_delay)
            raw = fetch_document_raw(http, adapter.fetch, doc_id)
            if raw is None:
                context.log.warning(f"Failed to fetch {doc_id} — skipping")
                continue

            payload = json.dumps(raw, ensure_ascii=False).encode("utf-8")
            minio_client.put_object(
                bucket_name=minio.bucket,
                object_name=object_name,
                data=io.BytesIO(payload),
                length=len(payload),
                content_type="application/json",
            )
            stored += 1

    context.log.info(f"Bronze: {stored} stored, {skipped} skipped")
    context.add_output_metadata({"stored": stored, "skipped": skipped})
