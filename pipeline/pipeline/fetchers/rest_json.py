from __future__ import annotations
import logging
from typing import Any
import jmespath
import httpx
from pipeline.adapters.models import FetchConfig

logger = logging.getLogger(__name__)


def _ensure_list(val: Any) -> list:
    if val is None:
        return []
    if isinstance(val, list):
        return val
    return [val]


def fetch_index_ids(client: httpx.Client, config: FetchConfig, date_str: str) -> list[str]:
    """Fetch document IDs from the daily index for a given date (YYYYMMDD)."""
    url = config.base_url + config.daily_index_endpoint.format(date=date_str)
    try:
        r = client.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logger.warning("Failed to fetch index for %s: %s", date_str, e)
        return []

    id_path = config.index_id_path
    root = jmespath.search(id_path.root, data) or {}

    # Traverse nested keys; apply _ensure_list at each step to handle
    # the BOE API's inconsistency of returning a dict when there is only one item.
    items: list[Any] = [root]
    for key in id_path.nest:
        next_items: list[Any] = []
        for item in items:
            if isinstance(item, dict):
                next_items.extend(_ensure_list(item.get(key)))
        items = next_items

    return [
        item[id_path.id_field]
        for item in items
        if isinstance(item, dict) and id_path.id_field in item
    ]


def fetch_document_raw(client: httpx.Client, config: FetchConfig, doc_id: str) -> dict | None:
    """Fetch a single document and return its raw API response."""
    url = config.base_url + config.document_endpoint.format(id=doc_id)
    try:
        r = client.get(url, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning("Failed to fetch document %s: %s", doc_id, e)
        return None


def extract_fields(raw: dict, config: FetchConfig) -> dict[str, Any]:
    """Apply all configured JMESPath expressions to the raw API response."""
    return {
        name: jmespath.search(expr, raw)
        for name, expr in config.doc_fields.items()
    }


def extract_relationships(fields: dict[str, Any], config: FetchConfig) -> list[dict[str, str]]:
    """Extract raw relationships from the JMESPath-extracted fields dict."""
    rels: list[dict[str, str]] = []
    for direction_key in [config.refs_before_key, config.refs_after_key]:
        raw_refs = fields.get(direction_key)
        for item in _ensure_list(raw_refs):
            if not isinstance(item, dict):
                continue
            ref = item.get(config.ref_item_key, item) if config.ref_item_key else item
            if not isinstance(ref, dict):
                continue
            source_term = str(ref.get(config.ref_source_term_field, "")).lower().strip()
            target_id = str(ref.get(config.ref_target_id_field, "")).strip()
            if source_term and target_id:
                rels.append({"source_term": source_term, "target_id": target_id})
    return rels
