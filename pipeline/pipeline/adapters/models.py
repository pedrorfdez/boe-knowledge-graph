from __future__ import annotations
from typing import Any, Literal
from pydantic import BaseModel, Field, model_validator


class IndexIdPath(BaseModel):
    root: str
    nest: list[str] = Field(min_length=1)
    id_field: str


class FetchConfig(BaseModel):
    type: Literal["rest_json"]
    base_url: str
    daily_index_endpoint: str
    document_endpoint: str
    rate_limit_rps: float = 2.0
    index_id_path: IndexIdPath
    doc_fields: dict[str, str]
    refs_before_key: str = "refs_before"
    refs_after_key: str = "refs_after"
    ref_item_key: str = "referencia"
    ref_source_term_field: str = "texto"
    ref_target_id_field: str = "id"


class ParseConfig(BaseModel):
    type: Literal["html"]
    article_selector: str
    article_title_selector: str
    preamble_selectors: list[str]
    provision_selectors: dict[str, str]
    annex_selector: str | None = None


class SourceAdapter(BaseModel):
    country: str
    source: str
    fetch: FetchConfig
    parse: ParseConfig
    norm_type_field: str
    status_field: str
    norm_types: dict[str, str]
    relationship_types: dict[str, str]
    status_mapping: dict[str, str]

    @model_validator(mode="before")
    @classmethod
    def validate_against_canonical(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        canonical = values.pop("_canonical", {})
        # Validation is opt-in: load_adapter() always injects _canonical.
        # Direct construction without _canonical (e.g. in unit tests) skips vocab checks.
        if not canonical:
            return values
        for field, canon_key in [
            ("norm_types", "norm_types"),
            ("relationship_types", "relationship_types"),
            ("status_mapping", "statuses"),
        ]:
            mapping = values.get(field, {})
            invalid = set(mapping.values()) - set(canonical.get(canon_key, []))
            if invalid:
                raise ValueError(f"Unknown {field} values not in canonical: {invalid}")
        return values
