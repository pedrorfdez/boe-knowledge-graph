import os
import pytest
from pathlib import Path
from pydantic import ValidationError

_PROJECT_ROOT = Path(os.environ.get("LEGISLATION_ROOT", Path(__file__).parent.parent.parent.parent))
ADAPTERS_DIR = _PROJECT_ROOT / "adapters"
ONTOLOGY_DIR = _PROJECT_ROOT / "ontology"


def test_load_canonical_returns_expected_keys():
    from pipeline.adapters.loader import load_canonical
    canonical = load_canonical(ONTOLOGY_DIR / "canonical.yml")
    assert "norm_types" in canonical
    assert "relationship_types" in canonical
    assert "statuses" in canonical
    assert "act" in canonical["norm_types"]
    assert "AMENDS" in canonical["relationship_types"]


def test_load_boe_adapter_succeeds():
    from pipeline.adapters.loader import load_adapter
    adapter = load_adapter(ADAPTERS_DIR / "es" / "boe.yml", ONTOLOGY_DIR / "canonical.yml")
    assert adapter.country == "es"
    assert adapter.source == "boe"
    assert adapter.norm_types["Ley"] == "act"
    assert adapter.relationship_types["modifica"] == "AMENDS"
    assert adapter.status_mapping["En vigor"] == "in_force"
    # fetch config
    assert adapter.fetch.type == "rest_json"
    assert adapter.fetch.index_id_path.id_field == "id"
    assert "norm_id" in adapter.fetch.doc_fields
    # parse config
    assert adapter.parse.type == "html"
    assert "disposicion_adicional" in adapter.parse.provision_selectors


def test_adapter_rejects_unknown_norm_type():
    from pipeline.adapters.models import SourceAdapter, FetchConfig, ParseConfig, IndexIdPath
    with pytest.raises(ValidationError, match="Unknown norm_types"):
        SourceAdapter(
            country="es",
            source="test",
            fetch=FetchConfig(
                type="rest_json",
                base_url="http://example.com",
                daily_index_endpoint="/index/{date}",
                document_endpoint="/doc/{id}",
                index_id_path=IndexIdPath(root="data", nest=["items"], id_field="id"),
                doc_fields={"norm_id": "data.id"},
            ),
            parse=ParseConfig(
                type="html",
                article_selector="p.articulo",
                article_title_selector="span.titulo",
                preamble_selectors=["p.preambulo"],
                provision_selectors={},
                annex_selector="p.anexo",
            ),
            norm_type_field="rango",
            status_field="raw_status",
            norm_types={"Ley": "primary_act"},  # invalid — not in canonical
            relationship_types={"modifica": "AMENDS"},
            status_mapping={"En vigor": "in_force"},
            _canonical={
                "norm_types": ["act"],
                "relationship_types": ["AMENDS"],
                "statuses": ["in_force"],
            },
        )


def test_adapter_rejects_unknown_relationship_type():
    from pipeline.adapters.models import SourceAdapter, FetchConfig, ParseConfig, IndexIdPath
    with pytest.raises(ValidationError, match="Unknown relationship_types"):
        SourceAdapter(
            country="es",
            source="test",
            fetch=FetchConfig(
                type="rest_json",
                base_url="http://example.com",
                daily_index_endpoint="/index/{date}",
                document_endpoint="/doc/{id}",
                index_id_path=IndexIdPath(root="data", nest=["items"], id_field="id"),
                doc_fields={"norm_id": "data.id"},
            ),
            parse=ParseConfig(
                type="html",
                article_selector="p.articulo",
                article_title_selector="span.titulo",
                preamble_selectors=["p.preambulo"],
                provision_selectors={},
                annex_selector="p.anexo",
            ),
            norm_type_field="rango",
            status_field="raw_status",
            norm_types={"Ley": "act"},
            relationship_types={"modifica": "CHANGES"},  # invalid
            status_mapping={"En vigor": "in_force"},
            _canonical={
                "norm_types": ["act"],
                "relationship_types": ["AMENDS"],
                "statuses": ["in_force"],
            },
        )


def test_adapter_rejects_unknown_status():
    from pipeline.adapters.models import SourceAdapter, FetchConfig, ParseConfig, IndexIdPath
    with pytest.raises(ValidationError, match="Unknown status_mapping"):
        SourceAdapter(
            country="es",
            source="test",
            fetch=FetchConfig(
                type="rest_json",
                base_url="http://example.com",
                daily_index_endpoint="/index/{date}",
                document_endpoint="/doc/{id}",
                index_id_path=IndexIdPath(root="data", nest=["items"], id_field="id"),
                doc_fields={"norm_id": "data.id"},
            ),
            parse=ParseConfig(
                type="html",
                article_selector="p.articulo",
                article_title_selector="span.titulo",
                preamble_selectors=["p.preambulo"],
                provision_selectors={},
                annex_selector="p.anexo",
            ),
            norm_type_field="rango",
            status_field="raw_status",
            norm_types={"Ley": "act"},
            relationship_types={"modifica": "AMENDS"},
            status_mapping={"En vigor": "active"},  # invalid — not in canonical statuses
            _canonical={
                "norm_types": ["act"],
                "relationship_types": ["AMENDS"],
                "statuses": ["in_force"],
            },
        )


def test_adapter_maps_ley_organica_to_act():
    from pipeline.adapters.loader import load_adapter
    adapter = load_adapter(ADAPTERS_DIR / "es" / "boe.yml", ONTOLOGY_DIR / "canonical.yml")
    assert adapter.norm_types.get("Ley Orgánica") == "act"


def test_adapter_missing_source_term_returns_none():
    from pipeline.adapters.loader import load_adapter
    adapter = load_adapter(ADAPTERS_DIR / "es" / "boe.yml", ONTOLOGY_DIR / "canonical.yml")
    assert adapter.norm_types.get("Providencia") is None
