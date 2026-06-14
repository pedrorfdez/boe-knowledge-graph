import httpx
import pytest
import respx
from pipeline.adapters.models import FetchConfig, IndexIdPath


@pytest.fixture
def fetch_config():
    return FetchConfig(
        type="rest_json",
        base_url="https://www.boe.es/datosabiertos/api",
        daily_index_endpoint="/sumario/{date}",
        document_endpoint="/documento/{id}",
        rate_limit_rps=100.0,
        index_id_path=IndexIdPath(
            root="data.sumario.diario",
            nest=["seccion", "departamento", "epigrafe", "item"],
            id_field="id",
        ),
        doc_fields={
            "norm_id": "data.documento.metadatos.identificador",
            "title": "data.documento.metadatos.titulo",
            "body_html": "data.documento.texto",
            "refs_before": "data.documento.analisis.referencias.anteriores",
            "refs_after": "data.documento.analisis.referencias.posteriores",
        },
    )


@respx.mock
def test_fetch_index_ids_returns_ids(fetch_config, boe_sumario_response):
    respx.get("https://www.boe.es/datosabiertos/api/sumario/20240115").mock(
        return_value=httpx.Response(200, json=boe_sumario_response)
    )
    from pipeline.fetchers.rest_json import fetch_index_ids
    with httpx.Client() as client:
        ids = fetch_index_ids(client, fetch_config, "20240115")
    assert ids == ["BOE-A-2024-999"]


@respx.mock
def test_fetch_index_ids_returns_empty_on_http_error(fetch_config):
    respx.get("https://www.boe.es/datosabiertos/api/sumario/20240115").mock(
        return_value=httpx.Response(404)
    )
    from pipeline.fetchers.rest_json import fetch_index_ids
    with httpx.Client() as client:
        ids = fetch_index_ids(client, fetch_config, "20240115")
    assert ids == []


@respx.mock
def test_fetch_index_ids_handles_single_dict_instead_of_list(fetch_config):
    """BOE API sometimes returns a single dict instead of a list of one item."""
    single_item_response = {
        "data": {
            "sumario": {
                "diario": {
                    "seccion": {  # single dict, not list
                        "nombre": "I. Disposiciones generales",
                        "departamento": {  # single dict, not list
                            "epigrafe": {  # single dict, not list
                                "item": {  # single dict, not list
                                    "id": "BOE-A-2024-001",
                                    "titulo": "Solo item",
                                }
                            }
                        },
                    }
                }
            }
        }
    }
    respx.get("https://www.boe.es/datosabiertos/api/sumario/20240115").mock(
        return_value=httpx.Response(200, json=single_item_response)
    )
    from pipeline.fetchers.rest_json import fetch_index_ids
    with httpx.Client() as client:
        ids = fetch_index_ids(client, fetch_config, "20240115")
    assert ids == ["BOE-A-2024-001"]


@respx.mock
def test_fetch_document_raw_returns_full_response(fetch_config, boe_document_response):
    respx.get("https://www.boe.es/datosabiertos/api/documento/BOE-A-2024-999").mock(
        return_value=httpx.Response(200, json=boe_document_response)
    )
    from pipeline.fetchers.rest_json import fetch_document_raw
    with httpx.Client() as client:
        raw = fetch_document_raw(client, fetch_config, "BOE-A-2024-999")
    assert raw is not None
    assert raw["data"]["documento"]["metadatos"]["identificador"] == "BOE-A-2024-999"


@respx.mock
def test_fetch_document_raw_returns_none_on_error(fetch_config):
    respx.get("https://www.boe.es/datosabiertos/api/documento/INVALID").mock(
        return_value=httpx.Response(500)
    )
    from pipeline.fetchers.rest_json import fetch_document_raw
    with httpx.Client() as client:
        raw = fetch_document_raw(client, fetch_config, "INVALID")
    assert raw is None


def test_extract_relationships_maps_anterior_refs(fetch_config):
    from pipeline.fetchers.rest_json import extract_relationships
    fields = {
        "refs_before": [{"referencia": {"texto": "Modifica", "id": "BOE-A-1980-1000"}}],
        "refs_after": [],
    }
    rels = extract_relationships(fields, fetch_config)
    assert len(rels) == 1
    assert rels[0]["source_term"] == "modifica"  # lowercased
    assert rels[0]["target_id"] == "BOE-A-1980-1000"


def test_extract_relationships_handles_single_dict_ref(fetch_config):
    from pipeline.fetchers.rest_json import extract_relationships
    fields = {
        "refs_before": {"referencia": {"texto": "deroga", "id": "BOE-A-1990-500"}},  # single dict
        "refs_after": None,
    }
    rels = extract_relationships(fields, fetch_config)
    assert len(rels) == 1
    assert rels[0]["source_term"] == "deroga"


def test_extract_relationships_skips_empty_refs(fetch_config):
    from pipeline.fetchers.rest_json import extract_relationships
    fields = {"refs_before": [], "refs_after": []}
    rels = extract_relationships(fields, fetch_config)
    assert rels == []
