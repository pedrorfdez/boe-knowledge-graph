# Ingestion Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the full BOE data pipeline — Bronze (raw API JSON in MinIO) → Silver (parsed Delta Lake) → Gold (canonical Delta Lake) → Neo4j graph — with the four pre-computed political briefings written to Redis, using a config-driven architecture where adding any future source requires only a new adapter YAML, not new Python files.

**Architecture:** A generic `RestJsonFetcher` (driven by JMESPath field config) and a generic `HtmlParser` (driven by CSS selector config) are each parameterised entirely by `adapters/es/boe.yml`. Dagster assets for Bronze, Silver, and Gold call these generics via `SourceAdapter`; the assets themselves never reference BOE-specific code. Daily schedule triggers incremental ingestion; initial bulk load is a Dagster backfill.

**Tech Stack:** Dagster 1.7, httpx 0.27, jmespath 1.0, Polars 0.20, delta-rs 0.18, PyArrow 16, spaCy 3.7 (es_core_news_sm), BeautifulSoup4, lxml, neo4j 5.20, redis 5.0, MinIO (S3-compatible), Pydantic 2.7, PyYAML 6.0, respx 0.21, pytest 8.0, pytest-asyncio 0.23

**Deferred to Plan 3:** Qdrant embeddings, Elasticsearch indexing, GDS analytics (Louvain/PageRank), EuroVoc/NUTS/Wikidata bootstrap.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `pipeline/pyproject.toml` | Modify | Add beautifulsoup4, lxml, jmespath, pytest, pytest-asyncio, respx |
| `docker-compose.yml` | Modify | Add `./adapters` + `./ontology` bind mounts + `LEGISLATION_ROOT` env var to both Dagster services |
| `pipeline/pipeline/utils.py` | Create | `delta_storage_options()` shared utility |
| `ontology/canonical.yml` | Create | Canonical vocabulary (norm types, rel types, statuses) |
| `adapters/es/boe.yml` | Create | BOE adapter: fetch config + parse config + ontology mapping |
| `pipeline/pipeline/adapters/__init__.py` | Create | Package marker |
| `pipeline/pipeline/adapters/models.py` | Create | `IndexIdPath`, `FetchConfig`, `ParseConfig`, `SourceAdapter` Pydantic models |
| `pipeline/pipeline/adapters/loader.py` | Create | Load + validate adapter YAML against canonical |
| `pipeline/pipeline/fetchers/__init__.py` | Create | Package marker |
| `pipeline/pipeline/fetchers/rest_json.py` | Create | Generic REST JSON fetcher + relationship extractor driven by `FetchConfig` |
| `pipeline/pipeline/parsers/__init__.py` | Create | Package marker |
| `pipeline/pipeline/parsers/html_parser.py` | Create | Generic HTML parser driven by `ParseConfig` (CSS selectors) |
| `pipeline/pipeline/partitions.py` | Create | `MultiPartitionsDefinition` for country × date |
| `pipeline/pipeline/schedules.py` | Create | Daily BOE schedule at 23:30 UTC |
| `pipeline/pipeline/assets/__init__.py` | Create | Package marker |
| `pipeline/pipeline/assets/minio_init.py` | Create | MinIO bucket creation asset |
| `pipeline/pipeline/assets/bronze.py` | Create | Generic Bronze fetch asset — loads adapter, calls `RestJsonFetcher` |
| `pipeline/pipeline/assets/silver.py` | Create | Generic Silver transform asset — JMESPath extract + HTML parse + NER → Delta Lake |
| `pipeline/pipeline/assets/gold.py` | Create | Gold transform asset (ontology mapping → Delta Lake) + asset check |
| `pipeline/pipeline/assets/neo4j_nodes.py` | Create | MERGE Norm nodes into Neo4j |
| `pipeline/pipeline/assets/neo4j_edges.py` | Create | MERGE relationships into Neo4j |
| `pipeline/pipeline/assets/briefings.py` | Create | 4 Cypher queries → Redis |
| `pipeline/pipeline/__init__.py` | Modify | Wire all assets, resources, schedule into `Definitions` |
| `pipeline/tests/__init__.py` | Create | Package marker |
| `pipeline/tests/pipeline/__init__.py` | Create | Package marker |
| `pipeline/tests/pipeline/conftest.py` | Create | Sample BOE API fixtures |
| `pipeline/tests/pipeline/test_adapter_models.py` | Create | Pydantic adapter validation unit tests |
| `pipeline/tests/pipeline/test_rest_json_fetcher.py` | Create | Fetcher unit tests with respx HTTP mocking |
| `pipeline/tests/pipeline/test_html_parser.py` | Create | HTML parser unit tests |
| `pipeline/tests/pipeline/test_gold_transform.py` | Create | Gold ontology mapping unit tests |

> **Docker note:** `tests/` lives inside `pipeline/` so it is covered by the existing `./pipeline:/opt/dagster/app` bind mount. `adapters/` and `ontology/` are mounted at `/legislation/adapters` and `/legislation/ontology` via the bind mounts added in Task 1; `LEGISLATION_ROOT=/legislation` is the env var the Python code reads.

---

## Task 1: Add dependencies, test infrastructure, and Docker bind mounts

**Files:**
- Modify: `pipeline/pyproject.toml`
- Modify: `docker-compose.yml`
- Create: `pipeline/pipeline/utils.py`
- Create: `pipeline/tests/__init__.py`
- Create: `pipeline/tests/pipeline/__init__.py`
- Create: `pipeline/tests/pipeline/conftest.py`

- [ ] **Step 1: Replace `pipeline/pyproject.toml` with the following**

```toml
[build-system]
requires = ["setuptools>=42"]
build-backend = "setuptools.build_meta"

[project]
name = "pipeline"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "dagster>=1.7",
    "dagster-webserver>=1.7",
    "dagster-postgres>=0.23",
    "neo4j>=5.20",
    "minio>=7.2",
    "deltalake>=0.18",
    "polars>=0.20",
    "pyarrow>=16",
    "qdrant-client>=1.9",
    "elasticsearch>=8.13",
    "redis>=5.0",
    "pydantic>=2.7",
    "httpx>=0.27",
    "spacy>=3.7",
    "PyYAML>=6.0",
    "jmespath>=1.0",
    "beautifulsoup4>=4.12",
    "lxml>=5.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "respx>=0.21",
]

[tool.setuptools.packages.find]
where = ["."]
include = ["pipeline*"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

- [ ] **Step 2: Rebuild the pipeline Docker image**

```bash
docker compose build pipeline --no-cache
```

Expected: build completes (spaCy model download takes ~2 minutes on first run).

- [ ] **Step 3: Add adapter/ontology bind mounts to `docker-compose.yml`**

In both the `dagster-webserver` and `dagster-daemon` service definitions, under `volumes:` add:
```yaml
      - ./adapters:/legislation/adapters
      - ./ontology:/legislation/ontology
```

Under `environment:` add:
```yaml
      LEGISLATION_ROOT: /legislation
```

Then restart:
```bash
docker compose up -d dagster-webserver dagster-daemon
```

- [ ] **Step 4: Create `pipeline/pipeline/utils.py`**

```python
from pipeline.resources import MinioResource


def delta_storage_options(minio: MinioResource) -> dict[str, str]:
    return {
        "AWS_ENDPOINT_URL": minio.endpoint,
        "AWS_ACCESS_KEY_ID": minio.access_key,
        "AWS_SECRET_ACCESS_KEY": minio.secret_key,
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
```

- [ ] **Step 5: Create `pipeline/tests/__init__.py` and `pipeline/tests/pipeline/__init__.py`**

Both files are empty.

- [ ] **Step 6: Create `pipeline/tests/pipeline/conftest.py`**

```python
import os
import pytest
from pathlib import Path

_PROJECT_ROOT = Path(os.environ.get("LEGISLATION_ROOT", Path(__file__).parent.parent.parent.parent))
ADAPTERS_DIR = _PROJECT_ROOT / "adapters"
ONTOLOGY_DIR = _PROJECT_ROOT / "ontology"

BOE_SUMARIO_RESPONSE = {
    "data": {
        "sumario": {
            "diario": {
                "numero": "13",
                "fecha": "20240115",
                "seccion": [
                    {
                        "nombre": "I. Disposiciones generales",
                        "departamento": [
                            {
                                "nombre": "MINISTERIO DE TRABAJO",
                                "epigrafe": [
                                    {
                                        "item": [
                                            {
                                                "id": "BOE-A-2024-999",
                                                "titulo": "Ley de prueba",
                                            }
                                        ]
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        }
    }
}

BOE_DOCUMENT_RESPONSE = {
    "data": {
        "documento": {
            "metadatos": {
                "identificador": "BOE-A-2024-999",
                "titulo": "Ley de prueba",
                "fecha_publicacion": "20240115",
                "departamento": "MINISTERIO DE TRABAJO",
                "rango": "Ley",
                "estatus_derogacion": "En vigor",
            },
            "analisis": {
                "referencias": {
                    "anteriores": [
                        {
                            "referencia": {
                                "texto": "modifica",
                                "id": "BOE-A-1980-1000",
                            }
                        }
                    ],
                    "posteriores": [],
                },
            },
            "texto": """<html><body>
<p class="parrafo_1">Preámbulo: Esta ley tiene por objeto regular el trabajo.</p>
<p class="articulo"><span class="titulo_articulo">Artículo 1.</span> Objeto.</p>
<p class="parrafo">La presente ley regula las condiciones de trabajo.</p>
<p class="articulo"><span class="titulo_articulo">Artículo 2.</span> Ámbito de aplicación.</p>
<p class="parrafo">Se aplica a todos los trabajadores por cuenta ajena.</p>
<p class="disposicion_adicional"><span>Disposición adicional primera.</span> Texto adicional.</p>
</body></html>""",
        }
    }
}


@pytest.fixture
def boe_sumario_response():
    return BOE_SUMARIO_RESPONSE


@pytest.fixture
def boe_document_response():
    return BOE_DOCUMENT_RESPONSE
```

- [ ] **Step 7: Verify pytest discovers tests**

```bash
docker compose exec dagster-webserver python -m pytest tests/ --collect-only 2>&1 | head -20
```

Expected: pytest finds `tests/pipeline/` (currently 0 test files — that's fine).

- [ ] **Step 8: Commit**

```
feat(pipeline): add deps, utils, test infra, project root bind mount
```
Stage: `pipeline/pyproject.toml`, `pipeline/pipeline/utils.py`, `pipeline/tests/`, `docker-compose.yml`

---

## Task 2: Canonical vocabulary and adapter model framework

**Files:**
- Create: `ontology/canonical.yml`
- Create: `adapters/es/boe.yml`
- Create: `pipeline/pipeline/adapters/__init__.py`
- Create: `pipeline/pipeline/adapters/models.py`
- Create: `pipeline/pipeline/adapters/loader.py`
- Create: `pipeline/tests/pipeline/test_adapter_models.py`

- [ ] **Step 1: Create `ontology/canonical.yml`**

```yaml
norm_types:
  - act
  - bill
  - amendment
  - regulation
  - order
  - resolution
  - decree
  - decision

relationship_types:
  - AMENDS
  - REPEALS
  - CITES
  - IMPLEMENTS

statuses:
  - in_force
  - repealed
  - partially_repealed
```

- [ ] **Step 2: Create `adapters/es/boe.yml`**

```yaml
country: es
source: boe

fetch:
  type: rest_json
  base_url: "https://www.boe.es/datosabiertos/api"
  daily_index_endpoint: "/sumario/{date}"
  document_endpoint: "/documento/{id}"
  rate_limit_rps: 2.0
  index_id_path:
    root: "data.sumario.diario"
    nest:
      - "seccion"
      - "departamento"
      - "epigrafe"
      - "item"
    id_field: "id"
  doc_fields:
    norm_id:      "data.documento.metadatos.identificador"
    title:        "data.documento.metadatos.titulo"
    date_pub:     "data.documento.metadatos.fecha_publicacion"
    raw_status:   "data.documento.metadatos.estatus_derogacion"
    rango:        "data.documento.metadatos.rango"
    departamento: "data.documento.metadatos.departamento"
    body_html:    "data.documento.texto"
    refs_before:  "data.documento.analisis.referencias.anteriores"
    refs_after:   "data.documento.analisis.referencias.posteriores"
  refs_before_key: "refs_before"
  refs_after_key: "refs_after"
  ref_item_key: "referencia"
  ref_source_term_field: "texto"
  ref_target_id_field: "id"

parse:
  type: html
  article_selector: "p.articulo"
  article_title_selector: "span.titulo_articulo"
  preamble_selectors:
    - "p.parrafo_1"
    - "p.preambulo"
    - "p.exposicion_motivos"
  provision_selectors:
    disposicion_adicional: "additional"
    disposicion_transitoria: "transitional"
    disposicion_derogatoria: "repealing"
    disposicion_final: "final"
  annex_selector: "p.anexo"

norm_type_field: "rango"
status_field: "raw_status"

norm_types:
  "Ley":                      "act"
  "Ley Orgánica":             "act"
  "Real Decreto Legislativo": "act"
  "Real Decreto-ley":         "regulation"
  "Real Decreto":             "regulation"
  "Orden":                    "order"
  "Orden Ministerial":        "order"
  "Resolución":               "resolution"
  "Decreto":                  "decree"
  "Decisión":                 "decision"

relationship_types:
  "modifica":           "AMENDS"
  "queda modificado":   "AMENDS"
  "deroga":             "REPEALS"
  "queda derogado":     "REPEALS"
  "cita":               "CITES"
  "hace referencia":    "CITES"
  "desarrolla":         "IMPLEMENTS"
  "transpone":          "IMPLEMENTS"
  "ejecuta":            "IMPLEMENTS"

status_mapping:
  "En vigor":                   "in_force"
  "Vigente":                    "in_force"
  "Vigente con modificaciones": "in_force"
  "Derogado":                   "repealed"
  "Derogada":                   "repealed"
  "Derogado parcialmente":      "partially_repealed"
  "Derogada parcialmente":      "partially_repealed"
```

- [ ] **Step 3: Write failing tests in `pipeline/tests/pipeline/test_adapter_models.py`**

```python
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


def test_adapter_maps_ley_organica_to_act():
    from pipeline.adapters.loader import load_adapter
    adapter = load_adapter(ADAPTERS_DIR / "es" / "boe.yml", ONTOLOGY_DIR / "canonical.yml")
    assert adapter.norm_types.get("Ley Orgánica") == "act"


def test_adapter_missing_source_term_returns_none():
    from pipeline.adapters.loader import load_adapter
    adapter = load_adapter(ADAPTERS_DIR / "es" / "boe.yml", ONTOLOGY_DIR / "canonical.yml")
    assert adapter.norm_types.get("Providencia") is None
```

- [ ] **Step 4: Run tests — expect FAIL**

```bash
docker compose exec dagster-webserver python -m pytest tests/pipeline/test_adapter_models.py -v 2>&1 | head -20
```

Expected: `ModuleNotFoundError: No module named 'pipeline.adapters'`

- [ ] **Step 5: Create `pipeline/pipeline/adapters/__init__.py`**

Empty file.

- [ ] **Step 6: Create `pipeline/pipeline/adapters/models.py`**

```python
from __future__ import annotations
from typing import Any, Literal
from pydantic import BaseModel, model_validator


class IndexIdPath(BaseModel):
    root: str
    nest: list[str]
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
    annex_selector: str


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

    model_config = {"arbitrary_types_allowed": True}

    @model_validator(mode="before")
    @classmethod
    def validate_against_canonical(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        canonical = values.pop("_canonical", {})
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
```

- [ ] **Step 7: Create `pipeline/pipeline/adapters/loader.py`**

```python
from pathlib import Path
import yaml
from pipeline.adapters.models import SourceAdapter


def load_canonical(canonical_path: Path) -> dict[str, list[str]]:
    with open(canonical_path) as f:
        return yaml.safe_load(f)


def load_adapter(adapter_path: Path, canonical_path: Path) -> SourceAdapter:
    canonical = load_canonical(canonical_path)
    with open(adapter_path) as f:
        data = yaml.safe_load(f)
    data["_canonical"] = canonical
    return SourceAdapter(**data)
```

- [ ] **Step 8: Run tests — expect PASS**

```bash
docker compose exec dagster-webserver python -m pytest tests/pipeline/test_adapter_models.py -v
```

Expected: `6 passed`

- [ ] **Step 9: Commit**

```
feat(pipeline): canonical vocab, BOE adapter YAML with fetch/parse config, Pydantic models
```
Stage: `ontology/`, `adapters/`, `pipeline/pipeline/adapters/`, `pipeline/tests/pipeline/test_adapter_models.py`

---

## Task 3: Generic REST JSON fetcher (TDD)

**Files:**
- Create: `pipeline/pipeline/fetchers/__init__.py`
- Create: `pipeline/pipeline/fetchers/rest_json.py`
- Create: `pipeline/tests/pipeline/test_rest_json_fetcher.py`

- [ ] **Step 1: Write failing tests in `pipeline/tests/pipeline/test_rest_json_fetcher.py`**

```python
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
```

- [ ] **Step 2: Run tests — expect FAIL**

```bash
docker compose exec dagster-webserver python -m pytest tests/pipeline/test_rest_json_fetcher.py -v 2>&1 | head -15
```

Expected: `ModuleNotFoundError: No module named 'pipeline.fetchers'`

- [ ] **Step 3: Create `pipeline/pipeline/fetchers/__init__.py`**

Empty file.

- [ ] **Step 4: Create `pipeline/pipeline/fetchers/rest_json.py`**

```python
from __future__ import annotations
from typing import Any
import jmespath
import httpx
from pipeline.adapters.models import FetchConfig


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
    except Exception:
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
    except Exception:
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
```

- [ ] **Step 5: Run tests — expect PASS**

```bash
docker compose exec dagster-webserver python -m pytest tests/pipeline/test_rest_json_fetcher.py -v
```

Expected: `8 passed`

- [ ] **Step 6: Commit**

```
feat(pipeline): generic REST JSON fetcher with JMESPath field extraction
```
Stage: `pipeline/pipeline/fetchers/`, `pipeline/tests/pipeline/test_rest_json_fetcher.py`

---

## Task 4: Generic HTML parser (TDD)

**Files:**
- Create: `pipeline/pipeline/parsers/__init__.py`
- Create: `pipeline/pipeline/parsers/html_parser.py`
- Create: `pipeline/tests/pipeline/test_html_parser.py`

- [ ] **Step 1: Write failing tests in `pipeline/tests/pipeline/test_html_parser.py`**

```python
import pytest
from pipeline.adapters.models import ParseConfig


SAMPLE_HTML = """<html><body>
<p class="parrafo_1">Preámbulo de la ley.</p>
<p class="articulo"><span class="titulo_articulo">Artículo 1.</span> Objeto.</p>
<p class="parrafo">Esta ley regula el trabajo a distancia.</p>
<p class="articulo"><span class="titulo_articulo">Artículo 2.</span> Ámbito.</p>
<p class="parrafo">Se aplica a todos los trabajadores.</p>
<p class="disposicion_adicional"><span>Disposición adicional primera.</span> Texto adicional aquí.</p>
<p class="disposicion_final"><span>Disposición final primera.</span> Entrada en vigor.</p>
</body></html>"""


@pytest.fixture
def boe_parse_config():
    return ParseConfig(
        type="html",
        article_selector="p.articulo",
        article_title_selector="span.titulo_articulo",
        preamble_selectors=["p.parrafo_1", "p.preambulo", "p.exposicion_motivos"],
        provision_selectors={
            "disposicion_adicional": "additional",
            "disposicion_transitoria": "transitional",
            "disposicion_derogatoria": "repealing",
            "disposicion_final": "final",
        },
        annex_selector="p.anexo",
    )


def test_parse_extracts_preamble(boe_parse_config):
    from pipeline.parsers.html_parser import parse_html
    result = parse_html(SAMPLE_HTML, boe_parse_config)
    assert result["preamble_text"] is not None
    assert "Preámbulo" in result["preamble_text"]


def test_parse_extracts_articles(boe_parse_config):
    from pipeline.parsers.html_parser import parse_html
    result = parse_html(SAMPLE_HTML, boe_parse_config)
    assert len(result["articles"]) == 2
    assert result["articles"][0]["article_num"] == "Artículo 1"
    assert "trabajo a distancia" in result["articles"][0]["text"]
    assert result["articles"][1]["article_num"] == "Artículo 2"


def test_parse_articles_have_required_fields(boe_parse_config):
    from pipeline.parsers.html_parser import parse_html
    result = parse_html(SAMPLE_HTML, boe_parse_config)
    for article in result["articles"]:
        assert "article_id" in article
        assert "article_num" in article
        assert "text" in article


def test_parse_extracts_provisions(boe_parse_config):
    from pipeline.parsers.html_parser import parse_html
    result = parse_html(SAMPLE_HTML, boe_parse_config)
    provisions = result["provisions"]
    assert len(provisions) == 2
    additional = [p for p in provisions if p["type"] == "additional"]
    assert len(additional) == 1
    final = [p for p in provisions if p["type"] == "final"]
    assert len(final) == 1


def test_parse_empty_html_returns_empty_structure(boe_parse_config):
    from pipeline.parsers.html_parser import parse_html
    result = parse_html("<html><body></body></html>", boe_parse_config)
    assert result["preamble_text"] is None
    assert result["articles"] == []
    assert result["provisions"] == []
    assert result["annexes"] == []
```

- [ ] **Step 2: Run tests — expect FAIL**

```bash
docker compose exec dagster-webserver python -m pytest tests/pipeline/test_html_parser.py -v 2>&1 | head -10
```

Expected: `ModuleNotFoundError: No module named 'pipeline.parsers'`

- [ ] **Step 3: Create `pipeline/pipeline/parsers/__init__.py`**

Empty file.

- [ ] **Step 4: Create `pipeline/pipeline/parsers/html_parser.py`**

```python
from __future__ import annotations
from typing import Any
from bs4 import BeautifulSoup
from pipeline.adapters.models import ParseConfig


def parse_html(html: str, config: ParseConfig) -> dict[str, Any]:
    """Parse BOE HTML using CSS selectors configured in ParseConfig."""
    soup = BeautifulSoup(html or "", "lxml")

    # Articles
    articles: list[dict] = []
    for i, tag in enumerate(soup.select(config.article_selector), 1):
        span = tag.select_one(config.article_title_selector)
        if span:
            article_num = span.get_text(strip=True).rstrip(".")
            articles.append({
                "article_id": f"art-{i}",
                "article_num": article_num,
                "text": tag.get_text(separator=" ", strip=True),
            })

    # Provisions — grouped by type, numbered per type
    provisions: list[dict] = []
    type_counters: dict[str, int] = {}
    for css_class, prov_type in config.provision_selectors.items():
        for tag in soup.select(f".{css_class}"):
            type_counters[prov_type] = type_counters.get(prov_type, 0) + 1
            provisions.append({
                "type": prov_type,
                "num": str(type_counters[prov_type]),
                "text": tag.get_text(separator=" ", strip=True),
            })

    # Annexes
    annexes: list[dict] = [
        {
            "annex_id": f"annex-{i}",
            "title": tag.get_text(strip=True)[:100],
            "text": tag.get_text(separator=" ", strip=True),
        }
        for i, tag in enumerate(soup.select(config.annex_selector), 1)
    ]

    # Preamble — text from configured selectors
    preamble_parts = [
        tag.get_text(separator=" ", strip=True)
        for sel in config.preamble_selectors
        for tag in soup.select(sel)
        if tag.get_text(strip=True)
    ]
    preamble_text = " ".join(preamble_parts) if preamble_parts else None

    return {
        "preamble_text": preamble_text,
        "articles": articles,
        "provisions": provisions,
        "annexes": annexes,
    }
```

- [ ] **Step 5: Run tests — expect PASS**

```bash
docker compose exec dagster-webserver python -m pytest tests/pipeline/test_html_parser.py -v
```

Expected: `5 passed`

- [ ] **Step 6: Commit**

```
feat(pipeline): generic HTML parser driven by CSS selector config
```
Stage: `pipeline/pipeline/parsers/`, `pipeline/tests/pipeline/test_html_parser.py`

---

## Task 5: Partition definition, MinIO init, and Bronze fetch asset

**Files:**
- Create: `pipeline/pipeline/partitions.py`
- Create: `pipeline/pipeline/assets/__init__.py`
- Create: `pipeline/pipeline/assets/minio_init.py`
- Create: `pipeline/pipeline/assets/bronze.py`
- Modify: `pipeline/pipeline/__init__.py`

- [ ] **Step 1: Create `pipeline/pipeline/partitions.py`**

```python
from dagster import MultiPartitionsDefinition, StaticPartitionsDefinition, DailyPartitionsDefinition

boe_partitions = MultiPartitionsDefinition(
    {
        "country": StaticPartitionsDefinition(["es"]),
        "date": DailyPartitionsDefinition(start_date="2010-01-01"),
    }
)
```

- [ ] **Step 2: Verify partition key format**

```bash
docker compose exec dagster-webserver python -c "
from pipeline.partitions import boe_partitions
keys = boe_partitions.get_partition_keys()
print(f'Total: {len(keys)}')
print(f'First: {keys[0]}')
print(f'Last: {keys[-1]}')
"
```

Expected output (approximate):
```
Total: 5000+
First: country=es|date=2010-01-01
Last: country=es|date=2026-06-14
```

- [ ] **Step 3: Create `pipeline/pipeline/assets/__init__.py`**

Empty file.

- [ ] **Step 4: Create `pipeline/pipeline/assets/minio_init.py`**

```python
from dagster import asset, AssetExecutionContext
from pipeline.resources import MinioResource


@asset(description="Creates the legislation-lake MinIO bucket if it does not exist.")
def legislation_lake_bucket(context: AssetExecutionContext, minio: MinioResource) -> None:
    client = minio.get_client()
    if not client.bucket_exists(minio.bucket):
        client.make_bucket(minio.bucket)
        context.log.info(f"Created bucket: {minio.bucket}")
    else:
        context.log.info(f"Bucket already exists: {minio.bucket}")
```

- [ ] **Step 5: Create `pipeline/pipeline/assets/bronze.py`**

```python
import io
import json
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

_PROJECT_ROOT = Path(os.environ.get("LEGISLATION_ROOT", Path(__file__).parent.parent.parent.parent))
_CANONICAL_PATH = _PROJECT_ROOT / "ontology" / "canonical.yml"
_ADAPTER_PATHS = {
    "es": _PROJECT_ROOT / "adapters" / "es" / "boe.yml",
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
```

- [ ] **Step 6: Wire into `pipeline/pipeline/__init__.py`**

```python
from dagster import Definitions, EnvVar
from pipeline.resources import (
    Neo4jResource, MinioResource, QdrantResource, ElasticsearchResource, RedisResource,
)
from pipeline.assets.minio_init import legislation_lake_bucket
from pipeline.assets.bronze import bronze_boe_norms

resources = {
    "neo4j": Neo4jResource(uri=EnvVar("NEO4J_URI"), password=EnvVar("NEO4J_PASSWORD")),
    "minio": MinioResource(
        endpoint=EnvVar("MINIO_ENDPOINT"),
        access_key=EnvVar("MINIO_ROOT_USER"),
        secret_key=EnvVar("MINIO_ROOT_PASSWORD"),
        bucket=EnvVar("MINIO_BUCKET"),
    ),
    "qdrant": QdrantResource(url=EnvVar("QDRANT_URL")),
    "elasticsearch": ElasticsearchResource(url=EnvVar("ELASTICSEARCH_URL")),
    "redis": RedisResource(url=EnvVar("REDIS_URL")),
}

defs = Definitions(assets=[legislation_lake_bucket, bronze_boe_norms], resources=resources)
```

- [ ] **Step 7: Materialise bucket and a single Bronze partition**

Open `http://localhost:3000`. Materialise `legislation_lake_bucket`.

Then materialise `bronze_boe_norms` → partition `country=es|date=2024-01-15`.

Expected: Dagster logs "Found N documents for 2024-01-15". MinIO console (`http://localhost:9001`) → `legislation-lake` → `bronze/es/boe/year=2024/month=01/day=15/` → JSON files appear.

If zero documents found: the sumario API returned an unexpected structure. Run the exploration query:
```bash
docker compose exec dagster-webserver python -c "
import httpx, json
r = httpx.get('https://www.boe.es/datosabiertos/api/sumario/20240115', timeout=30)
print(r.status_code)
d = r.json()
print(json.dumps(d, indent=2, ensure_ascii=False)[:1000])
"
```
Compare the actual `seccion`/`departamento`/`epigrafe`/`item` nesting to the `index_id_path.nest` config in `adapters/es/boe.yml` and adjust.

- [ ] **Step 8: Commit**

```
feat(pipeline): partition def, MinIO init, Bronze fetch asset (config-driven)
```
Stage: `pipeline/pipeline/partitions.py`, `pipeline/pipeline/assets/`, `pipeline/pipeline/__init__.py`

---

## Task 6: Silver transform asset

**Files:**
- Create: `pipeline/pipeline/assets/silver.py`
- Modify: `pipeline/pipeline/__init__.py`

- [ ] **Step 1: Create `pipeline/pipeline/assets/silver.py`**

```python
import json
import os
from datetime import date as Date
from pathlib import Path
from typing import Any

import polars as pl
import spacy
from dagster import asset, AssetExecutionContext
from deltalake import write_deltalake

from pipeline.adapters.loader import load_adapter
from pipeline.assets.bronze import bronze_boe_norms
from pipeline.fetchers.rest_json import extract_fields, extract_relationships
from pipeline.parsers.html_parser import parse_html
from pipeline.partitions import boe_partitions
from pipeline.resources import MinioResource
from pipeline.utils import delta_storage_options

_PROJECT_ROOT = Path(os.environ.get("LEGISLATION_ROOT", Path(__file__).parent.parent.parent.parent))
_CANONICAL_PATH = _PROJECT_ROOT / "ontology" / "canonical.yml"
_ADAPTER_PATHS = {
    "es": _PROJECT_ROOT / "adapters" / "es" / "boe.yml",
}

_nlp = None


def _get_nlp():
    global _nlp
    if _nlp is None:
        _nlp = spacy.load("es_core_news_sm")
    return _nlp


def _run_ner(text: str) -> list[dict[str, str]]:
    if not text:
        return []
    nlp = _get_nlp()
    doc = nlp(text[:50_000])
    label_map = {"ORG": "ministry", "GPE": "territory", "LOC": "territory", "PER": "person"}
    return [
        {"entity_type": label_map.get(ent.label_, "other"), "value": ent.text, "wikidata_id": None}
        for ent in doc.ents
        if ent.label_ in label_map
    ]


@asset(
    partitions_def=boe_partitions,
    deps=[bronze_boe_norms],
    description="Parse Bronze JSON into structured Silver Delta Lake table.",
)
def silver_boe_norms(context: AssetExecutionContext, minio: MinioResource) -> None:
    partition_keys = context.partition_key.keys_by_dimension
    country = partition_keys["country"]
    date_str_iso = partition_keys["date"]
    pub_date = Date.fromisoformat(date_str_iso)
    year, month, day = pub_date.strftime("%Y"), pub_date.strftime("%m"), pub_date.strftime("%d")

    adapter = load_adapter(_ADAPTER_PATHS[country], _CANONICAL_PATH)
    minio_client = minio.get_client()
    storage_opts = delta_storage_options(minio)

    prefix = f"bronze/{country}/{adapter.source}/year={year}/month={month}/day={day}/"
    objects = list(minio_client.list_objects(minio.bucket, prefix=prefix))

    if not objects:
        context.log.info(f"No Bronze objects for {date_str_iso} — skipping Silver")
        return

    rows: list[dict] = []
    silver_path = f"s3://{minio.bucket}/silver/{country}/{adapter.source}/year={year}/month={month}/day={day}/"

    for obj in objects:
        raw_bytes = minio_client.get_object(minio.bucket, obj.object_name).read()
        raw = json.loads(raw_bytes)

        fields = extract_fields(raw, adapter.fetch)
        norm_id = (fields.get("norm_id") or "").strip()
        if not norm_id:
            context.log.warning(f"Missing norm_id in {obj.object_name} — skipping")
            continue

        parsed = parse_html(fields.get("body_html") or "", adapter.parse)
        raw_rels = extract_relationships(fields, adapter.fetch)

        sample_text = " ".join(filter(None, [
            parsed.get("preamble_text") or "",
            *[a["text"] for a in parsed.get("articles", [])[:3]],
        ]))
        ner_entities = _run_ner(sample_text)

        rows.append({
            "norm_id": norm_id,
            "title": fields.get("title") or "",
            "country": country,
            "source": adapter.source,
            "date_published": date_str_iso,
            "preamble_text": parsed.get("preamble_text"),
            "articles": json.dumps(parsed.get("articles", []), ensure_ascii=False),
            "provisions": json.dumps(parsed.get("provisions", []), ensure_ascii=False),
            "annexes": json.dumps(parsed.get("annexes", []), ensure_ascii=False),
            "raw_relationships": json.dumps(raw_rels, ensure_ascii=False),
            "raw_status": fields.get(adapter.status_field) or "",
            "ner_entities": json.dumps(ner_entities, ensure_ascii=False),
            "storage_key": silver_path,
            "source_norm_type_raw": fields.get(adapter.norm_type_field) or "",
        })

    if not rows:
        context.log.info("No valid documents to write")
        return

    df = pl.DataFrame({col: [r[col] for r in rows] for col in rows[0].keys()})
    write_deltalake(silver_path, df.to_arrow(), mode="overwrite", storage_options=storage_opts)

    context.log.info(f"Silver: wrote {len(rows)} norms to {silver_path}")
    context.add_output_metadata({"norms_written": len(rows), "path": silver_path})
```

- [ ] **Step 2: Update `pipeline/pipeline/__init__.py`**

```python
from dagster import Definitions, EnvVar
from pipeline.resources import (
    Neo4jResource, MinioResource, QdrantResource, ElasticsearchResource, RedisResource,
)
from pipeline.assets.minio_init import legislation_lake_bucket
from pipeline.assets.bronze import bronze_boe_norms
from pipeline.assets.silver import silver_boe_norms

resources = {
    "neo4j": Neo4jResource(uri=EnvVar("NEO4J_URI"), password=EnvVar("NEO4J_PASSWORD")),
    "minio": MinioResource(
        endpoint=EnvVar("MINIO_ENDPOINT"),
        access_key=EnvVar("MINIO_ROOT_USER"),
        secret_key=EnvVar("MINIO_ROOT_PASSWORD"),
        bucket=EnvVar("MINIO_BUCKET"),
    ),
    "qdrant": QdrantResource(url=EnvVar("QDRANT_URL")),
    "elasticsearch": ElasticsearchResource(url=EnvVar("ELASTICSEARCH_URL")),
    "redis": RedisResource(url=EnvVar("REDIS_URL")),
}

defs = Definitions(
    assets=[legislation_lake_bucket, bronze_boe_norms, silver_boe_norms],
    resources=resources,
)
```

- [ ] **Step 3: Materialise Silver for the test partition**

In Dagster UI → Assets → `silver_boe_norms` → Materialize → `country=es|date=2024-01-15`.

Expected: MinIO console shows `legislation-lake/silver/es/boe/year=2024/month=01/day=15/` with a `_delta_log/` directory.

- [ ] **Step 4: Commit**

```
feat(pipeline): Silver Delta Lake asset with JMESPath extraction, HTML parsing, NER
```
Stage: `pipeline/pipeline/assets/silver.py`, `pipeline/pipeline/__init__.py`

---

## Task 7: Gold transform asset and asset check (TDD)

**Files:**
- Create: `pipeline/pipeline/assets/gold.py`
- Create: `pipeline/tests/pipeline/test_gold_transform.py`
- Modify: `pipeline/pipeline/__init__.py`

- [ ] **Step 1: Write failing tests in `pipeline/tests/pipeline/test_gold_transform.py`**

```python
import pytest
from pipeline.adapters.models import (
    SourceAdapter, FetchConfig, ParseConfig, IndexIdPath,
)


@pytest.fixture
def boe_adapter():
    return SourceAdapter(
        country="es",
        source="boe",
        fetch=FetchConfig(
            type="rest_json",
            base_url="http://example.com",
            daily_index_endpoint="/index/{date}",
            document_endpoint="/doc/{id}",
            index_id_path=IndexIdPath(root="data", nest=["items"], id_field="id"),
            doc_fields={"norm_id": "data.id", "rango": "data.rango", "raw_status": "data.status"},
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
        norm_types={"Ley": "act", "Real Decreto": "regulation"},
        relationship_types={"modifica": "AMENDS", "deroga": "REPEALS"},
        status_mapping={"En vigor": "in_force", "Derogado": "repealed"},
    )


def test_map_norm_row_known_type(boe_adapter):
    from pipeline.assets.gold import map_norm_row, UNKNOWN
    row = {
        "norm_id": "BOE-A-2024-999",
        "title": "Ley de prueba",
        "country": "es",
        "source": "boe",
        "date_published": "2024-01-15",
        "raw_status": "En vigor",
        "storage_key": "silver/es/boe/year=2024/month=01/day=15/",
        "source_norm_type_raw": "Ley",
    }
    result = map_norm_row(row, boe_adapter)
    assert result["norm_type"] == "act"
    assert result["source_type"] == "Ley"
    assert result["status"] == "in_force"
    assert result["norm_id"] == "BOE-A-2024-999"


def test_map_norm_row_unknown_type_returns_unknown(boe_adapter):
    from pipeline.assets.gold import map_norm_row, UNKNOWN
    row = {
        "norm_id": "BOE-A-2024-999",
        "title": "X",
        "country": "es",
        "source": "boe",
        "date_published": "2024-01-15",
        "raw_status": "En vigor",
        "storage_key": "s",
        "source_norm_type_raw": "Providencia",
    }
    result = map_norm_row(row, boe_adapter)
    assert result["norm_type"] == UNKNOWN


def test_map_edge_row_known_rel(boe_adapter):
    from pipeline.assets.gold import map_edge_rows
    edges = map_edge_rows(
        source_id="BOE-A-2024-999",
        raw_rels=[{"source_term": "modifica", "target_id": "BOE-A-1980-100"}],
        adapter=boe_adapter,
        date_published="2024-01-15",
        country="es",
    )
    assert len(edges) == 1
    assert edges[0]["relationship_type"] == "AMENDS"
    assert edges[0]["source_id"] == "BOE-A-2024-999"
    assert edges[0]["target_id"] == "BOE-A-1980-100"
    assert edges[0]["is_ner_derived"] is False


def test_map_edge_row_unknown_rel_is_skipped(boe_adapter):
    from pipeline.assets.gold import map_edge_rows
    edges = map_edge_rows(
        source_id="BOE-A-2024-999",
        raw_rels=[{"source_term": "desconocido", "target_id": "BOE-A-1980-100"}],
        adapter=boe_adapter,
        date_published="2024-01-15",
        country="es",
    )
    assert edges == []


def test_map_edge_row_empty_rels(boe_adapter):
    from pipeline.assets.gold import map_edge_rows
    edges = map_edge_rows(
        source_id="BOE-A-2024-999",
        raw_rels=[],
        adapter=boe_adapter,
        date_published="2024-01-15",
        country="es",
    )
    assert edges == []
```

- [ ] **Step 2: Run tests — expect FAIL**

```bash
docker compose exec dagster-webserver python -m pytest tests/pipeline/test_gold_transform.py -v 2>&1 | head -10
```

Expected: `ModuleNotFoundError: No module named 'pipeline.assets.gold'`

- [ ] **Step 3: Create `pipeline/pipeline/assets/gold.py`**

```python
import json
import os
from datetime import date as Date
from pathlib import Path
from typing import Any

import polars as pl
from dagster import (
    asset, asset_check,
    AssetCheckExecutionContext, AssetCheckResult, AssetExecutionContext,
)
from deltalake import DeltaTable, write_deltalake

from pipeline.adapters.loader import load_adapter, load_canonical
from pipeline.assets.silver import silver_boe_norms
from pipeline.partitions import boe_partitions
from pipeline.resources import MinioResource
from pipeline.utils import delta_storage_options

UNKNOWN = "unknown"

_PROJECT_ROOT = Path(os.environ.get("LEGISLATION_ROOT", Path(__file__).parent.parent.parent.parent))
_CANONICAL_PATH = _PROJECT_ROOT / "ontology" / "canonical.yml"
_ADAPTER_PATHS = {
    "es": _PROJECT_ROOT / "adapters" / "es" / "boe.yml",
}


def map_norm_row(row: dict[str, Any], adapter) -> dict[str, Any]:
    raw_type = row.get("source_norm_type_raw", "")
    raw_status = row.get("raw_status", "")
    return {
        "norm_id": row["norm_id"],
        "title": row["title"],
        "norm_type": adapter.norm_types.get(raw_type, UNKNOWN),
        "source_type": raw_type,
        "status": adapter.status_mapping.get(raw_status, UNKNOWN),
        "country": row["country"],
        "source": row["source"],
        "date_published": row["date_published"],
        "date_repealed": None,
        "ministry_wikidata_id": None,
        "territory_code": row["country"].upper(),
        "storage_key": row["storage_key"],
    }


def map_edge_rows(
    source_id: str,
    raw_rels: list[dict[str, str]],
    adapter,
    date_published: str,
    country: str,
    is_ner_derived: bool = False,
) -> list[dict[str, Any]]:
    edges = []
    for rel in raw_rels:
        source_term = rel.get("source_term", "").lower().strip()
        target_id = rel.get("target_id", "")
        canonical_rel = adapter.relationship_types.get(source_term)
        if canonical_rel and target_id:
            edges.append({
                "source_id": source_id,
                "target_id": target_id,
                "relationship_type": canonical_rel,
                "country": country,
                "date_published": date_published,
                "is_ner_derived": is_ner_derived,
            })
    return edges


@asset(
    partitions_def=boe_partitions,
    deps=[silver_boe_norms],
    description="Apply ontology mapping to Silver data; write canonical Gold Delta Lake tables.",
)
def gold_boe_norms(context: AssetExecutionContext, minio: MinioResource) -> None:
    partition_keys = context.partition_key.keys_by_dimension
    country = partition_keys["country"]
    date_str = partition_keys["date"]
    pub_date = Date.fromisoformat(date_str)
    year, month, day = pub_date.strftime("%Y"), pub_date.strftime("%m"), pub_date.strftime("%d")

    adapter = load_adapter(_ADAPTER_PATHS[country], _CANONICAL_PATH)
    storage_opts = delta_storage_options(minio)

    silver_path = f"s3://{minio.bucket}/silver/{country}/{adapter.source}/year={year}/month={month}/day={day}/"
    try:
        silver_df = pl.from_arrow(DeltaTable(silver_path, storage_options=storage_opts).to_pyarrow())
    except Exception:
        context.log.info(f"No Silver data at {silver_path} — skipping Gold")
        return

    node_rows: list[dict] = []
    edge_rows: list[dict] = []
    unknown_count = 0

    for row in silver_df.to_dicts():
        mapped = map_norm_row(row, adapter)
        if mapped["norm_type"] == UNKNOWN:
            unknown_count += 1
        node_rows.append(mapped)

        raw_rels = json.loads(row.get("raw_relationships") or "[]")
        edge_rows.extend(map_edge_rows(row["norm_id"], raw_rels, adapter, date_str, country))

    if unknown_count > 0:
        pct = unknown_count / len(node_rows) * 100
        context.log.warning(f"{unknown_count}/{len(node_rows)} ({pct:.1f}%) norms have unknown norm_type")

    gold_base = f"s3://{minio.bucket}/gold/country={country}/year={year}/month={month}/day={day}"

    if node_rows:
        nodes_df = pl.DataFrame({col: [r[col] for r in node_rows] for col in node_rows[0].keys()})
        write_deltalake(f"{gold_base}/nodes", nodes_df.to_arrow(), mode="overwrite", storage_options=storage_opts)

    if edge_rows:
        edges_df = pl.DataFrame({col: [r[col] for r in edge_rows] for col in edge_rows[0].keys()})
        write_deltalake(f"{gold_base}/edges", edges_df.to_arrow(), mode="overwrite", storage_options=storage_opts)

    context.log.info(f"Gold: {len(node_rows)} nodes, {len(edge_rows)} edges")
    context.add_output_metadata({
        "nodes_written": len(node_rows),
        "edges_written": len(edge_rows),
        "unknown_norm_types": unknown_count,
    })


@asset_check(asset=gold_boe_norms, description="Validates Gold nodes against canonical vocabulary.")
def gold_ontology_check(context: AssetCheckExecutionContext, minio: MinioResource) -> AssetCheckResult:
    canonical = load_canonical(_CANONICAL_PATH)
    storage_opts = delta_storage_options(minio)
    gold_prefix = f"s3://{minio.bucket}/gold/"
    try:
        df = pl.from_arrow(
            DeltaTable(gold_prefix, storage_options=storage_opts).to_pyarrow(columns=["norm_id", "norm_type", "status"])
        )
    except Exception:
        return AssetCheckResult(passed=True, description="No Gold data yet")

    valid_types = set(canonical["norm_types"]) | {UNKNOWN}
    valid_statuses = set(canonical["statuses"]) | {UNKNOWN}
    bad_types = df.filter(~pl.col("norm_type").is_in(list(valid_types)))
    bad_statuses = df.filter(~pl.col("status").is_in(list(valid_statuses)))
    passed = len(bad_types) == 0 and len(bad_statuses) == 0
    return AssetCheckResult(
        passed=passed,
        description=f"{len(bad_types)} invalid norm_types, {len(bad_statuses)} invalid statuses",
        metadata={"invalid_norm_type_count": len(bad_types), "invalid_status_count": len(bad_statuses)},
    )
```

- [ ] **Step 4: Run Gold transform tests — expect PASS**

```bash
docker compose exec dagster-webserver python -m pytest tests/pipeline/test_gold_transform.py -v
```

Expected: `5 passed`

- [ ] **Step 5: Update `pipeline/pipeline/__init__.py`**

```python
from dagster import Definitions, EnvVar
from pipeline.resources import (
    Neo4jResource, MinioResource, QdrantResource, ElasticsearchResource, RedisResource,
)
from pipeline.assets.minio_init import legislation_lake_bucket
from pipeline.assets.bronze import bronze_boe_norms
from pipeline.assets.silver import silver_boe_norms
from pipeline.assets.gold import gold_boe_norms, gold_ontology_check

resources = {
    "neo4j": Neo4jResource(uri=EnvVar("NEO4J_URI"), password=EnvVar("NEO4J_PASSWORD")),
    "minio": MinioResource(
        endpoint=EnvVar("MINIO_ENDPOINT"),
        access_key=EnvVar("MINIO_ROOT_USER"),
        secret_key=EnvVar("MINIO_ROOT_PASSWORD"),
        bucket=EnvVar("MINIO_BUCKET"),
    ),
    "qdrant": QdrantResource(url=EnvVar("QDRANT_URL")),
    "elasticsearch": ElasticsearchResource(url=EnvVar("ELASTICSEARCH_URL")),
    "redis": RedisResource(url=EnvVar("REDIS_URL")),
}

defs = Definitions(
    assets=[legislation_lake_bucket, bronze_boe_norms, silver_boe_norms, gold_boe_norms],
    asset_checks=[gold_ontology_check],
    resources=resources,
)
```

- [ ] **Step 6: Materialise Gold and run the asset check**

In Dagster UI → Assets → `gold_boe_norms` → Materialize → `country=es|date=2024-01-15`.

Expected: MinIO shows `legislation-lake/gold/country=es/year=2024/month=01/day=15/nodes/` and `edges/` with `_delta_log/`.

Then run the asset check via the Dagster UI (Checks tab on `gold_boe_norms`). Expected: passed.

- [ ] **Step 7: Commit**

```
feat(pipeline): Gold Delta Lake asset with ontology mapping and asset check
```
Stage: `pipeline/pipeline/assets/gold.py`, `pipeline/tests/pipeline/test_gold_transform.py`, `pipeline/pipeline/__init__.py`

---

## Task 8: Neo4j nodes and edges assets

**Files:**
- Create: `pipeline/pipeline/assets/neo4j_nodes.py`
- Create: `pipeline/pipeline/assets/neo4j_edges.py`
- Modify: `pipeline/pipeline/__init__.py`

- [ ] **Step 1: Create `pipeline/pipeline/assets/neo4j_nodes.py`**

```python
from datetime import date as Date

import polars as pl
from dagster import asset, AssetExecutionContext
from deltalake import DeltaTable

from pipeline.assets.gold import gold_boe_norms
from pipeline.partitions import boe_partitions
from pipeline.resources import MinioResource, Neo4jResource
from pipeline.utils import delta_storage_options

MERGE_NORMS_CYPHER = """
UNWIND $batch AS row
MERGE (n:Norm {id: row.norm_id})
SET
  n.title          = row.title,
  n.norm_type      = row.norm_type,
  n.source_type    = row.source_type,
  n.status         = row.status,
  n.country        = row.country,
  n.source         = row.source,
  n.date_published = date(row.date_published),
  n.territory_code = row.territory_code,
  n.storage_key    = row.storage_key
"""

BATCH_SIZE = 500


@asset(
    partitions_def=boe_partitions,
    deps=[gold_boe_norms],
    description="MERGE Gold Norm nodes into Neo4j.",
)
def neo4j_norm_nodes(
    context: AssetExecutionContext,
    minio: MinioResource,
    neo4j: Neo4jResource,
) -> None:
    partition_keys = context.partition_key.keys_by_dimension
    country = partition_keys["country"]
    date_str = partition_keys["date"]
    pub_date = Date.fromisoformat(date_str)
    year, month, day = pub_date.strftime("%Y"), pub_date.strftime("%m"), pub_date.strftime("%d")

    nodes_path = f"s3://{minio.bucket}/gold/country={country}/year={year}/month={month}/day={day}/nodes"
    try:
        df = pl.from_arrow(DeltaTable(nodes_path, storage_options=delta_storage_options(minio)).to_pyarrow())
    except Exception:
        context.log.info(f"No Gold nodes at {nodes_path} — skipping")
        return

    rows = df.to_dicts()
    for r in rows:
        r.pop("date_repealed", None)
        r.pop("ministry_wikidata_id", None)

    with neo4j.get_driver() as driver:
        with driver.session() as session:
            for i in range(0, len(rows), BATCH_SIZE):
                session.run(MERGE_NORMS_CYPHER, batch=rows[i: i + BATCH_SIZE])

    context.log.info(f"Neo4j: merged {len(rows)} Norm nodes for {date_str}")
    context.add_output_metadata({"nodes_merged": len(rows)})
```

- [ ] **Step 2: Create `pipeline/pipeline/assets/neo4j_edges.py`**

```python
from collections import defaultdict
from datetime import date as Date

import polars as pl
from dagster import asset, AssetExecutionContext
from deltalake import DeltaTable

from pipeline.assets.neo4j_nodes import neo4j_norm_nodes
from pipeline.partitions import boe_partitions
from pipeline.resources import MinioResource, Neo4jResource
from pipeline.utils import delta_storage_options

_CYPHER = {
    "AMENDS":     "UNWIND $batch AS r MERGE (s:Norm {id:r.source_id}) MERGE (t:Norm {id:r.target_id}) MERGE (s)-[e:AMENDS]->(t) SET e.is_ner_derived=r.is_ner_derived, e.date_published=date(r.date_published)",
    "REPEALS":    "UNWIND $batch AS r MERGE (s:Norm {id:r.source_id}) MERGE (t:Norm {id:r.target_id}) MERGE (s)-[e:REPEALS]->(t) SET e.is_ner_derived=r.is_ner_derived, e.date_published=date(r.date_published)",
    "CITES":      "UNWIND $batch AS r MERGE (s:Norm {id:r.source_id}) MERGE (t:Norm {id:r.target_id}) MERGE (s)-[e:CITES]->(t) SET e.is_ner_derived=r.is_ner_derived, e.date_published=date(r.date_published)",
    "IMPLEMENTS": "UNWIND $batch AS r MERGE (s:Norm {id:r.source_id}) MERGE (t:Norm {id:r.target_id}) MERGE (s)-[e:IMPLEMENTS]->(t) SET e.is_ner_derived=r.is_ner_derived, e.date_published=date(r.date_published)",
}

BATCH_SIZE = 200


@asset(
    partitions_def=boe_partitions,
    deps=[neo4j_norm_nodes],
    description="MERGE Gold relationship edges into Neo4j.",
)
def neo4j_norm_edges(
    context: AssetExecutionContext,
    minio: MinioResource,
    neo4j: Neo4jResource,
) -> None:
    partition_keys = context.partition_key.keys_by_dimension
    country = partition_keys["country"]
    date_str = partition_keys["date"]
    pub_date = Date.fromisoformat(date_str)
    year, month, day = pub_date.strftime("%Y"), pub_date.strftime("%m"), pub_date.strftime("%d")

    edges_path = f"s3://{minio.bucket}/gold/country={country}/year={year}/month={month}/day={day}/edges"
    try:
        df = pl.from_arrow(DeltaTable(edges_path, storage_options=delta_storage_options(minio)).to_pyarrow())
    except Exception:
        context.log.info(f"No Gold edges at {edges_path} — skipping")
        return

    by_type: dict[str, list] = defaultdict(list)
    skipped = 0
    for row in df.to_dicts():
        rel_type = row.get("relationship_type", "")
        if rel_type in _CYPHER:
            by_type[rel_type].append(row)
        else:
            context.log.warning(f"Unknown rel type {rel_type!r} — skipping")
            skipped += 1

    merged = 0
    with neo4j.get_driver() as driver:
        with driver.session() as session:
            for rel_type, rel_rows in by_type.items():
                for i in range(0, len(rel_rows), BATCH_SIZE):
                    session.run(_CYPHER[rel_type], batch=rel_rows[i: i + BATCH_SIZE])
                    merged += len(rel_rows[i: i + BATCH_SIZE])

    context.log.info(f"Neo4j: merged {merged} edges, skipped {skipped} for {date_str}")
    context.add_output_metadata({"edges_merged": merged, "edges_skipped": skipped})
```

- [ ] **Step 3: Update `pipeline/pipeline/__init__.py`**

```python
from dagster import Definitions, EnvVar
from pipeline.resources import (
    Neo4jResource, MinioResource, QdrantResource, ElasticsearchResource, RedisResource,
)
from pipeline.assets.minio_init import legislation_lake_bucket
from pipeline.assets.bronze import bronze_boe_norms
from pipeline.assets.silver import silver_boe_norms
from pipeline.assets.gold import gold_boe_norms, gold_ontology_check
from pipeline.assets.neo4j_nodes import neo4j_norm_nodes
from pipeline.assets.neo4j_edges import neo4j_norm_edges

resources = {
    "neo4j": Neo4jResource(uri=EnvVar("NEO4J_URI"), password=EnvVar("NEO4J_PASSWORD")),
    "minio": MinioResource(
        endpoint=EnvVar("MINIO_ENDPOINT"),
        access_key=EnvVar("MINIO_ROOT_USER"),
        secret_key=EnvVar("MINIO_ROOT_PASSWORD"),
        bucket=EnvVar("MINIO_BUCKET"),
    ),
    "qdrant": QdrantResource(url=EnvVar("QDRANT_URL")),
    "elasticsearch": ElasticsearchResource(url=EnvVar("ELASTICSEARCH_URL")),
    "redis": RedisResource(url=EnvVar("REDIS_URL")),
}

defs = Definitions(
    assets=[
        legislation_lake_bucket, bronze_boe_norms, silver_boe_norms,
        gold_boe_norms, neo4j_norm_nodes, neo4j_norm_edges,
    ],
    asset_checks=[gold_ontology_check],
    resources=resources,
)
```

- [ ] **Step 4: Materialise and verify in Neo4j browser**

Materialise `neo4j_norm_nodes` → `country=es|date=2024-01-15`, then `neo4j_norm_edges` → same partition.

In Neo4j browser (`http://localhost:7474`, login `neo4j` / your `NEO4J_PASSWORD`):

```cypher
MATCH (n:Norm) RETURN count(n) AS total
```
Expected: non-zero.

```cypher
MATCH (n:Norm) RETURN n LIMIT 3
```
Expected: nodes have `title`, `norm_type`, `status`, `date_published`.

```cypher
MATCH ()-[r]->() RETURN type(r), count(r) ORDER BY count(r) DESC
```
Expected: rows for whichever relationship types had data that day.

- [ ] **Step 5: Commit**

```
feat(pipeline): Neo4j Norm node and edge MERGE assets
```
Stage: `pipeline/pipeline/assets/neo4j_nodes.py`, `pipeline/pipeline/assets/neo4j_edges.py`, `pipeline/pipeline/__init__.py`

---

## Task 9: Briefing cache asset

**Files:**
- Create: `pipeline/pipeline/assets/briefings.py`
- Modify: `pipeline/pipeline/__init__.py`

- [ ] **Step 1: Create `pipeline/pipeline/assets/briefings.py`**

```python
import json
from dagster import asset, AssetExecutionContext
from pipeline.assets.neo4j_edges import neo4j_norm_edges
from pipeline.resources import Neo4jResource, RedisResource

_QUERIES = {
    "briefing_1": """
        MATCH (n:Norm)<-[:AMENDS]-(m:Norm)
        RETURN n.id AS norm_id, n.title AS title, count(m) AS times_amended
        ORDER BY times_amended DESC LIMIT 5
    """,
    "briefing_2": """
        MATCH (n:Norm)-[:AMENDS]->(m:Norm)
        RETURN n.id AS norm_id, n.title AS title, count(m) AS norms_amended
        ORDER BY norms_amended DESC LIMIT 5
    """,
    "briefing_3a": """
        MATCH (total:Norm {status: "in_force"})
        WITH count(total) AS total_live
        MATCH (n:Norm {status: "in_force"})-[:CITES]->(:Norm {status: "repealed"})
        WITH total_live, count(DISTINCT n) AS citing_live
        RETURN round(100.0 * citing_live / total_live, 2) AS pct_citing_ghost
    """,
    "briefing_3b": """
        MATCH (live:Norm {status: "in_force"})-[:CITES]->(ghost:Norm {status: "repealed"})
        RETURN ghost.id AS norm_id, ghost.title AS title, count(live) AS cited_by_live
        ORDER BY cited_by_live DESC LIMIT 5
    """,
    "briefing_4": """
        MATCH (n:Norm {status: "in_force"})-[:CITES]->(ghost:Norm)
        WHERE ghost.id STARTS WITH "BOE-A-1992-" AND ghost.status = "repealed"
        RETURN n.id AS norm_id, n.title AS title, n.date_published AS date_published
        ORDER BY n.date_published DESC
    """,
}


@asset(
    deps=[neo4j_norm_edges],
    description="Run the 4 political briefing Cypher queries and write results to Redis.",
)
def briefing_cache(
    context: AssetExecutionContext,
    neo4j: Neo4jResource,
    redis: RedisResource,
) -> None:
    redis_client = redis.get_client()

    with neo4j.get_driver() as driver:
        with driver.session() as session:
            for key, cypher in _QUERIES.items():
                result = session.run(cypher)
                rows = [dict(record) for record in result]
                redis_client.set(key, json.dumps(rows, default=str))
                context.log.info(f"Cached {key}: {len(rows)} rows")

    context.add_output_metadata({"cached_keys": list(_QUERIES.keys())})
```

> **Note on Briefing 4:** The Cypher above uses `STARTS WITH "BOE-A-1992-"` as a conservative filter for Ley 30/1992. Before the full backfill, run this Cypher in the Neo4j browser to find the exact `norm_id` for Ley 30/1992 and replace the `STARTS WITH` clause with `ghost.id = "<exact-id>"`.

- [ ] **Step 2: Update `pipeline/pipeline/__init__.py`**

```python
from dagster import Definitions, EnvVar
from pipeline.resources import (
    Neo4jResource, MinioResource, QdrantResource, ElasticsearchResource, RedisResource,
)
from pipeline.assets.minio_init import legislation_lake_bucket
from pipeline.assets.bronze import bronze_boe_norms
from pipeline.assets.silver import silver_boe_norms
from pipeline.assets.gold import gold_boe_norms, gold_ontology_check
from pipeline.assets.neo4j_nodes import neo4j_norm_nodes
from pipeline.assets.neo4j_edges import neo4j_norm_edges
from pipeline.assets.briefings import briefing_cache

resources = {
    "neo4j": Neo4jResource(uri=EnvVar("NEO4J_URI"), password=EnvVar("NEO4J_PASSWORD")),
    "minio": MinioResource(
        endpoint=EnvVar("MINIO_ENDPOINT"),
        access_key=EnvVar("MINIO_ROOT_USER"),
        secret_key=EnvVar("MINIO_ROOT_PASSWORD"),
        bucket=EnvVar("MINIO_BUCKET"),
    ),
    "qdrant": QdrantResource(url=EnvVar("QDRANT_URL")),
    "elasticsearch": ElasticsearchResource(url=EnvVar("ELASTICSEARCH_URL")),
    "redis": RedisResource(url=EnvVar("REDIS_URL")),
}

defs = Definitions(
    assets=[
        legislation_lake_bucket, bronze_boe_norms, silver_boe_norms,
        gold_boe_norms, neo4j_norm_nodes, neo4j_norm_edges, briefing_cache,
    ],
    asset_checks=[gold_ontology_check],
    resources=resources,
)
```

- [ ] **Step 3: Materialise briefing_cache and verify Redis**

Materialise `briefing_cache` (not partitioned — runs once). Then verify:

```bash
docker compose exec dagster-webserver python -c "
import redis, json, os
r = redis.from_url(os.environ['REDIS_URL'])
for key in ['briefing_1', 'briefing_2', 'briefing_3a', 'briefing_3b', 'briefing_4']:
    val = r.get(key)
    print(key, '->', len(json.loads(val)), 'rows' if val else 'MISSING')
"
```

Expected: all 5 keys present (some may have 0 rows if the single test partition has insufficient data — that is expected; they will be populated after the full backfill).

- [ ] **Step 4: Commit**

```
feat(pipeline): briefing cache asset — 4 Cypher queries to Redis
```
Stage: `pipeline/pipeline/assets/briefings.py`, `pipeline/pipeline/__init__.py`

---

## Task 10: Daily schedule and final Definitions wiring

**Files:**
- Create: `pipeline/pipeline/schedules.py`
- Modify: `pipeline/pipeline/__init__.py` (final version)

- [ ] **Step 1: Create `pipeline/pipeline/schedules.py`**

```python
from dagster import (
    AssetSelection, RunRequest, define_asset_job, schedule, MultiPartitionKey,
)
from pipeline.partitions import boe_partitions

boe_daily_job = define_asset_job(
    name="boe_daily_ingestion",
    selection=AssetSelection.all(),
    partitions_def=boe_partitions,
)


@schedule(
    cron_schedule="30 23 * * *",  # 23:30 UTC ~ 00:30 CET (BOE publishes by midnight)
    job=boe_daily_job,
    execution_timezone="UTC",
)
def daily_boe_schedule(context):
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(
        partition_key=MultiPartitionKey({"country": "es", "date": scheduled_date}),
    )
```

- [ ] **Step 2: Write the final `pipeline/pipeline/__init__.py`**

```python
from dagster import Definitions, EnvVar
from pipeline.resources import (
    Neo4jResource, MinioResource, QdrantResource, ElasticsearchResource, RedisResource,
)
from pipeline.assets.minio_init import legislation_lake_bucket
from pipeline.assets.bronze import bronze_boe_norms
from pipeline.assets.silver import silver_boe_norms
from pipeline.assets.gold import gold_boe_norms, gold_ontology_check
from pipeline.assets.neo4j_nodes import neo4j_norm_nodes
from pipeline.assets.neo4j_edges import neo4j_norm_edges
from pipeline.assets.briefings import briefing_cache
from pipeline.schedules import daily_boe_schedule

resources = {
    "neo4j": Neo4jResource(uri=EnvVar("NEO4J_URI"), password=EnvVar("NEO4J_PASSWORD")),
    "minio": MinioResource(
        endpoint=EnvVar("MINIO_ENDPOINT"),
        access_key=EnvVar("MINIO_ROOT_USER"),
        secret_key=EnvVar("MINIO_ROOT_PASSWORD"),
        bucket=EnvVar("MINIO_BUCKET"),
    ),
    "qdrant": QdrantResource(url=EnvVar("QDRANT_URL")),
    "elasticsearch": ElasticsearchResource(url=EnvVar("ELASTICSEARCH_URL")),
    "redis": RedisResource(url=EnvVar("REDIS_URL")),
}

defs = Definitions(
    assets=[
        legislation_lake_bucket, bronze_boe_norms, silver_boe_norms,
        gold_boe_norms, neo4j_norm_nodes, neo4j_norm_edges, briefing_cache,
    ],
    asset_checks=[gold_ontology_check],
    schedules=[daily_boe_schedule],
    resources=resources,
)
```

- [ ] **Step 3: Run the full test suite**

```bash
docker compose exec dagster-webserver python -m pytest tests/ -v
```

Expected: all tests pass (adapter models, REST JSON fetcher, HTML parser, Gold transform).

- [ ] **Step 4: Verify schedule appears in Dagster UI**

Open `http://localhost:3000` → Schedules tab. `daily_boe_schedule` should appear with cron `30 23 * * *`.

- [ ] **Step 5: Run the complete pipeline for one partition end-to-end via Dagster UI**

Assets → select all → Materialize → partition `country=es|date=2024-01-15`.

Dagster will execute: `legislation_lake_bucket` → `bronze_boe_norms` → `silver_boe_norms` → `gold_boe_norms` → `neo4j_norm_nodes` → `neo4j_norm_edges` → `briefing_cache`.

Verify each step turns green.

- [ ] **Step 6: Final commit**

```
feat(pipeline): daily schedule, final Definitions wiring — complete ingestion pipeline
```
Stage: `pipeline/pipeline/schedules.py`, `pipeline/pipeline/__init__.py`

---

## Adding a Second Source (Zero Python Required)

To add EUR-Lex after this plan is complete:

1. Create `adapters/eu/eur-lex.yml` with `fetch:`, `parse:`, `norm_types:`, `relationship_types:`, `status_mapping:`.
2. Add `"eu": _PROJECT_ROOT / "adapters" / "eu" / "eur-lex.yml"` to `_ADAPTER_PATHS` in `bronze.py`, `silver.py`, `gold.py`.
3. Add `"eu"` to `StaticPartitionsDefinition` in `partitions.py`.

No new fetcher or parser Python file. The generic `RestJsonFetcher` and `HtmlParser` handle it via config.
