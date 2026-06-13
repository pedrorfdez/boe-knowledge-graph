---
name: boe-knowledge-graph-design
description: Complete system design for the BOE Knowledge Graph platform — ingestion pipeline, graph schema, vector search, GraphRAG, and web UI
metadata:
  type: project
---

# BOE Knowledge Graph — System Design

**Date:** 2026-06-13
**Project:** Reversa AI — Founding Engineer Technical Challenge

---

## 1. Overview

The Boletín Oficial del Estado (BOE) is the source of all Spanish consolidated legislation. Today, no tool gives policymakers a navigable view of the statute book: which laws have become unreadable, which laws created the mess, how much live law rests on repealed ground, and which orphaned references remain from past reforms.

This platform ingests the full BOE consolidated corpus, models it as a knowledge graph, and delivers four political briefings to the Council of Ministers through an interactive web interface.

The architecture is designed to scale beyond Spain to millions of norms across any country's legislation, with a GraphRAG query layer that enables natural-language exploration of the graph.

### Deliverables

1. A data ingestion pipeline consuming the full BOE corpus and updating daily
2. A knowledge graph with navigable relationships between norms
3. A web platform with interactive graph visualisation and the four briefings
4. A GraphRAG query endpoint for natural-language exploration

---

## 2. Tech Stack

| Component | Technology | Rationale |
|---|---|---|
| Orchestration | Dagster | Asset-based model maps directly to pipeline stages; native partitioning handles daily incremental loads and multi-country backfills |
| Data lake | MinIO | S3-compatible, Docker-native, no vendor lock-in; supports Delta Lake via `delta-rs` |
| Table format | Delta Lake | ACID, schema evolution, partition evolution, time travel, UPSERT — applied to Silver and Gold; no catalog service required |
| Knowledge graph | Neo4j 5.x + GDS | Industry standard for graph traversal; GDS plugin for Louvain community detection and PageRank |
| Vector store | Qdrant | Hybrid dense + sparse search, payload filtering, Docker-native, Rust-based performance at scale |
| Full-text search | Elasticsearch 8.x | Keyword search, field-level filtering, snippet highlighting at norm and article level |
| Cache | Redis 7.x | Pre-computed briefing results and hot ego network TTL cache |
| Backend | FastAPI (Python) | Async, stateless, horizontally scalable; clean OpenAPI docs |
| GraphRAG orchestration | LangGraph | Explicit state machine for multi-step conditional pipeline; nodes are plain Python — LangGraph only routes |
| LLM | Claude (Anthropic SDK) | LLM synthesis in the GraphRAG query layer |
| Embedding model | Configurable (OpenAI / Cohere / local) | Decoupled from Qdrant write path; swap without pipeline changes |
| Frontend | React + Vite + Sigma.js | Sigma.js WebGL rendering handles large graphs; Vite for fast builds |
| Frontend hosting | Vercel | Auto-deploy from main branch; free tier sufficient |
| Backend hosting | VPS (Hetzner / DigitalOcean) | Docker Compose runs the full backend stack; persistent volumes for Neo4j, Qdrant, MinIO |
| Containerisation | Docker Compose | Same file for local dev and production |

---

## 3. Data Lake — MinIO (Medallion Architecture)

Three layers in a single MinIO bucket. Nothing is deleted — only appended.

### Layer summary

| Layer | Format | Purpose |
|---|---|---|
| Bronze | Raw files (JSON / XML / PDF / HTML) | Immutable copy of source data; format dictated by source |
| Silver | Delta Lake (nested Parquet) | Normalised per source, schema-enforced, article-structured |
| Gold | Delta Lake (flat Parquet) | Unified canonical schema; feeds Neo4j, Qdrant, Elasticsearch |

### Bronze

Exact copy of whatever the source delivers. Never transformed.

```
bronze/
  {country}/
    {source}/
      year={YYYY}/
        month={MM}/
          day={DD}/
            {document-id}.json        ← BOE API response
            {document-id}.xml         ← EUR-Lex XML
            {document-id}.pdf         ← scanned source
```

Examples:
```
bronze/es/boe/year=2024/month=01/day=15/BOE-A-2024-123.json
bronze/fr/legifrance/year=2024/month=01/day=15/LOI-2024-456.json
bronze/eu/eur-lex/year=2024/month=01/day=15/reg-2024-789.xml
```

### Silver

Delta Lake table, partitioned by `(country, source, year, month, day)`. One Parquet file per partition batch — eliminates the small-files problem (50k individual JSON writes become one batch write per source per day).

Schema evolution via Delta Lake `ALTER TABLE ADD COLUMN` — new fields for new countries do not require rewriting historical partitions. Partition evolution allows restructuring without full rewrites. Time travel enables rollback when a transformation contains a bug.

The legal text hierarchy is preserved as nested Parquet columns:

```
norm_id              STRING         BOE-A-2024-123
title                STRING
country              STRING         es
source               STRING         boe
date_published       DATE
preamble_text        STRING
articles             ARRAY<STRUCT<
                       article_id    STRING
                       title_num     STRING
                       chapter_num   STRING
                       article_num   STRING
                       text          STRING
                     >>
disposiciones        ARRAY<STRUCT<
                       type          STRING   adicional | transitoria | derogatoria | final
                       num           STRING
                       text          STRING
                     >>
annexes              ARRAY<STRUCT<
                       annex_id      STRING
                       title         STRING
                       text          STRING
                     >>
raw_relationships    ARRAY<STRUCT<
                       source_term   STRING   modifica | deroga | cita
                       target_id     STRING
                     >>
raw_status           STRING         En vigor | Derogada | ...
ner_entities         ARRAY<STRUCT<
                       entity_type   STRING   ministry | territory | norm_reference
                       value         STRING
                       wikidata_id   STRING   nullable
                     >>
```

Path convention:
```
silver/es/boe/year=2024/month=01/day=15/norms.parquet
```

### Gold

Delta Lake table, partitioned by `(country, year, month, day)`. Country-agnostic schema — all sources converge here. Gold is the only input to Neo4j, Qdrant, and Elasticsearch.

**nodes table:**
```
norm_id          STRING     BOE-A-2024-123
title            STRING
norm_type        STRING     canonical Akoma Ntoso type: act | regulation | order | ...
source_type      STRING     original term: Ley Orgánica (provenance)
status           STRING     in_force | repealed | partially_repealed
country          STRING     ISO 3166: es | fr | eu
source           STRING     boe | legifrance | eur-lex
date_published   DATE
date_repealed    DATE       nullable
ministry_wikidata_id STRING  nullable
territory_code   STRING     NUTS / ISO 3166
storage_key      STRING     MinIO Silver path for full structured text
```

**edges table:**
```
source_id        STRING
target_id        STRING
relationship_type STRING    canonical: AMENDS | REPEALS | CITES | IMPLEMENTS
country          STRING
date_published   DATE
is_ner_derived   BOOLEAN    true = extracted from body text, false = from formal analisis block
```

Path convention:
```
gold/country=es/year=2024/month=01/day=15/nodes.parquet
gold/country=es/year=2024/month=01/day=15/edges.parquet
```

---

## 4. Ontology Layer

Standards-based. No custom taxonomy invented. Loaded into Neo4j as a subgraph alongside the norm nodes.

| Concept | Standard | Source | Update cadence |
|---|---|---|---|
| Norm / document types | Akoma Ntoso (OASIS/UN) | OASIS | On breaking version change |
| Thematic topics | EuroVoc (SKOS) | EU Publications Office | Quarterly |
| Territories | NUTS + ISO 3166 | Eurostat / ISO | Annually |
| Ministries & government bodies | Wikidata | Wikimedia Foundation | Monthly |

### Canonical vocabulary — single source of truth

`ontology/canonical.yml` defines all valid values. Both the Pydantic adapter models and the Dagster Gold asset checks derive from this file. Adding a new canonical type means editing one file.

```yaml
# ontology/canonical.yml
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

### Adapter validation

Two complementary mechanisms ensure every adapter maps only to canonical values:

**1. Pydantic at load time** — catches errors in development and CI before any data is processed:

```python
class SourceAdapter(BaseModel):
    country: str
    source: str
    norm_types: dict[str, str]
    relationship_types: dict[str, str]
    status_mapping: dict[str, str]

    @field_validator("norm_types")
    def validate_norm_types(cls, v):
        canonical = load_canonical()["norm_types"]
        invalid = set(v.values()) - set(canonical)
        if invalid:
            raise ValueError(f"Unknown norm types: {invalid}")
        return v
    # same validators for relationship_types and status_mapping
```

**2. Dagster asset checks at pipeline runtime** — validates the Gold output after every materialisation:

```python
@asset_check(asset=gold_norms)
def gold_ontology_check(context, gold_norms):
    canonical = load_canonical()
    df = read_gold_nodes(gold_norms)
    invalid = df[~df["norm_type"].isin(canonical["norm_types"])]
    return AssetCheckResult(
        passed=len(invalid) == 0,
        metadata={"invalid_norm_type_rows": len(invalid)}
    )
```

### Adapter file structure

```
adapters/
  es/
    boe.yml
  fr/
    legifrance.yml
  eu/
    eur-lex.yml
```

Example adapter:
```yaml
# adapters/es/boe.yml
country: es
source: boe

norm_types:
  "Ley":                     "act"
  "Ley Orgánica":            "act"
  "Real Decreto-legislativo":"act"
  "Real Decreto-ley":        "regulation"
  "Real Decreto":            "regulation"
  "Orden Ministerial":       "order"
  "Resolución":              "resolution"
  "Decreto":                 "decree"

relationship_types:
  "modifica":   "AMENDS"
  "deroga":     "REPEALS"
  "cita":       "CITES"
  "desarrolla": "IMPLEMENTS"
  "transpone":  "IMPLEMENTS"

status_mapping:
  "En vigor":                  "in_force"
  "Derogada":                  "repealed"
  "Derogada parcialmente":     "partially_repealed"
  "Vigente con modificaciones":"in_force"
```

---

## 5. Ingestion Pipeline — Dagster

### Partition key

`MultiPartitionsDefinition(country × date)` — one logical partition per (country, day). Enables:
- Per-country backfills without touching other countries
- Daily incremental runs that only process new data
- Re-processing of a single country's history when an adapter is updated

### Asset graph

```
bronze_norms          (per country — fetch from source API / scraper)
      ↓
silver_norms          (per country adapter — normalize, NER, structure)
      ↓
gold_norms            (unified schema, ontology mapping applied)
      ↓ ─────────────────────────────────────────────────────────
      ↓              ↓                  ↓                  ↓
neo4j_nodes     neo4j_edges     qdrant_embeddings   elasticsearch_index
      ↓
neo4j_ontology        (bootstrap: EuroVoc, NUTS, Wikidata — runs once, then on schedule)
      ↓
neo4j_gds_analytics   (Louvain community detection + PageRank — after each daily run)
      ↓
briefing_cache        (4 Cypher queries → Redis — after GDS analytics)
```

### NER in Silver step

spaCy with a Spanish legal NER model extracts entities mentioned in norm body text that do not appear in the formal `analisis` block:
- Other norms referenced by name inline
- Ministries and government bodies
- Territories and municipalities

These become additional Gold edges with `is_ner_derived = true`. They enrich the graph beyond what the formal metadata provides.

---

## 6. Knowledge Graph — Neo4j

Neo4j 5.x with Graph Data Science (GDS) plugin enabled.

### Node types

**Norm** (core node — kept thin, only traversal-critical properties):
```
id                   BOE-A-2024-123
title                Real Decreto-ley 28/2020
norm_type            regulation               ← canonical (Akoma Ntoso)
source_type          Real Decreto-ley         ← original (provenance)
status               in_force
country              es
date_published       2020-09-22
date_repealed        null
storage_key          silver/es/boe/year=2020/month=09/day=22/norms.parquet
community_id         42                       ← computed by GDS Louvain
pagerank             0.0034                   ← computed by GDS
```

**OntologyConcept** (EuroVoc SKOS):
```
skos_id              eurovoc:5541
pref_label_es        derecho laboral
pref_label_en        labour law
```

**Territory** (NUTS / ISO 3166):
```
code                 ES
name                 España
level                country
nuts_id              ES
```

**GovernmentBody** (Wikidata):
```
wikidata_id          Q14917057
name                 Ministerio de Trabajo
country              es
```

### Relationship types (canonical — all cross-country)

```cypher
(:Norm)-[:AMENDS]->(:Norm)
(:Norm)-[:REPEALS]->(:Norm)
(:Norm)-[:CITES]->(:Norm)
(:Norm)-[:IMPLEMENTS]->(:Norm)
(:Norm)-[:COVERS_TOPIC]->(:OntologyConcept)
(:Norm)-[:ISSUED_BY]->(:GovernmentBody)
(:Norm)-[:APPLIES_TO]->(:Territory)
(:OntologyConcept)-[:BROADER_THAN]->(:OntologyConcept)
(:Territory)-[:PART_OF]->(:Territory)
```

### GDS analytics (Dagster asset, runs after each daily ingestion)

**Louvain community detection** on the `AMENDS` + `CITES` subgraph — groups of norms that reference each other heavily become a community (effectively: thematic clusters without manual labelling). Result stored as `community_id` on each Norm node.

**PageRank** on the citation graph — norms cited by authoritative norms rank higher than those with raw citation count alone. Result stored as `pagerank` on each Norm node.

### The four briefings as pre-computed Cypher

```cypher
-- Briefing 1: top 5 most amended norms
MATCH (n:Norm)<-[:AMENDS]-(m:Norm)
RETURN n.id, n.title, count(m) AS times_amended
ORDER BY times_amended DESC LIMIT 5

-- Briefing 2: top 5 omnibus laws
MATCH (n:Norm)-[:AMENDS]->(m:Norm)
RETURN n.id, n.title, count(m) AS norms_amended
ORDER BY norms_amended DESC LIMIT 5

-- Briefing 3a: % of in-force norms citing at least one repealed norm
MATCH (total:Norm {status: "in_force"})
WITH count(total) AS total_live
MATCH (n:Norm {status: "in_force"})-[:CITES]->(:Norm {status: "repealed"})
WITH total_live, count(DISTINCT n) AS citing_live
RETURN round(100.0 * citing_live / total_live, 2) AS pct_citing_ghost

-- Briefing 3b: top 5 repealed norms most cited by in-force norms (ghost norms)
MATCH (live:Norm {status: "in_force"})-[:CITES]->(ghost:Norm {status: "repealed"})
RETURN ghost.id, ghost.title, count(live) AS cited_by_live
ORDER BY cited_by_live DESC LIMIT 5

-- Briefing 4: blast radius of Ley 30/1992
-- NOTE: BOE-A-1992-26318 must be verified against the BOE API before use
MATCH (n:Norm {status: "in_force"})-[:CITES]->(ghost:Norm {id: "BOE-A-1992-26318"})
RETURN n.id, n.title, n.date_published
ORDER BY n.date_published DESC
```

Results are written to Redis by the `briefing_cache` Dagster asset after each daily ingestion cycle.

---

## 7. Chunking Strategy

The atomic unit of legislation is the **article** — numbered, self-contained, and the unit referenced by other norms ("Art. 14 de la Ley 39/2015"). Generic token-based chunking destroys this structure.

### Legislative hierarchy

```
Norm
  ├── Preámbulo / Exposición de motivos    1 chunk — the "why" of the law
  ├── Título I
  │   └── Capítulo I
  │       ├── Artículo 1                   1 chunk per article
  │       └── Artículo 2
  ├── Disposición adicional 1              1 chunk each
  ├── Disposición transitoria 1            1 chunk each
  ├── Disposición derogatoria 1            1 chunk each
  ├── Disposición final 1                  1 chunk each
  └── Anexo I                              1 chunk per annex (subdivided if long)
```

The BOE API's XML format preserves this hierarchy natively — chunking is a tree parse, not a text split.

### Context prepending

Articles are sometimes very short. The embedding is computed with a prepended context string that is not stored:

```
"Real Decreto-ley 28/2020, Título I, Capítulo II — Artículo 5: Jornada de trabajo a distancia..."
```

The stored Qdrant payload contains only the article text. The embedding reflects its position in the document.

### Small-to-large retrieval

Qdrant searches at article granularity (fine-grained, precise). When building LLM context for GraphRAG, the retrieval expands upward to the parent chapter or title for surrounding context. This gives the LLM enough legislative context without flooding the context window with the full norm.

---

## 8. Vector Store — Qdrant

### What is stored

One Qdrant point per article / disposición / annex chunk. Full text lives in the payload.

```python
{
    "norm_id":      "BOE-A-2020-11043",
    "chunk_id":     "BOE-A-2020-11043-art-5",
    "chunk_type":   "article",             # article | preamble | disposition | annex
    "hierarchy": {
        "title":    "Título I",
        "chapter":  "Capítulo II",
        "article":  "Artículo 5"
    },
    "text":         "Artículo 5. Jornada de trabajo a distancia...",
    "norm_type":    "regulation",
    "status":       "in_force",
    "country":      "es",
    "date_published": "2020-09-22",
    "community_id": 42
}
```

### Hybrid search

Qdrant runs dense (semantic) and sparse (BM25 keyword) vectors simultaneously in a single query, with score fusion. This is essential for legal text: a query for "Ley 30/1992" must match exactly, not semantically.

### Payload filtering

Vector search is combined with metadata filters in a single Qdrant call:

```python
qdrant.search(
    collection_name="norms",
    query_vector=embedding,
    query_filter=Filter(must=[
        FieldCondition(key="country", match=MatchValue(value="es")),
        FieldCondition(key="status", match=MatchValue(value="in_force")),
        FieldCondition(key="norm_type", match=MatchAny(any=["act", "regulation"]))
    ]),
    limit=20
)
```

No post-filtering. The filter runs inside the HNSW index scan.

---

## 9. Full-text Search — Elasticsearch

Two index levels:

**Norm-level index** — for broad search and dashboard filtering:
```json
{
  "norm_id": "BOE-A-2020-11043",
  "title": "Real Decreto-ley 28/2020...",
  "norm_type": "regulation",
  "status": "in_force",
  "country": "es",
  "date_published": "2020-09-22",
  "ministry": "Ministerio de Trabajo",
  "topics": ["labour law", "digital economy"],
  "preamble_text": "..."
}
```

**Article-level index** — for precise search with snippet highlighting:
```json
{
  "norm_id": "BOE-A-2020-11043",
  "chunk_id": "BOE-A-2020-11043-art-5",
  "article_number": "Artículo 5",
  "title_num": "Título I",
  "chapter_num": "Capítulo I",
  "text": "Artículo 5. Jornada de trabajo a distancia..."
}
```

The `/api/norms/search` endpoint queries both levels and merges results, returning norm-level metadata with article-level snippet highlights.

---

## 10. Cache — Redis

**Pre-computed briefing results** — written by the `briefing_cache` Dagster asset after each daily ingestion. Dashboard loads are instant; no on-demand Cypher in the critical path. TTL: none (invalidated on next Dagster run).

**Hot ego network cache** — ego networks for frequently viewed norms are cached with a 1-hour TTL. Invalidated after each daily ingestion cycle.

**GraphRAG intermediate results** — not cached (each query is unique). The LangGraph pipeline runs fresh per request.

---

## 11. Backend — FastAPI

Stateless. Connects to Neo4j, Qdrant, Elasticsearch, Redis. Runs behind Nginx on the VPS.

### Endpoints

```
GET  /api/briefings/{1..4}
     Returns pre-computed briefing result from Redis.
     Falls back to live Cypher if cache is cold.

GET  /api/graph/ego/{norm_id}?depth=2
     Returns ego network subgraph (nodes + edges) from Neo4j.
     depth: 1-3 hops. Cached in Redis for popular norms.

GET  /api/graph/community/{community_id}
     Returns all norms in a Louvain community cluster.

GET  /api/norms/{norm_id}
     Returns norm metadata from Neo4j + full structured text
     fetched from MinIO Silver using storage_key.

GET  /api/norms/search?q={query}&country=es&status=in_force&norm_type=act
     Hybrid search: Elasticsearch (keyword) merged with Qdrant (semantic).
     Returns norm list with article-level snippets.

GET  /api/ontology/topics
     EuroVoc concept tree from Neo4j OntologyConcept nodes.
     Used to populate the topic filter in the UI.

GET  /api/ontology/ministries
     GovernmentBody nodes from Neo4j. Used for ministry filter.

POST /api/query
     Body: { "query": "string", "filters": { "country": "es", ... } }
     Runs the LangGraph GraphRAG pipeline.
     Returns: { "answer": "string", "cited_norm_ids": [...], "subgraph": {...} }
```

---

## 12. GraphRAG Query Layer — LangGraph

LangGraph orchestrates the pipeline as an explicit state machine. All data access (Qdrant, Neo4j, Anthropic SDK) is implemented as plain Python functions. LangGraph only manages state transitions and conditional routing.

### State

```python
class GraphRAGState(TypedDict):
    query: str
    filters: dict                    # country, status, norm_type filters from request
    query_embedding: list[float]
    candidate_norm_ids: list[str]
    context_subgraph: dict           # nodes + edges from Neo4j traversal
    ontology_context: dict           # relevant EuroVoc concepts + ministry context
    traversal_depth: int             # current hop depth; max 3
    is_context_sufficient: bool
    answer: str
    cited_norm_ids: list[str]
```

### Nodes (plain Python functions)

1. **embed_query** — embed query string via embedding model
2. **vector_search** — Qdrant hybrid search with payload filters from `state.filters`; returns top-K norm IDs
3. **expand_graph_context** — Neo4j traversal from candidate norm IDs at `state.traversal_depth` hops; collects subgraph
4. **enrich_ontology** — pulls EuroVoc topic hierarchy and GovernmentBody context for candidates from Neo4j
5. **evaluate_context** — Claude decides: is this subgraph sufficient to answer the query? Returns `is_context_sufficient` and optionally a revised traversal strategy
6. **synthesise_answer** — Claude generates structured answer with inline citations to norm IDs

### Graph structure

```
embed_query
    ↓
vector_search
    ↓
expand_graph_context
    ↓
enrich_ontology
    ↓
evaluate_context ──── is_context_sufficient=false, depth < 3 ──→ expand_graph_context (loop)
    │
    └── is_context_sufficient=true
            ↓
    synthesise_answer
```

The conditional loop — where the LLM requests deeper traversal — is the reason LangGraph is used over a plain pipeline. A chain cannot express this cleanly.

---

## 13. Frontend — React + Sigma.js

Built with Vite. Deployed to Vercel. API URL configured via `VITE_API_URL` environment variable pointing to the VPS.

### Landing page (hybrid approach)

Four briefing cards, each showing a headline result:
- B1: "Top 5 most amended laws in Spain"
- B2: "Top 5 omnibus laws — single acts that rewrote dozens"
- B3: "X% of live law rests on repealed ground"
- B4: "Ley 30/1992 blast radius — Y laws still cite a repealed act"

Each card links to its full briefing view with the interactive graph.

Global search bar (full-text + semantic) and a stat strip: total norms indexed, countries covered, total relationships.

### Graph view — Sigma.js (WebGL)

The full corpus is never rendered in the browser. The graph view always shows a subgraph:
- Default entry: ego network of a selected norm at depth 2
- Briefing entry: briefing-filtered subgraph (e.g. the top 5 most amended norms and their amenders)

Visual encoding:
- Node colour: community cluster (Louvain `community_id`)
- Node size: PageRank score
- Edge colour: relationship type (AMENDS=red, CITES=blue, REPEALS=grey, IMPLEMENTS=green)

Interaction:
- Click node → sidebar with norm title, type, status, topics, ministry, dates, link to full text
- Sidebar "expand" button → loads ego network of that norm
- Filters panel: country, norm_type, status, topic (EuroVoc hierarchy picker), date range
- Search: calls `/api/norms/search` → highlights matching nodes in current view

### Briefing views

**B1 — Diagnosis:**
Ranked list of top 5 most-amended norms with amendment count. Graph panel shows the star pattern of amenders around each. Entry point for the consolidation reform workstream.

**B2 — Root cause:**
Ranked list of top 5 omnibus laws with count of norms they amend. Graph panel shows each omnibus as a hub with its amendment edges fanned out. Makes the legislative sprawl pattern visible.

**B3 — The rot:**
Headline percentage stat (% of in-force norms citing at least one repealed norm). Top 5 ghost norms ranked by how many live norms cite them. Graph panel highlights live norms → ghost edges.

**B4 — The scalpel:**
Ley 30/1992 shown as a central node. All in-force norms still citing it shown as a scrollable worklist alongside the graph. Each row in the worklist is clickable — opens that norm's ego network. This is the operational output: the list a ministry team would use to close the orphaned references.

### Query view — GraphRAG

Text input → `POST /api/query`. Two-panel output:
- **Answer panel**: LLM response with inline norm citations rendered as clickable chips
- **Graph panel**: the context subgraph used for the answer — makes the reasoning transparent

---

## 14. Infrastructure

### Docker Compose services

```yaml
services:

  neo4j:
    image: neo4j:5                     # Community Edition — GDS plugin works on Community
    volumes:
      - neo4j_data:/data
    environment:
      NEO4J_PLUGINS: '["graph-data-science"]'
      NEO4J_AUTH: neo4j/${NEO4J_PASSWORD}

  qdrant:
    image: qdrant/qdrant
    volumes:
      - qdrant_data:/qdrant/storage

  elasticsearch:
    image: elasticsearch:8.13.0
    volumes:
      - es_data:/usr/share/elasticsearch/data
    environment:
      discovery.type: single-node
      xpack.security.enabled: "false"

  redis:
    image: redis:7-alpine

  minio:
    image: minio/minio
    volumes:
      - minio_data:/data
    command: server /data --console-address ":9001"

  postgres:
    image: postgres:16-alpine          # Dagster metadata store only
    volumes:
      - postgres_data:/var/lib/postgresql/data

  dagster-webserver:
    build: ./pipeline
    command: dagster-webserver -h 0.0.0.0 -p 3000

  dagster-daemon:
    build: ./pipeline
    command: dagster-daemon run

  api:
    build: ./api
    environment:
      NEO4J_URL: bolt://neo4j:7687
      QDRANT_URL: http://qdrant:6333
      ELASTICSEARCH_URL: http://elasticsearch:9200
      REDIS_URL: redis://redis:6379
      MINIO_URL: http://minio:9000

  nginx:                               # production only
    image: nginx:alpine
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
      - certbot_certs:/etc/letsencrypt
```

### VPS specification

Minimum: 4 vCPU, 16 GB RAM (Neo4j and Elasticsearch each need headroom).
Hetzner CX31 (~€12/month) is sufficient for the BOE corpus. Scale to CX41 (8 vCPU, 16 GB) when adding a second country.

### Vercel

React app built with Vite. `VITE_API_URL=https://api.yourdomain.com`. Auto-deploys from `main` branch push.

### Local dev

Same `docker-compose.yml`. The `api` service runs with hot-reload (`uvicorn --reload`). React runs via `vite dev` outside Docker for fast HMR.

---

## 15. Daily Incremental Ingestion

Dagster scheduler triggers at 00:30 daily (BOE publishes by midnight).

```
1.  bronze_norms     Fetch today's BOE entries via /api/sumario/{date}
                     Write raw JSON to bronze/es/boe/year=.../month=.../day=.../

2.  silver_norms     Parse BOE XML structure into article hierarchy
                     Apply NER (spaCy)
                     Write Delta Lake batch to silver/es/boe/year=.../month=.../day=.../

3.  gold_norms       Apply adapter/es/boe.yml ontology mapping
                     Validate against ontology/canonical.yml (Dagster asset check)
                     Write Delta Lake batch to gold/country=es/year=.../month=.../day=.../

4.  neo4j_nodes      MERGE norms by norm_id (upsert — handles status changes)

5.  neo4j_edges      MERGE relationships (idempotent by source_id + target_id + type)

6.  qdrant_embeddings  Embed new norm chunks (context-prepended)
                       Upsert into Qdrant by chunk_id

7.  elasticsearch_index  Index new norms and articles
                          Update existing norm status if changed

8.  neo4j_gds_analytics  Re-run Louvain community detection (incremental)
                          Re-run PageRank
                          Write community_id and pagerank back to Norm nodes

9.  briefing_cache   Execute 4 Cypher queries
                     Write results to Redis
                     Invalidate hot ego network cache keys
```

Full backfill for a new country: trigger a Dagster backfill on the `(new-country, *)` partition. All pipeline steps run unchanged. Only the adapter YAML at `adapters/{country}/{source}.yml` is new.
