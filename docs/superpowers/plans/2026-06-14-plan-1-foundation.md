# Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the full local development environment — all services running, health-checked, and connected — with the Neo4j schema enforced and project scaffolds in place for the pipeline, API, and frontend.

**Architecture:** Docker Compose orchestrates all backend services (Neo4j, Qdrant, Elasticsearch, Redis, MinIO, PostgreSQL, Dagster). Neo4j schema constraints and indexes are applied at startup via an init script. The Dagster, FastAPI, and React projects are scaffolded with minimal working code so each can be iterated on independently in later plans.

**Tech Stack:** Docker Compose, Neo4j 5.x + GDS plugin, Qdrant, Elasticsearch 8.x, Redis 7, MinIO, PostgreSQL 16, Dagster, FastAPI, React + Vite, Python 3.12, uv (package manager)

---

## File Map

| File | Responsibility |
|---|---|
| `docker-compose.yml` | All services for local dev |
| `docker-compose.prod.yml` | Production overrides (Nginx, no dev ports) |
| `.env.example` | All required environment variables documented |
| `.env` | Local secrets (gitignored) |
| `neo4j/init/01_schema.cypher` | Constraints + indexes applied at Neo4j startup |
| `pipeline/Dockerfile` | Dagster image |
| `pipeline/pyproject.toml` | Dagster + dependencies |
| `pipeline/pipeline/__init__.py` | Empty Dagster definitions (filled in Plan 2) |
| `pipeline/pipeline/resources.py` | Neo4j, MinIO, Qdrant, Elasticsearch resource definitions |
| `api/Dockerfile` | FastAPI image |
| `api/pyproject.toml` | FastAPI + dependencies |
| `api/main.py` | FastAPI app with `/health` endpoint only |
| `frontend/package.json` | React + Vite + Sigma.js dependencies |
| `frontend/vite.config.ts` | Vite config with API proxy |
| `frontend/src/main.tsx` | React entry point |
| `frontend/src/App.tsx` | Root component with placeholder text |
| `scripts/wait-for-services.sh` | Health check script for CI |

---

## Task 1: Repository structure and environment config

**Files:**
- Create: `.env.example`
- Create: `.gitignore`

- [ ] **Step 1: Create `.gitignore`**

```gitignore
.env
__pycache__/
*.pyc
.venv/
node_modules/
dist/
.dagster/
neo4j/data/
minio/data/
postgres/data/
qdrant/data/
elasticsearch/data/
```

- [ ] **Step 2: Create `.env.example`**

```env
# Neo4j
NEO4J_PASSWORD=localpassword

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_BUCKET=legislation-lake

# PostgreSQL (Dagster metadata store)
POSTGRES_USER=dagster
POSTGRES_PASSWORD=dagster
POSTGRES_DB=dagster

# Elasticsearch
# No auth in local dev — xpack.security disabled

# Qdrant
# No auth in local dev

# Anthropic (GraphRAG — not needed until Plan 4)
ANTHROPIC_API_KEY=

# Embedding model (not needed until Plan 3)
OPENAI_API_KEY=
```

- [ ] **Step 3: Copy `.env.example` to `.env`**

```bash
cp .env.example .env
```

Fill in `NEO4J_PASSWORD`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `POSTGRES_PASSWORD` with local values. Leave API keys empty for now.

- [ ] **Step 4: Commit**

```bash
git add .gitignore .env.example
git commit -m "chore: add gitignore and env template"
```

---

## Task 2: Docker Compose — all backend services

**Files:**
- Create: `docker-compose.yml`

- [ ] **Step 1: Write `docker-compose.yml`**

```yaml
services:

  neo4j:
    image: neo4j:5
    ports:
      - "7474:7474"   # browser UI
      - "7687:7687"   # bolt
    volumes:
      - neo4j_data:/data
      - ./neo4j/init:/var/lib/neo4j/import/init
    environment:
      NEO4J_AUTH: neo4j/${NEO4J_PASSWORD}
      NEO4J_PLUGINS: '["graph-data-science"]'
      NEO4J_server_memory_heap_initial__size: 1G
      NEO4J_server_memory_heap_max__size: 2G
    healthcheck:
      test: ["CMD", "neo4j", "status"]
      interval: 10s
      timeout: 5s
      retries: 10

  qdrant:
    image: qdrant/qdrant:latest
    ports:
      - "6333:6333"
    volumes:
      - qdrant_data:/qdrant/storage
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:6333/readyz"]
      interval: 10s
      timeout: 5s
      retries: 10

  elasticsearch:
    image: elasticsearch:8.13.0
    ports:
      - "9200:9200"
    volumes:
      - es_data:/usr/share/elasticsearch/data
    environment:
      discovery.type: single-node
      xpack.security.enabled: "false"
      ES_JAVA_OPTS: "-Xms512m -Xmx512m"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9200/_cluster/health"]
      interval: 10s
      timeout: 5s
      retries: 10

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 10

  minio:
    image: minio/minio:latest
    ports:
      - "9000:9000"   # API
      - "9001:9001"   # console UI
    volumes:
      - minio_data:/data
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
    command: server /data --console-address ":9001"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 10s
      timeout: 5s
      retries: 10

  postgres:
    image: postgres:16-alpine
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"]
      interval: 5s
      timeout: 3s
      retries: 10

  dagster-webserver:
    build:
      context: ./pipeline
      dockerfile: Dockerfile
    ports:
      - "3000:3000"
    command: dagster-webserver -h 0.0.0.0 -p 3000 -w /opt/dagster/app/workspace.yaml
    environment:
      DAGSTER_PG_HOST: postgres
      DAGSTER_PG_USER: ${POSTGRES_USER}
      DAGSTER_PG_PASSWORD: ${POSTGRES_PASSWORD}
      DAGSTER_PG_DB: ${POSTGRES_DB}
      NEO4J_URI: bolt://neo4j:7687
      NEO4J_PASSWORD: ${NEO4J_PASSWORD}
      MINIO_ENDPOINT: http://minio:9000
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
      MINIO_BUCKET: ${MINIO_BUCKET}
      QDRANT_URL: http://qdrant:6333
      ELASTICSEARCH_URL: http://elasticsearch:9200
      REDIS_URL: redis://redis:6379
    depends_on:
      postgres:
        condition: service_healthy
      neo4j:
        condition: service_healthy
      minio:
        condition: service_healthy
    volumes:
      - ./pipeline:/opt/dagster/app
      - dagster_home:/opt/dagster/dagster_home

  dagster-daemon:
    build:
      context: ./pipeline
      dockerfile: Dockerfile
    command: dagster-daemon run
    environment:
      DAGSTER_PG_HOST: postgres
      DAGSTER_PG_USER: ${POSTGRES_USER}
      DAGSTER_PG_PASSWORD: ${POSTGRES_PASSWORD}
      DAGSTER_PG_DB: ${POSTGRES_DB}
      NEO4J_URI: bolt://neo4j:7687
      NEO4J_PASSWORD: ${NEO4J_PASSWORD}
      MINIO_ENDPOINT: http://minio:9000
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
      MINIO_BUCKET: ${MINIO_BUCKET}
      QDRANT_URL: http://qdrant:6333
      ELASTICSEARCH_URL: http://elasticsearch:9200
      REDIS_URL: redis://redis:6379
    depends_on:
      - dagster-webserver
    volumes:
      - ./pipeline:/opt/dagster/app
      - dagster_home:/opt/dagster/dagster_home

  api:
    build:
      context: ./api
      dockerfile: Dockerfile
    ports:
      - "8000:8000"
    environment:
      NEO4J_URI: bolt://neo4j:7687
      NEO4J_PASSWORD: ${NEO4J_PASSWORD}
      QDRANT_URL: http://qdrant:6333
      ELASTICSEARCH_URL: http://elasticsearch:9200
      REDIS_URL: redis://redis:6379
      MINIO_ENDPOINT: http://minio:9000
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
      MINIO_BUCKET: ${MINIO_BUCKET}
    depends_on:
      neo4j:
        condition: service_healthy
      redis:
        condition: service_healthy
    volumes:
      - ./api:/app
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 10s
      timeout: 5s
      retries: 10

volumes:
  neo4j_data:
  qdrant_data:
  es_data:
  minio_data:
  postgres_data:
  dagster_home:
```

- [ ] **Step 2: Commit**

```bash
git add docker-compose.yml
git commit -m "chore: add docker-compose with all backend services"
```

---

## Task 3: Neo4j schema — constraints and indexes

**Files:**
- Create: `neo4j/init/01_schema.cypher`

Neo4j does not auto-apply Cypher files — the init script is run manually once after the container first starts (see Task 7). The file serves as the authoritative schema definition.

- [ ] **Step 1: Create `neo4j/init/01_schema.cypher`**

```cypher
// ── Norm ──────────────────────────────────────────────────────────────────
// Uniqueness constraint also creates a lookup index on norm_id
CREATE CONSTRAINT norm_id_unique IF NOT EXISTS
  FOR (n:Norm) REQUIRE n.id IS UNIQUE;

CREATE INDEX norm_status IF NOT EXISTS
  FOR (n:Norm) ON (n.status);

CREATE INDEX norm_country IF NOT EXISTS
  FOR (n:Norm) ON (n.country);

CREATE INDEX norm_type IF NOT EXISTS
  FOR (n:Norm) ON (n.norm_type);

CREATE INDEX norm_date IF NOT EXISTS
  FOR (n:Norm) ON (n.date_published);

// Composite: most briefing queries filter by country + status together
CREATE INDEX norm_country_status IF NOT EXISTS
  FOR (n:Norm) ON (n.country, n.status);

// Full-text index on title for in-graph keyword search
CREATE FULLTEXT INDEX norm_title_fulltext IF NOT EXISTS
  FOR (n:Norm) ON EACH [n.title];

// ── OntologyConcept ───────────────────────────────────────────────────────
CREATE CONSTRAINT ontology_concept_id_unique IF NOT EXISTS
  FOR (o:OntologyConcept) REQUIRE o.skos_id IS UNIQUE;

CREATE INDEX ontology_label_es IF NOT EXISTS
  FOR (o:OntologyConcept) ON (o.pref_label_es);

// ── Territory ─────────────────────────────────────────────────────────────
CREATE CONSTRAINT territory_code_unique IF NOT EXISTS
  FOR (t:Territory) REQUIRE t.code IS UNIQUE;

// ── GovernmentBody ────────────────────────────────────────────────────────
CREATE CONSTRAINT gov_body_wikidata_unique IF NOT EXISTS
  FOR (g:GovernmentBody) REQUIRE g.wikidata_id IS UNIQUE;
```

- [ ] **Step 2: Commit**

```bash
git add neo4j/init/01_schema.cypher
git commit -m "feat: add Neo4j schema constraints and indexes"
```

---

## Task 4: Dagster project scaffold

**Files:**
- Create: `pipeline/Dockerfile`
- Create: `pipeline/pyproject.toml`
- Create: `pipeline/dagster.yaml`
- Create: `pipeline/workspace.yaml`
- Create: `pipeline/pipeline/__init__.py`
- Create: `pipeline/pipeline/resources.py`

- [ ] **Step 1: Create `pipeline/pyproject.toml`**

```toml
[build-system]
requires = ["setuptools"]
build-backend = "setuptools.backends.legacy:build"

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
]

[tool.setuptools.packages.find]
where = ["."]
include = ["pipeline*"]
```

- [ ] **Step 2: Create `pipeline/Dockerfile`**

```dockerfile
FROM python:3.12-slim

RUN pip install uv

WORKDIR /opt/dagster/app

COPY pyproject.toml .
COPY dagster.yaml /opt/dagster/dagster_home/dagster.yaml

RUN uv pip install --system -e .

# Download spaCy Spanish model
RUN python -m spacy download es_core_news_sm

ENV DAGSTER_HOME=/opt/dagster/dagster_home
```

- [ ] **Step 3: Create `pipeline/dagster.yaml`**

```yaml
telemetry:
  enabled: false

storage:
  postgres:
    postgres_db:
      username:
        env: DAGSTER_PG_USER
      password:
        env: DAGSTER_PG_PASSWORD
      hostname:
        env: DAGSTER_PG_HOST
      db_name:
        env: DAGSTER_PG_DB
      port: 5432
```

- [ ] **Step 4: Create `pipeline/workspace.yaml`**

```yaml
load_from:
  - python_package:
      package_name: pipeline
```

- [ ] **Step 5: Create `pipeline/pipeline/__init__.py`**

```python
from dagster import Definitions

# Assets and resources are added in Plan 2.
defs = Definitions(assets=[], resources={})
```

- [ ] **Step 6: Create `pipeline/pipeline/resources.py`**

```python
from dagster import ConfigurableResource
from neo4j import GraphDatabase
from minio import Minio
from qdrant_client import QdrantClient
from elasticsearch import Elasticsearch
import redis


class Neo4jResource(ConfigurableResource):
    uri: str
    password: str

    def get_driver(self):
        return GraphDatabase.driver(self.uri, auth=("neo4j", self.password))


class MinioResource(ConfigurableResource):
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str

    def get_client(self) -> Minio:
        # Strip http:// prefix — Minio client takes host:port only
        host = self.endpoint.replace("http://", "").replace("https://", "")
        secure = self.endpoint.startswith("https://")
        return Minio(host, access_key=self.access_key, secret_key=self.secret_key, secure=secure)


class QdrantResource(ConfigurableResource):
    url: str

    def get_client(self) -> QdrantClient:
        return QdrantClient(url=self.url)


class ElasticsearchResource(ConfigurableResource):
    url: str

    def get_client(self) -> Elasticsearch:
        return Elasticsearch(self.url)


class RedisResource(ConfigurableResource):
    url: str

    def get_client(self):
        return redis.from_url(self.url)
```

- [ ] **Step 7: Update `pipeline/pipeline/__init__.py` to wire resources**

```python
import os
from dagster import Definitions
from pipeline.resources import (
    Neo4jResource,
    MinioResource,
    QdrantResource,
    ElasticsearchResource,
    RedisResource,
)

resources = {
    "neo4j": Neo4jResource(
        uri=os.environ["NEO4J_URI"],
        password=os.environ["NEO4J_PASSWORD"],
    ),
    "minio": MinioResource(
        endpoint=os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        bucket=os.environ["MINIO_BUCKET"],
    ),
    "qdrant": QdrantResource(url=os.environ["QDRANT_URL"]),
    "elasticsearch": ElasticsearchResource(url=os.environ["ELASTICSEARCH_URL"]),
    "redis": RedisResource(url=os.environ["REDIS_URL"]),
}

# Assets are added in Plan 2.
defs = Definitions(assets=[], resources=resources)
```

- [ ] **Step 8: Commit**

```bash
git add pipeline/
git commit -m "feat: scaffold Dagster project with service resources"
```

---

## Task 5: FastAPI project scaffold

**Files:**
- Create: `api/Dockerfile`
- Create: `api/pyproject.toml`
- Create: `api/main.py`

- [ ] **Step 1: Create `api/pyproject.toml`**

```toml
[build-system]
requires = ["setuptools"]
build-backend = "setuptools.backends.legacy:build"

[project]
name = "api"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "fastapi>=0.111",
    "uvicorn[standard]>=0.30",
    "neo4j>=5.20",
    "qdrant-client>=1.9",
    "elasticsearch>=8.13",
    "redis>=5.0",
    "minio>=7.2",
    "anthropic>=0.28",
    "langgraph>=0.1",
    "pydantic>=2.7",
    "httpx>=0.27",
]
```

- [ ] **Step 2: Create `api/Dockerfile`**

```dockerfile
FROM python:3.12-slim

RUN pip install uv

WORKDIR /app

COPY pyproject.toml .
RUN uv pip install --system -e .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
```

- [ ] **Step 3: Create `api/main.py`**

```python
from fastapi import FastAPI

app = FastAPI(title="BOE Knowledge Graph API", version="0.1.0")


@app.get("/health")
def health():
    return {"status": "ok"}
```

- [ ] **Step 4: Commit**

```bash
git add api/
git commit -m "feat: scaffold FastAPI project with health endpoint"
```

---

## Task 6: React + Vite frontend scaffold

**Files:**
- Create: `frontend/package.json`
- Create: `frontend/vite.config.ts`
- Create: `frontend/index.html`
- Create: `frontend/src/main.tsx`
- Create: `frontend/src/App.tsx`
- Create: `frontend/tsconfig.json`

- [ ] **Step 1: Create `frontend/package.json`**

```json
{
  "name": "boe-knowledge-graph",
  "version": "0.1.0",
  "private": true,
  "scripts": {
    "dev": "vite",
    "build": "tsc && vite build",
    "preview": "vite preview"
  },
  "dependencies": {
    "react": "^18.3.1",
    "react-dom": "^18.3.1",
    "@react-sigma/core": "^4.0.3",
    "sigma": "^3.0.0",
    "graphology": "^0.25.4",
    "graphology-types": "^0.24.7"
  },
  "devDependencies": {
    "@types/react": "^18.3.3",
    "@types/react-dom": "^18.3.0",
    "@vitejs/plugin-react": "^4.3.0",
    "typescript": "^5.4.5",
    "vite": "^5.2.11"
  }
}
```

- [ ] **Step 2: Create `frontend/vite.config.ts`**

```typescript
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': {
        target: process.env.VITE_API_URL ?? 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
})
```

- [ ] **Step 3: Create `frontend/tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "ES2020",
    "lib": ["ES2020", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "moduleResolution": "bundler",
    "jsx": "react-jsx",
    "strict": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noFallthroughCasesInSwitch": true,
    "skipLibCheck": true
  },
  "include": ["src"]
}
```

- [ ] **Step 4: Create `frontend/index.html`**

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>BOE Knowledge Graph</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

- [ ] **Step 5: Create `frontend/src/main.tsx`**

```tsx
import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
)
```

- [ ] **Step 6: Create `frontend/src/App.tsx`**

```tsx
export default function App() {
  return (
    <div style={{ fontFamily: 'sans-serif', padding: '2rem' }}>
      <h1>BOE Knowledge Graph</h1>
      <p>Platform coming in Plan 5.</p>
    </div>
  )
}
```

- [ ] **Step 7: Install dependencies**

```bash
cd frontend && npm install
```

Expected: `node_modules/` created, no errors.

- [ ] **Step 8: Verify dev server starts**

```bash
npm run dev
```

Expected output includes: `Local: http://localhost:5173/`

Open `http://localhost:5173` in a browser. Expected: page renders "BOE Knowledge Graph — Platform coming in Plan 5."

Stop the dev server (`Ctrl+C`).

- [ ] **Step 9: Commit**

```bash
cd .. && git add frontend/
git commit -m "feat: scaffold React + Vite + Sigma.js frontend"
```

---

## Task 7: Bring up all services and apply Neo4j schema

- [ ] **Step 1: Start all services**

```bash
docker compose up -d
```

Expected: all containers start. Verify with:

```bash
docker compose ps
```

All services should show `healthy` or `running`. Allow ~60 seconds for Neo4j and Elasticsearch to fully initialise before proceeding.

- [ ] **Step 2: Wait for Neo4j to be ready**

```bash
docker compose exec neo4j neo4j status
```

Expected output includes: `Neo4j is running`

- [ ] **Step 3: Apply the schema**

```bash
docker compose exec neo4j cypher-shell \
  -u neo4j \
  -p "${NEO4J_PASSWORD}" \
  --file /var/lib/neo4j/import/init/01_schema.cypher
```

Expected: each `CREATE CONSTRAINT` and `CREATE INDEX` statement outputs `0 rows available after X ms`.

- [ ] **Step 4: Verify constraints and indexes in Neo4j browser**

Open `http://localhost:7474` in a browser. Log in with username `neo4j` and the password from `.env`.

Run in the browser query box:
```cypher
SHOW CONSTRAINTS
```
Expected: 4 rows — `norm_id_unique`, `ontology_concept_id_unique`, `territory_code_unique`, `gov_body_wikidata_unique`.

```cypher
SHOW INDEXES
```
Expected: indexes for `norm_status`, `norm_country`, `norm_type`, `norm_date`, `norm_country_status`, `norm_title_fulltext` present alongside the constraint-backing indexes.

- [ ] **Step 5: Verify Dagster webserver**

Open `http://localhost:3000` in a browser.
Expected: Dagster UI loads. The "Definitions" tab shows an empty asset graph (no assets yet — added in Plan 2).

- [ ] **Step 6: Verify MinIO console**

Open `http://localhost:9001` in a browser. Log in with `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` from `.env`.
Expected: MinIO console loads. No buckets exist yet (created in Plan 2).

- [ ] **Step 7: Verify FastAPI health endpoint**

```bash
curl http://localhost:8000/health
```

Expected:
```json
{"status": "ok"}
```

- [ ] **Step 8: Verify Elasticsearch**

```bash
curl http://localhost:9200/_cluster/health
```

Expected: JSON response with `"status": "green"` or `"yellow"` (yellow is normal for single-node).

- [ ] **Step 9: Verify Qdrant**

```bash
curl http://localhost:6333/readyz
```

Expected: `{}` with HTTP 200.

- [ ] **Step 10: Verify Redis**

```bash
docker compose exec redis redis-cli ping
```

Expected: `PONG`

---

## Task 8: Production Docker Compose overrides

**Files:**
- Create: `docker-compose.prod.yml`
- Create: `nginx/nginx.conf`

- [ ] **Step 1: Create `nginx/nginx.conf`**

```nginx
events {}

http {
    server {
        listen 80;
        server_name _;

        location / {
            return 301 https://$host$request_uri;
        }
    }

    server {
        listen 443 ssl;
        server_name _;

        # Replace yourdomain.com with your actual domain before deploying (Plan 6)
        ssl_certificate /etc/letsencrypt/live/yourdomain.com/fullchain.pem;
        ssl_certificate_key /etc/letsencrypt/live/yourdomain.com/privkey.pem;

        location /api/ {
            proxy_pass http://api:8000/;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
        }

        location /dagster/ {
            proxy_pass http://dagster-webserver:3000/;
            proxy_set_header Host $host;
        }
    }
}
```

- [ ] **Step 2: Create `docker-compose.prod.yml`**

```yaml
services:
  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/nginx.conf:ro
      - certbot_certs:/etc/letsencrypt:ro
    depends_on:
      - api

  api:
    volumes: []   # no bind mount in prod — image is the source of truth

  dagster-webserver:
    volumes:
      - dagster_home:/opt/dagster/dagster_home  # keep state, drop code bind mount

  dagster-daemon:
    volumes:
      - dagster_home:/opt/dagster/dagster_home

volumes:
  certbot_certs:
```

Production is started with:
```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

- [ ] **Step 3: Commit**

```bash
git add docker-compose.prod.yml nginx/
git commit -m "feat: add production Docker Compose overrides and Nginx config"
```

---

## Task 9: Health check script

**Files:**
- Create: `scripts/wait-for-services.sh`

Useful in CI and when running Plan 2 ingestion for the first time.

- [ ] **Step 1: Create `scripts/wait-for-services.sh`**

```bash
#!/usr/bin/env bash
set -e

echo "Waiting for services..."

wait_for() {
  local name=$1
  local cmd=$2
  local retries=30
  until eval "$cmd" > /dev/null 2>&1; do
    retries=$((retries - 1))
    if [ $retries -eq 0 ]; then
      echo "TIMEOUT: $name did not become ready"
      exit 1
    fi
    echo "  $name not ready, retrying..."
    sleep 2
  done
  echo "  $name ready"
}

wait_for "Neo4j"        "docker compose exec neo4j neo4j status"
wait_for "Qdrant"       "curl -sf http://localhost:6333/readyz"
wait_for "Elasticsearch" "curl -sf http://localhost:9200/_cluster/health"
wait_for "Redis"        "docker compose exec redis redis-cli ping"
wait_for "MinIO"        "curl -sf http://localhost:9000/minio/health/live"
wait_for "API"          "curl -sf http://localhost:8000/health"

echo "All services ready."
```

- [ ] **Step 2: Make executable**

```bash
chmod +x scripts/wait-for-services.sh
```

- [ ] **Step 3: Run it to verify all services are up**

```bash
./scripts/wait-for-services.sh
```

Expected: each service prints `ready` and final line is `All services ready.`

- [ ] **Step 4: Commit**

```bash
git add scripts/wait-for-services.sh
git commit -m "chore: add service health check script"
```

---

## Plan 1 complete

At this point:
- All backend services are running and healthy locally
- Neo4j schema constraints and indexes are applied
- Dagster UI is accessible at `http://localhost:3000`
- FastAPI returns `{"status": "ok"}` at `http://localhost:8000/health`
- React dev server serves the scaffold page at `http://localhost:5173`
- Production Compose overrides are ready for Plan 6 deployment

**Next:** Plan 2 — Ingestion Pipeline (BOE Bronze → Silver → Gold → Neo4j)
