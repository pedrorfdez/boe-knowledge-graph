// ── Norm ──────────────────────────────────────────────────────────────────
// Uniqueness constraint also creates a backing range index on norm_id
CREATE CONSTRAINT norm_id_unique IF NOT EXISTS
  FOR (n:Norm) REQUIRE n.id IS UNIQUE;

CREATE RANGE INDEX norm_status IF NOT EXISTS
  FOR (n:Norm) ON (n.status);

CREATE RANGE INDEX norm_country IF NOT EXISTS
  FOR (n:Norm) ON (n.country);

CREATE RANGE INDEX norm_type IF NOT EXISTS
  FOR (n:Norm) ON (n.norm_type);

CREATE RANGE INDEX norm_date IF NOT EXISTS
  FOR (n:Norm) ON (n.date_published);

// Composite: most briefing queries filter by country + status together
CREATE INDEX norm_country_status IF NOT EXISTS
  FOR (n:Norm) ON (n.country, n.status);

// Composite: country + status + norm_type for search endpoint and GraphRAG filters
CREATE INDEX norm_country_status_type IF NOT EXISTS
  FOR (n:Norm) ON (n.country, n.status, n.norm_type);

// Needed for /api/graph/community/{community_id} endpoint
CREATE RANGE INDEX norm_community_id IF NOT EXISTS
  FOR (n:Norm) ON (n.community_id);

// ── OntologyConcept ───────────────────────────────────────────────────────
CREATE CONSTRAINT ontology_concept_id_unique IF NOT EXISTS
  FOR (o:OntologyConcept) REQUIRE o.skos_id IS UNIQUE;

CREATE RANGE INDEX ontology_label_es IF NOT EXISTS
  FOR (o:OntologyConcept) ON (o.pref_label_es);

// ── Territory ─────────────────────────────────────────────────────────────
CREATE CONSTRAINT territory_code_unique IF NOT EXISTS
  FOR (t:Territory) REQUIRE t.code IS UNIQUE;

// ── GovernmentBody ────────────────────────────────────────────────────────
CREATE CONSTRAINT gov_body_wikidata_unique IF NOT EXISTS
  FOR (g:GovernmentBody) REQUIRE g.wikidata_id IS UNIQUE;
