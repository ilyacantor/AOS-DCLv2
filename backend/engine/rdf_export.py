"""RDF graph export assembly (ContextOS Gate 2C) — the tenant's graph as an
rdflib Graph, serialized by the export routes as Turtle and JSON-LD.

Content contract (the test ground-truth formula mirrors this list exactly;
changing it is a contract change, not a tweak):

  Ontology classes — one per concept in config/ontology_concepts.yaml
  (via the same ConceptRegistry the hierarchy API serves):
    (concept, rdf:type,       owl:Class)               always
    (concept, rdfs:label,     name)                    when name non-empty
    (concept, rdfs:comment,   description)             when description non-empty
    (concept, skos:altLabel,  alias)                   one per DISTINCT alias
    (concept, urn:dcl:meta:<field>, text)              per non-null semantic-depth
                                                       field (SEMANTIC_DEPTH_FIELDS)
    (concept, rdfs:subClassOf, parent)                 parent = tenant/builtin
                                                       concept_hierarchy link if
                                                       present, else the concept's
                                                       domain root; absent when
                                                       neither exists
  plus one (concept, rdfs:subClassOf, parent) per concept_hierarchy row whose
  concept is NOT an ontology concept (tenant-attached custom concepts).

  Edge-type vocabulary — one per type in the tenant's registry
  (load_edge_types: built-ins overlaid by tenant rows):
    (edgeType, rdf:type,     owl:ObjectProperty)       always
    (edgeType, rdfs:comment, description)              when description non-empty
    (edgeType, urn:dcl:meta:cardinality, cardinality)  when cardinality non-empty
    (edgeType, urn:dcl:meta:allowedPairs, JSON text)   when allowed_pairs non-null
  Cardinality stays an annotation — deliberately NOT an OWL cardinality
  restriction (the constraint engine at the persistence boundary is the
  enforcement point, the export only describes it).

  Entity individuals — from the live edge subgraph's node endpoints:
    (node, rdf:type, owl:NamedIndividual)              one per DISTINCT node_key
    (node, rdf:type, urn:dcl:node:<node_type>)         one per DISTINCT
                                                       (node_key, node_type)

  Edges — one triple per DISTINCT (src_key, edge_type, dst_key) among the
  tenant's live edges:
    (urn:dcl:entity:src, urn:dcl:edge:TYPE, urn:dcl:entity:dst)
  with per-edge-ROW provenance as a standard OWL axiom annotation
  (10 triples per live edge row, fresh blank node each):
    (ax, rdf:type, owl:Axiom)
    (ax, owl:annotatedSource/annotatedProperty/annotatedTarget, ...)
    (ax, urn:dcl:meta:sourceSystem,    source_system)
    (ax, urn:dcl:meta:confidenceScore, confidence_score as xsd:double)
    (ax, urn:dcl:meta:confidenceTier,  confidence_tier)
    (ax, urn:dcl:meta:derivation,      derivation)
    (ax, urn:dcl:meta:dclIngestId,     the edge row's ingest id — EXPORTED
                                       NAME is dclIngestId; the literal string
                                       run_id appears NOWHERE in any export
                                       body (I1))
    (ax, urn:dcl:meta:ingestedAt,      ingested_at as xsd:dateTime)

Facts (semantic_triples values) are deliberately NOT exported — graph
topology + vocabulary only.

Identity: tenant scoping is the caller's tenant_id parameter only. The tenant
UUID is never written into the graph (I2 — no tenant identifier in any export
body). A tenant (or tenant+entity filter) with no live edges raises
GraphExportEmpty, which the routes serve as a loud 404 — never an empty file.

IRI scheme (campaign-pinned): urn:dcl:concept:/{entity,node,edge,meta}: with
local names percent-encoded (urllib quote, unreserved charset) so arbitrary
keys stay valid IRIs. No import-time DB access — all reads happen inside
build_export_graph().
"""

import json
from typing import Optional
from urllib.parse import quote

from rdflib import BNode, Graph, Literal, Namespace
from rdflib.namespace import OWL, RDF, RDFS, SKOS, XSD

from backend.db.edge_store import get_edge_store, load_edge_types
from backend.registry.concept_hierarchy import _registry, _tenant_links
from backend.utils.log_utils import get_logger

logger = get_logger(__name__)

CONCEPT_NS = Namespace("urn:dcl:concept:")
ENTITY_NS = Namespace("urn:dcl:entity:")
NODE_NS = Namespace("urn:dcl:node:")
EDGE_NS = Namespace("urn:dcl:edge:")
META_NS = Namespace("urn:dcl:meta:")

# The five semantic-depth fields the ontology YAML carries per concept.
SEMANTIC_DEPTH_FIELDS = (
    "recognition_basis",
    "timing_semantics",
    "scope_boundaries",
    "calculation_methodology",
    "comparability_rules",
)

# Per-edge provenance fields (export name -> subgraph row key). All six are
# required by the edge provenance contract at write time — a null here is a
# store-contract breach and fails the export loudly, never a silent skip.
_PROVENANCE_FIELDS = (
    ("sourceSystem", "source_system"),
    ("confidenceScore", "confidence_score"),
    ("confidenceTier", "confidence_tier"),
    ("derivation", "derivation"),
    ("dclIngestId", "dcl_ingest_id"),
    ("ingestedAt", "ingested_at"),
)

# An export that HITS this limit would be silently incomplete — refuse instead.
_EXPORT_EDGE_LIMIT = 100_000


class GraphExportEmpty(LookupError):
    """The tenant (or tenant + entity filter) has no live entity graph."""


def _local(value: str) -> str:
    """IRI local name: percent-encode everything outside the unreserved set."""
    return quote(str(value), safe="-._~")


def jsonld_context() -> dict:
    """Compact @context for the JSON-LD serialization: namespace prefixes plus
    a term mapping for every urn:dcl:meta: annotation the export emits."""
    ctx: dict = {
        "concept": str(CONCEPT_NS),
        "entity": str(ENTITY_NS),
        "node": str(NODE_NS),
        "edge": str(EDGE_NS),
        "meta": str(META_NS),
        "rdfs": str(RDFS),
        "owl": str(OWL),
        "skos": str(SKOS),
        "xsd": str(XSD),
    }
    for term in [name for name, _ in _PROVENANCE_FIELDS] + [
        "cardinality", "allowedPairs", *SEMANTIC_DEPTH_FIELDS,
    ]:
        ctx[term] = {"@id": str(META_NS[term])}
    return ctx


def _bind_prefixes(g: Graph) -> None:
    g.bind("concept", CONCEPT_NS)
    g.bind("entity", ENTITY_NS)
    g.bind("node", NODE_NS)
    g.bind("edge", EDGE_NS)
    g.bind("meta", META_NS)
    g.bind("owl", OWL)
    g.bind("skos", SKOS)


def list_graph_entities(tenant_id: str, entity_id: Optional[str] = None) -> list[str]:
    """The entities the export will walk. Empty result = the tenant has no
    graph (or the requested entity has no live edges) — raises loudly so the
    routes can 404 instead of serving a hollow file."""
    known = get_edge_store().list_entities(tenant_id)
    if entity_id is not None:
        targets = [entity_id] if entity_id in known else []
    else:
        targets = known
    if not targets:
        scope = f"tenant {tenant_id}" + (f", entity {entity_id!r}" if entity_id else "")
        raise GraphExportEmpty(
            f"No entity graph to export for {scope} — no live edges exist in "
            f"entity_edges for that scope. Ingest edges (POST /api/dcl/ingest-edges "
            f"or the records path) before exporting."
        )
    return targets


def _add_ontology_classes(g: Graph, tenant_id: str) -> None:
    reg = _registry()
    links = _tenant_links(tenant_id)  # builtin '*' rows overlaid by tenant rows
    ontology_ids = set(reg.list_concepts())

    for cid in sorted(ontology_ids):
        entry = reg.get_concept(cid) or {}
        iri = CONCEPT_NS[_local(cid)]
        g.add((iri, RDF.type, OWL.Class))
        if entry.get("name"):
            g.add((iri, RDFS.label, Literal(entry["name"])))
        if entry.get("description"):
            g.add((iri, RDFS.comment, Literal(entry["description"])))
        for alias in entry.get("aliases") or []:
            g.add((iri, SKOS.altLabel, Literal(alias)))
        for field in SEMANTIC_DEPTH_FIELDS:
            value = entry.get(field)
            if value:
                g.add((iri, META_NS[field], Literal(value)))
        parent = links.get(cid) or entry.get("domain")
        if parent:
            g.add((iri, RDFS.subClassOf, CONCEPT_NS[_local(parent)]))

    # Tenant-attached custom concepts: hierarchy rows whose subject is not an
    # ontology concept still shape the tenant's class tree.
    for concept, parent in links.items():
        if concept not in ontology_ids and parent:
            g.add((CONCEPT_NS[_local(concept)], RDFS.subClassOf, CONCEPT_NS[_local(parent)]))


def _add_edge_types(g: Graph, tenant_id: str) -> None:
    for edge_type, spec in load_edge_types(tenant_id).items():
        prop = EDGE_NS[_local(edge_type)]
        g.add((prop, RDF.type, OWL.ObjectProperty))
        if spec.get("description"):
            g.add((prop, RDFS.comment, Literal(spec["description"])))
        if spec.get("cardinality"):
            g.add((prop, META_NS["cardinality"], Literal(spec["cardinality"])))
        if spec.get("allowed_pairs") is not None:
            g.add((prop, META_NS["allowedPairs"], Literal(json.dumps(spec["allowed_pairs"]))))


def _add_entity_subgraph(g: Graph, subgraph: dict) -> None:
    for node in subgraph["nodes"]:
        individual = ENTITY_NS[_local(node["node_key"])]
        g.add((individual, RDF.type, OWL.NamedIndividual))
        g.add((individual, RDF.type, NODE_NS[_local(node["node_type"])]))

    for edge in subgraph["edges"]:
        for export_name, row_key in _PROVENANCE_FIELDS:
            if edge.get(row_key) in (None, ""):
                raise RuntimeError(
                    f"Edge {edge.get('id')!r} ({edge.get('src_key')!r} "
                    f"-{edge.get('edge_type')!r}-> {edge.get('dst_key')!r}) is missing "
                    f"required provenance field {row_key!r} — the edge provenance "
                    f"contract guarantees it; refusing to export an incomplete graph."
                )
        src = ENTITY_NS[_local(edge["src_key"])]
        prop = EDGE_NS[_local(edge["edge_type"])]
        dst = ENTITY_NS[_local(edge["dst_key"])]
        g.add((src, prop, dst))

        ax = BNode()
        g.add((ax, RDF.type, OWL.Axiom))
        g.add((ax, OWL.annotatedSource, src))
        g.add((ax, OWL.annotatedProperty, prop))
        g.add((ax, OWL.annotatedTarget, dst))
        g.add((ax, META_NS["sourceSystem"], Literal(edge["source_system"])))
        g.add((ax, META_NS["confidenceScore"], Literal(float(edge["confidence_score"]))))
        g.add((ax, META_NS["confidenceTier"], Literal(edge["confidence_tier"])))
        g.add((ax, META_NS["derivation"], Literal(edge["derivation"])))
        g.add((ax, META_NS["dclIngestId"], Literal(edge["dcl_ingest_id"])))
        g.add((ax, META_NS["ingestedAt"], Literal(edge["ingested_at"], datatype=XSD.dateTime)))


def build_export_graph(tenant_id: str, entity_id: Optional[str] = None) -> Graph:
    """Assemble the tenant's full export graph (optionally filtered to one
    entity). Raises GraphExportEmpty when the scope has no live edges."""
    store = get_edge_store()
    targets = list_graph_entities(tenant_id, entity_id)

    g = Graph()
    _bind_prefixes(g)
    _add_ontology_classes(g, tenant_id)
    _add_edge_types(g, tenant_id)

    for ent in targets:
        subgraph = store.get_subgraph(tenant_id, ent, limit=_EXPORT_EDGE_LIMIT)
        if len(subgraph["edges"]) >= _EXPORT_EDGE_LIMIT:
            raise RuntimeError(
                f"Entity {ent!r} has >= {_EXPORT_EDGE_LIMIT} live edges — the export "
                f"would be silently truncated. Raise _EXPORT_EDGE_LIMIT deliberately "
                f"instead of serving a partial graph."
            )
        _add_entity_subgraph(g, subgraph)

    logger.info(
        "[rdf-export] assembled graph: %d triples across %d entit%s",
        len(g), len(targets), "y" if len(targets) == 1 else "ies",
    )
    return g
