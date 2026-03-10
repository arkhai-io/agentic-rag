"""
JSON-LD Exporter for FAIR-compliant data serialization.

Exports Neo4j graph data to JSON-LD format using standard vocabularies:
- schema.org: General metadata
- Dublin Core (dcterms): Document metadata
- PROV-O: Provenance information
- DCAT: Dataset catalog vocabulary

This enables interoperability with other linked data systems and
compliance with FAIR principles I1-I3 (Interoperability).
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..components.neo4j_manager import GraphStore
from ..types.node_types import ARKHAI_NAMESPACE

# JSON-LD Context with standard vocabularies
JSONLD_CONTEXT = {
    "@vocab": "https://schema.org/",
    "schema": "https://schema.org/",
    "dcterms": "http://purl.org/dc/terms/",
    "prov": "http://www.w3.org/ns/prov#",
    "dcat": "http://www.w3.org/ns/dcat#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "arkhai": ARKHAI_NAMESPACE + "/",
    # Property mappings
    "identifier": "dcterms:identifier",
    "created": {"@id": "dcterms:created", "@type": "xsd:dateTime"},
    "modified": {"@id": "dcterms:modified", "@type": "xsd:dateTime"},
    "title": "dcterms:title",
    "description": "dcterms:description",
    "creator": "dcterms:creator",
    "publisher": "dcterms:publisher",
    "source": "dcterms:source",
    "format": "dcterms:format",
    # PROV-O mappings
    "wasGeneratedBy": {"@id": "prov:wasGeneratedBy", "@type": "@id"},
    "wasDerivedFrom": {"@id": "prov:wasDerivedFrom", "@type": "@id"},
    "wasAttributedTo": {"@id": "prov:wasAttributedTo", "@type": "@id"},
    "generatedAtTime": {"@id": "prov:generatedAtTime", "@type": "xsd:dateTime"},
    "startedAtTime": {"@id": "prov:startedAtTime", "@type": "xsd:dateTime"},
    "endedAtTime": {"@id": "prov:endedAtTime", "@type": "xsd:dateTime"},
    "used": {"@id": "prov:used", "@type": "@id"},
    # DCAT mappings
    "distribution": "dcat:distribution",
    "accessURL": {"@id": "dcat:accessURL", "@type": "@id"},
    "downloadURL": {"@id": "dcat:downloadURL", "@type": "@id"},
    "byteSize": "dcat:byteSize",
    "mediaType": "dcat:mediaType",
    # Custom Arkhai properties
    "fingerprint": "arkhai:fingerprint",
    "contentType": "arkhai:contentType",
    "storageKey": "arkhai:storageKey",
    "configHash": "arkhai:configHash",
    "processingTime": "arkhai:processingTimeMs",
}


class JSONLDExporter:
    """
    Export Neo4j graph data to JSON-LD format.

    Supports exporting:
    - Individual DataPieces as schema:DigitalDocument
    - Runs as prov:Activity
    - Pipelines as schema:SoftwareApplication
    - Complete datasets as dcat:Dataset

    Example:
        ```python
        exporter = JSONLDExporter(graph_store)

        # Export a single DataPiece
        doc = exporter.export_datapiece("fp_abc123")

        # Export a complete dataset for a user/project
        dataset = exporter.export_dataset(
            username="user1",
            project="my_project",
            title="My RAG Dataset",
            description="Documents processed for RAG"
        )

        # Save to file
        exporter.save_to_file(dataset, "dataset.jsonld")
        ```
    """

    def __init__(self, graph_store: GraphStore):
        """
        Initialize the exporter.

        Args:
            graph_store: Neo4j GraphStore instance
        """
        self.graph_store = graph_store

    def export_datapiece(self, fingerprint: str) -> Optional[Dict[str, Any]]:
        """
        Export a single DataPiece as JSON-LD.

        Args:
            fingerprint: The fingerprint of the DataPiece

        Returns:
            JSON-LD document or None if not found
        """
        query = """
        MATCH (d:DataPiece {fingerprint: $fingerprint})
        OPTIONAL MATCH (d)-[:GENERATED_BY]->(r:Run)
        OPTIONAL MATCH (parent:DataPiece)-[t:TRANSFORMED_BY]->(d)
        RETURN d, r, parent, t
        """

        with self.graph_store.driver.session(
            database=self.graph_store.database
        ) as session:
            result = session.run(query, fingerprint=fingerprint).single()

            if not result:
                return None

            node = result["d"]
            run = result["r"]
            parent = result["parent"]
            transform = result["t"]

            return self._datapiece_to_jsonld(node, run, parent, transform)

    def export_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """
        Export a Run as JSON-LD prov:Activity.

        Args:
            run_id: The run ID

        Returns:
            JSON-LD document or None if not found
        """
        query = """
        MATCH (r:Run {id: $run_id})
        OPTIONAL MATCH (d:DataPiece)-[:GENERATED_BY]->(r)
        RETURN r, collect(d.fingerprint) as generated_fingerprints
        """

        with self.graph_store.driver.session(
            database=self.graph_store.database
        ) as session:
            result = session.run(query, run_id=run_id).single()

            if not result:
                return None

            run = result["r"]
            generated = result["generated_fingerprints"]

            return self._run_to_jsonld(run, generated)

    def export_dataset(
        self,
        username: str,
        project: str,
        title: Optional[str] = None,
        description: Optional[str] = None,
        include_runs: bool = True,
        include_lineage: bool = True,
    ) -> Dict[str, Any]:
        """
        Export a complete dataset as JSON-LD dcat:Dataset.

        Args:
            username: Owner username
            project: Project name
            title: Dataset title (defaults to project name)
            description: Dataset description
            include_runs: Include Run provenance
            include_lineage: Include transformation lineage

        Returns:
            JSON-LD dataset document
        """
        # Get all DataPieces for this user/project
        datapieces_query = """
        MATCH (d:DataPiece {username: $username})
        OPTIONAL MATCH (d)-[:GENERATED_BY]->(r:Run)
        WHERE r.pipeline_name CONTAINS $project OR r IS NULL
        RETURN DISTINCT d, r
        ORDER BY d.created_at
        """

        # Get lineage if requested
        lineage_query = """
        MATCH (d1:DataPiece {username: $username})-[t:TRANSFORMED_BY]->(d2:DataPiece)
        RETURN d1.fingerprint as from_fp, d2.fingerprint as to_fp,
               t.component_name as component, t.config_hash as config_hash
        """

        # Get runs if requested
        runs_query = """
        MATCH (r:Run)
        WHERE r.pipeline_name CONTAINS $project
        RETURN r
        ORDER BY r.started_at
        """

        datapieces = []
        runs = []
        lineage = []

        with self.graph_store.driver.session(
            database=self.graph_store.database
        ) as session:
            # Fetch DataPieces
            for record in session.run(
                datapieces_query, username=username, project=project
            ):
                node = record["d"]
                run = record["r"]
                datapieces.append(self._datapiece_to_jsonld(node, run))

            # Fetch lineage
            if include_lineage:
                for record in session.run(lineage_query, username=username):
                    lineage.append(
                        {
                            "from": f"{ARKHAI_NAMESPACE}/data/{record['from_fp']}",
                            "to": f"{ARKHAI_NAMESPACE}/data/{record['to_fp']}",
                            "component": record["component"],
                            "configHash": record["config_hash"],
                        }
                    )

            # Fetch runs
            if include_runs:
                for record in session.run(runs_query, project=project):
                    run_node = record["r"]
                    runs.append(self._run_to_jsonld(run_node, []))

        # Build dataset document
        dataset_id = f"{ARKHAI_NAMESPACE}/dataset/{username}/{project}"

        dataset: Dict[str, Any] = {
            "@context": JSONLD_CONTEXT,
            "@type": "dcat:Dataset",
            "@id": dataset_id,
            "identifier": f"{username}/{project}",
            "title": title or project,
            "creator": username,
            "created": datetime.utcnow().isoformat() + "Z",
        }

        if description:
            dataset["description"] = description

        # Add DataPieces as distributions
        if datapieces:
            dataset["distribution"] = datapieces

        # Add provenance activities
        if runs:
            dataset["prov:wasGeneratedBy"] = runs

        # Add lineage graph
        if lineage:
            dataset["arkhai:lineage"] = lineage

        # Add statistics
        dataset["arkhai:statistics"] = {
            "totalDataPieces": len(datapieces),
            "totalRuns": len(runs),
            "totalTransformations": len(lineage),
        }

        return dataset

    def export_pipeline_definition(
        self, username: str, project: str, pipeline_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        Export a pipeline definition as JSON-LD.

        Args:
            username: Owner username
            project: Project name
            pipeline_name: Pipeline name

        Returns:
            JSON-LD document or None if not found
        """
        query = """
        MATCH (c:Component)
        WHERE c.author = $username
          AND c.project = $project
          AND c.pipeline_name = $pipeline_name
        RETURN c
        ORDER BY c.id
        """

        components = []
        with self.graph_store.driver.session(
            database=self.graph_store.database
        ) as session:
            for record in session.run(
                query, username=username, project=project, pipeline_name=pipeline_name
            ):
                comp = record["c"]
                components.append(self._component_to_jsonld(comp))

        if not components:
            return None

        pipeline_id = (
            f"{ARKHAI_NAMESPACE}/pipeline/{username}/{project}/{pipeline_name}"
        )

        return {
            "@context": JSONLD_CONTEXT,
            "@type": "schema:SoftwareApplication",
            "@id": pipeline_id,
            "name": pipeline_name,
            "creator": username,
            "schema:hasPart": components,
        }

    def _datapiece_to_jsonld(
        self,
        node: Any,
        run: Optional[Any] = None,
        parent: Optional[Any] = None,
        transform: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Convert a DataPiece node to JSON-LD."""
        fingerprint = node.get("fingerprint", "")
        uri = node.get("uri") or f"{ARKHAI_NAMESPACE}/data/{fingerprint}"

        doc: Dict[str, Any] = {
            "@type": "schema:DigitalDocument",
            "@id": uri,
            "identifier": fingerprint,
            "fingerprint": fingerprint,
        }

        # Add optional fields
        if node.get("ipfs_hash"):
            doc["storageKey"] = node["ipfs_hash"]

        if node.get("data_type"):
            doc["format"] = node["data_type"]

        if node.get("content_type"):
            doc["contentType"] = node["content_type"]

        if node.get("created_at"):
            created = node["created_at"]
            if hasattr(created, "isoformat"):
                doc["created"] = created.isoformat() + "Z"
            else:
                doc["created"] = str(created)

        if node.get("username"):
            doc["creator"] = node["username"]

        # Add provenance
        if run:
            run_id = run.get("id", "")
            doc["wasGeneratedBy"] = f"{ARKHAI_NAMESPACE}/run/{run_id}"
            if run.get("started_at"):
                started = run["started_at"]
                if hasattr(started, "isoformat"):
                    doc["generatedAtTime"] = started.isoformat() + "Z"

        # Add derivation
        if parent:
            parent_fp = parent.get("fingerprint", "")
            doc["wasDerivedFrom"] = f"{ARKHAI_NAMESPACE}/data/{parent_fp}"

        if transform:
            doc["arkhai:transformedBy"] = {
                "component": transform.get("component_name"),
                "configHash": transform.get("config_hash"),
            }

        return doc

    def _run_to_jsonld(
        self, run: Any, generated_fingerprints: List[str]
    ) -> Dict[str, Any]:
        """Convert a Run node to JSON-LD prov:Activity."""
        run_id = run.get("id", "")
        uri = f"{ARKHAI_NAMESPACE}/run/{run_id}"

        activity: Dict[str, Any] = {
            "@type": "prov:Activity",
            "@id": uri,
            "identifier": run_id,
        }

        if run.get("pipeline_name"):
            activity["prov:used"] = (
                f"{ARKHAI_NAMESPACE}/pipeline/{run.get('pipeline_name')}"
            )

        if run.get("started_at"):
            started = run["started_at"]
            if hasattr(started, "isoformat"):
                activity["startedAtTime"] = started.isoformat() + "Z"
            else:
                activity["startedAtTime"] = str(started)

        if run.get("ended_at"):
            ended = run["ended_at"]
            if hasattr(ended, "isoformat"):
                activity["endedAtTime"] = ended.isoformat() + "Z"
            else:
                activity["endedAtTime"] = str(ended)

        if run.get("success") is not None:
            activity["arkhai:success"] = run["success"]

        if run.get("input_count"):
            activity["arkhai:inputCount"] = run["input_count"]

        if run.get("output_count"):
            activity["arkhai:outputCount"] = run["output_count"]

        # Add generated entities
        if generated_fingerprints:
            activity["prov:generated"] = [
                f"{ARKHAI_NAMESPACE}/data/{fp}" for fp in generated_fingerprints
            ]

        return activity

    def _component_to_jsonld(self, comp: Any) -> Dict[str, Any]:
        """Convert a Component node to JSON-LD."""
        comp_id = comp.get("id", "")

        return {
            "@type": "schema:SoftwareSourceCode",
            "@id": f"{ARKHAI_NAMESPACE}/component/{comp_id}",
            "identifier": comp_id,
            "name": comp.get("component_name"),
            "schema:programmingLanguage": "Python",
            "arkhai:componentType": comp.get("component_type"),
            "arkhai:cacheKey": comp.get("cache_key"),
            "arkhai:version": comp.get("version"),
        }

    def to_json(self, data: Dict[str, Any], indent: int = 2) -> str:
        """
        Serialize JSON-LD to string.

        Args:
            data: JSON-LD document
            indent: Indentation level

        Returns:
            JSON string
        """
        return json.dumps(data, indent=indent, default=str)

    def save_to_file(
        self, data: Dict[str, Any], filepath: str, indent: int = 2
    ) -> None:
        """
        Save JSON-LD to file.

        Args:
            data: JSON-LD document
            filepath: Output file path
            indent: Indentation level
        """
        with open(filepath, "w") as f:
            json.dump(data, f, indent=indent, default=str)


# Convenience function for quick exports
def export_user_dataset(
    graph_store: GraphStore,
    username: str,
    project: str,
    output_path: Optional[str] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Quick export of a user's dataset to JSON-LD.

    Args:
        graph_store: Neo4j GraphStore instance
        username: Owner username
        project: Project name
        output_path: Optional file path to save
        **kwargs: Additional arguments for export_dataset

    Returns:
        JSON-LD dataset document
    """
    exporter = JSONLDExporter(graph_store)
    dataset = exporter.export_dataset(username, project, **kwargs)

    if output_path:
        exporter.save_to_file(dataset, output_path)

    return dataset
