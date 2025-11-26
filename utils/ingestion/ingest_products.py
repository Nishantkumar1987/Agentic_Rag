# utils/ingestion/ingest_products.py

import json
from pathlib import Path
from utils.neo4j_connector import Neo4jConnector


class ProductIngestor:

    def __init__(self):
        self.db = Neo4jConnector()

    # --------------------------------------
    # Product Node
    # --------------------------------------
    def ingest_product(self, product):
        q = """
        MERGE (p:Product {product_id: $pid})
        SET p.product_name = $pname,
            p.product_line = $pline
        RETURN p
        """
        self.db.run(q, {
            "pid": product["product_id"],
            "pname": product["product_name"],
            "pline": product.get("product_line", "")
        })

    # --------------------------------------
    # Document Node
    # --------------------------------------
    def ingest_document(self, product_id, doc):
        document_id = f"{product_id}_{doc['file_name']}"

        q = """
        MERGE (d:Document {document_id: $did})
        SET d.file_name = $fn,
            d.source_type = $stype,
            d.parsed_at = $parsed
        WITH d
        MATCH (p:Product {product_id: $pid})
        MERGE (p)-[:HAS_DOCUMENT]->(d)
        RETURN d
        """

        self.db.run(q, {
            "did": document_id,
            "fn": doc["file_name"],
            "stype": doc["source_type"],
            "parsed": doc["parsed_at"],
            "pid": product_id
        })

        return document_id

    # --------------------------------------
    # Section Node
    # --------------------------------------
    def ingest_section(self, doc_id, section):
        q = """
        MERGE (s:Section {section_id: $sid})
        SET s.title = $title,
            s.type = $stype,
            s.text = $text,
            s.status = $status
        WITH s
        MATCH (d:Document {document_id: $did})
        MERGE (d)-[:HAS_SECTION]->(s)
        RETURN s
        """

        self.db.run(q, {
            "sid": section["section_id"],
            "title": section.get("title", ""),
            "stype": section.get("type", "Unknown"),
            "text": section.get("text", ""),
            "status": section.get("status", ""),
            "did": doc_id
        })

    # --------------------------------------
    # Table Node
    # --------------------------------------
    def ingest_table(self, section_id, table):
        q = """
        MERGE (t:Table {table_id: $tid})
        SET t.json = $json
        WITH t
        MATCH (s:Section {section_id: $sid})
        MERGE (s)-[:HAS_TABLE]->(t)
        RETURN t
        """

        self.db.run(q, {
            "tid": table["table_id"],
            "sid": section_id,
            "json": json.dumps(table["json"])
        })

    # --------------------------------------
    # Full ingestion of one JSON file
    # --------------------------------------
    def ingest_json(self, json_path):
    # FORCE UTF-8 — IGNORE invalid Windows cp1252 characters
        text = Path(json_path).read_text(encoding="utf-8", errors="ignore")
        data = json.loads(text)

        # Create Product
        self.ingest_product(data)

        for doc in data["documents"]:
            doc_id = self.ingest_document(data["product_id"], doc)

            for section in doc.get("sections", []):
                self.ingest_section(doc_id, section)

                for table in section.get("tables", []):
                    self.ingest_table(section["section_id"], table)

    # --------------------------------------
    # Ingest every JSON file in a folder
    # --------------------------------------
    def ingest_folder(self, folder_path):
        folder = Path(folder_path)

        for f in folder.iterdir():
            if f.suffix.lower() == ".json":
                print(f"→ Ingesting {f.name}")
                self.ingest_json(f)

        print("✓ Completed ingestion of folder:", folder_path)
