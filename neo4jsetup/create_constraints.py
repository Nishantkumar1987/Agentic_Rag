from utils.neo4j_connector import Neo4jConnector

constraints = [
    """
    CREATE CONSTRAINT product_line_unique IF NOT EXISTS
    FOR (pl:ProductLine) REQUIRE pl.name IS UNIQUE;
    """,
    """
    CREATE CONSTRAINT product_unique IF NOT EXISTS
    FOR (p:Product) REQUIRE p.product_id IS UNIQUE;
    """,
    """
    CREATE CONSTRAINT document_unique IF NOT EXISTS
    FOR (d:Document) REQUIRE d.file_name IS UNIQUE;
    """,
    """
    CREATE CONSTRAINT section_unique IF NOT EXISTS
    FOR (s:Section) REQUIRE (s.product_id, s.title) IS UNIQUE;
    """,
    """
    CREATE CONSTRAINT subsection_unique IF NOT EXISTS
    FOR (ss:SubSection) REQUIRE (ss.section_id, ss.title) IS UNIQUE;
    """,
    """
    CREATE CONSTRAINT table_unique IF NOT EXISTS
    FOR (t:Table) REQUIRE t.table_id IS UNIQUE;
    """,
    """
    CREATE CONSTRAINT topic_unique IF NOT EXISTS
    FOR (t:Topic) REQUIRE t.name IS UNIQUE;
    """,
    """
    CREATE CONSTRAINT chunk_unique IF NOT EXISTS
    FOR (c:Chunk) REQUIRE c.chunk_id IS UNIQUE;
    """,
    """
    CREATE FULLTEXT INDEX sectionTextIndex IF NOT EXISTS
    FOR (s:Section) ON EACH [s.text];
    """
]

def run_constraints():
    db = Neo4jConnector()
    for cypher in constraints:
        db.write(cypher)
    db.close()
    print("All constraints created successfully.")

if __name__ == "__main__":
    run_constraints()
