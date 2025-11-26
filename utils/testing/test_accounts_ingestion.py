from utils.neo4j_connector import Neo4jConnector
import pandas as pd


def run_account_ingestion_tests():
    db = Neo4jConnector()

    # Helper
    def run(q, params=None):
        try:
            return db.query(q, params)
        except Exception as e:
            print("Query failed:", e)
            return []

    print("\n🔍 Running Account Ingestion Test Suite...\n")

    # ---------------------------------------------------
    # TEST 1 — Count Account Products
    # ---------------------------------------------------
    print("TEST 1: Product Count (Accounts)")
    q1 = """
    MATCH (p:Product)
    WHERE p.product_line = "Account"
    RETURN p.product_id AS id, p.product_name AS name
    """
    products = run(q1)

    print("→ Found", len(products), "Account products")
    if len(products) == 7:
        print("✔ PASS — All 7 account products exist\n")
    else:
        print("❌ FAIL — Expected 7 products but found", len(products))
        print(pd.DataFrame(products))
        print("\n")

    # ---------------------------------------------------
    # TEST 2 — Product–Document Links
    # ---------------------------------------------------
    print("TEST 2: Product–Document Links")
    q2 = """
    MATCH (p:Product)-[:HAS_DOCUMENT]->(d:Document)
    WHERE p.product_line = "Account"
    RETURN p.product_id AS pid, d.file_name AS doc
    """
    prod_docs = run(q2)

    print("→ Found", len(prod_docs), "Product–Document links")
    if len(prod_docs) == 7:
        print("✔ PASS — All products have documents\n")
    else:
        print("❌ FAIL — Missing documents")
        print(pd.DataFrame(prod_docs))
        print("\n")

    # ---------------------------------------------------
    # TEST 3 — Document–Section Count
    # ---------------------------------------------------
    print("TEST 3: Document–Section Counts")
    q3 = """
    MATCH (p:Product)-[:HAS_DOCUMENT]->(d:Document)-[:HAS_SECTION]->(s:Section)
    WHERE p.product_line = "Account"
    RETURN d.file_name AS doc, COUNT(s) AS section_count
    ORDER BY section_count DESC
    """
    sections = run(q3)
    print(pd.DataFrame(sections), "\n")

    if all(row['section_count'] >= 10 for row in sections):
        print("✔ PASS — All documents have enough sections\n")
    else:
        print("❌ FAIL — Some documents have too few sections\n")

    # ---------------------------------------------------
    # TEST 4 — Required Section Names Check
    # ---------------------------------------------------
    print("TEST 4: Required Section Names")
    required_keywords = ["features", "eligibility", "kyc", "mitc", "fees"]

    q4 = "MATCH (s:Section) RETURN DISTINCT toLower(s.type) AS name"
    result = run(q4)

    section_names = [row["name"] for row in result if row["name"]]

    missing = [k for k in required_keywords if not any(k in name for name in section_names)]

    if not missing:
        print("✔ PASS — All required canonical sections exist\n")
    else:
        print("❌ FAIL — Missing:", missing, "\n")

    # ---------------------------------------------------
    # TEST 5 — Table Extraction
    # ---------------------------------------------------
    print("TEST 5: Table Extraction")
    q5 = """
    MATCH (t:Table)
    RETURN COUNT(t) AS tables
    """
    tables = run(q5)[0]["tables"] if run(q5) else 0

    print("→ Found", tables, "tables\n")

    # ---------------------------------------------------
    # TEST 6 — Orphan Node Checks
    # ---------------------------------------------------
    print("TEST 6: Orphan Node Checks")

    orph1 = run("""
    MATCH (p:Product)
    WHERE p.product_line = "Account" AND NOT (p)-[:HAS_DOCUMENT]->()
    RETURN p.product_id AS id
    """)

    if not orph1:
        print("✔ PASS — No orphan products")
    else:
        print("❌ FAIL — Orphan Products:", orph1)

    orph2 = run("""
    MATCH (d:Document)
    WHERE NOT (d)-[:HAS_SECTION]->()
    RETURN d.file_name AS doc
    """)

    if not orph2:
        print("✔ PASS — No orphan documents")
    else:
        print("❌ FAIL — Orphan Documents:", orph2)

    db.close()
    print("\n🎉 Test Suite Complete!\n")
