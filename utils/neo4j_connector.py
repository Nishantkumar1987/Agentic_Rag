import os
from neo4j import GraphDatabase           # FIXED IMPORT
from dotenv import load_dotenv

class Neo4jConnector:
    def __init__(self, secrets_path=".secrets/neo4j.env"):
        load_dotenv(secrets_path)

        self.uri = os.getenv("NEO4J_URI")
        self.user = os.getenv("NEO4J_USERNAME")
        self.password = os.getenv("NEO4J_PASSWORD")
        self.database = os.getenv("NEO4J_DATABASE", "neo4j")   # FIXED NAME

        self.driver = GraphDatabase.driver(
            self.uri,
            auth=(self.user, self.password),
        )

    # ----------------------------------
    # Query (read)
    # ----------------------------------
    def query(self, cypher, params=None):
        params = params or {}
        with self.driver.session(database=self.database) as session:
            result = session.run(cypher, params)
            return [r.data() for r in result]

    # ----------------------------------
    # Write
    # ----------------------------------
    def write(self, cypher, params=None):
        params = params or {}
        with self.driver.session(database=self.database) as session:
            session.run(cypher, params)

    # ----------------------------------
    # Unified run() API USED BY INGESTION
    # ----------------------------------
    def run(self, query, params=None):
        params = params or {}
        with self.driver.session(database=self.database) as session:
            return session.run(query, params)

    # ----------------------------------
    # Close connection
    # ----------------------------------
    def close(self):
        if self.driver:
            self.driver.close()
