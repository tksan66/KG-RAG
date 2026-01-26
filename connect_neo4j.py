from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()
NEO4J_URI=os.getenv("NEO4J_URI")
NEO4J_USER=os.getenv("NEO4J_USER")
NEO4J_PASSWORD=os.getenv("NEO4J_PASSWORD")

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD)
)

def get_pump_context(pump_id):
    with driver.session() as session:
        result = session.run("""
            MATCH (p:Pump {id: $pid})
            OPTIONAL MATCH (p)-[:HAS_SENSOR]->(s)
            OPTIONAL MATCH (p)-[:HAS_MAINTENANCE]->(m)
            RETURN
              p.name AS pump,
              collect(s.type) AS sensors_type,
              collect(s.id) AS sensors_id,
              collect(s.external_timeseries_key) AS external_key,
              collect(m.date) AS maint_dates
        """, pid=pump_id)

        record = result.single()
        return record['pump'], record['sensors_type'],record['sensors_id'],record['external_key'],record['maint_dates']
    
    