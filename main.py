from langchain_openai import ChatOpenAI
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import streamlit as st

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
              collect(s.type) AS sensors,
              collect(m.date) AS maint_dates
        """, pid=pump_id)

        record = result.single()
        return f"""
Pump: {record['pump']}
Sensors: {', '.join(record['sensors'])}
Upcoming Maintenance Dates: {', '.join(record['maint_dates'])}
"""
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)


st.title("Industrial AI Demo")

pump_id = st.selectbox(
    "Select Pump",
    ["Pump-23"]
)

question = st.text_input(
    "Ask a question",
    "Any operational risk?"
)

if st.button("Ask"):
    context = get_pump_context(pump_id)
    prompt = f"""
Context:
{context}

Question:
{question}
"""
    answer = llm.invoke(prompt)
    st.subheader("Answer")
    st.write(answer.content)

    st.subheader("Context Used")
    st.code(context)