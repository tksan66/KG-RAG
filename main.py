from langchain_openai import ChatOpenAI
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import streamlit as st
import psycopg
import pandas as pd

load_dotenv()
NEO4J_URI=os.getenv("NEO4J_URI")
NEO4J_USER=os.getenv("NEO4J_USER")
NEO4J_PASSWORD=os.getenv("NEO4J_PASSWORD")
PORT=os.getenv("POSTGRES_PORT")
DBNAME=os.getenv("POSTGRES_DBNAME")
USER=os.getenv("POSTGRES_USER")
PASSWORD=os.getenv("POSTGRES_PASSWORD")


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
    
conninfo = f"host=127.0.0.1 port={PORT} dbname={DBNAME} user={USER} password={PASSWORD}"

def fetch_timeseries(sensor_key: str, start_ts: str, end_ts: str, conninfo: str) -> pd.DataFrame:
    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts, value
                FROM sensor_readings
                WHERE sensor_id = %s
                  AND ts >= %s::timestamptz
                  AND ts <= %s::timestamptz
                ORDER BY ts
                """,
                (sensor_key, start_ts, end_ts),
            )
            rows = cur.fetchall()

    return pd.DataFrame(rows, columns=["ts", "value"])

def detect_anomaly_zscore(df: pd.DataFrame, window: int = 6, z_th: float = 1.5) -> pd.DataFrame:
    df = df.copy()
    df["mean"] = df["value"].rolling(window, min_periods=window).mean()
    df["std"]  = df["value"].rolling(window, min_periods=window).std(ddof=0)
    df["z"] = (df["value"] - df["mean"]) / df["std"]
    df["is_anomaly"] = df["z"].abs() >= z_th
    return df[df["is_anomaly"] == True]



    
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)


st.title("Industrial AI Demo")

pump_id = st.selectbox(
    "Select Pump",
    ["Pump-23"]
)

question = st.text_input(
        "質問をしてください",
        "オペレーション上のリスクはなにかありますか"
    )

pump_info,sensors_type,sensors_id,sensors_key, main_date= get_pump_context(pump_id)
context = ""
for num in range(len(sensors_key)):
    df = fetch_timeseries(sensors_key[num], "2025-01-17 00:00:00+00","2025-01-17 23:00:00+00",conninfo)
    df_result = detect_anomaly_zscore(df)

    context += f"""
    pump_id:{pump_id}
    pump_name:{pump_info}
    pump_maintanance_day:{main_date[num]}
    sensor_id:{sensors_id[num]}
    sensor_type:{sensors_type[num]}
    anomaly_data:{df_result}
-----------------
    """

if st.button("Ask"):
    
    prompt = f"""
あなたは、工場のオペレーションデータをもとに各設備に異常がないかを判断する必要があります。
あたえられたcontextをもとに、異常値がある場合は、その設備に関する情報や判断根拠を提示しながら以下の質問に答える必要があります。
    
Context:
{context}

質問:
{question}
"""
    answer = llm.invoke(prompt)
    st.subheader("Answer")
    st.write(answer.content)

    st.subheader("Context Used")
    st.code(context)