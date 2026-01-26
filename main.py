from langchain_openai import ChatOpenAI
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import streamlit as st
import psycopg
import pandas as pd
import connect_neo4j
import connect_postgredb


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
    ["Pump-23","Pump-25"]
)

question = st.text_input(
        "質問をしてください",
        "オペレーション上のリスクはなにかありますか"
    )

try:
    pump_info,sensors_type,sensors_id,sensors_key, main_date= connect_neo4j.get_pump_context(pump_id)
except:
    pump_info,sensors_type,sensors_id,sensors_key, main_date="該当のpumpは存在しません","","","",""

    
context = ""
for num in range(len(sensors_key)):
    df = connect_postgredb.fetch_timeseries(sensors_key[num], "2025-01-17 00:00:00+00","2025-01-17 23:00:00+00")
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