import streamlit as st
import pandas as pd
from kafka import KafkaProducer
import json
import time
import os
import uuid
import psycopg2
import plotly.express as px

# Конфигурация Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions")
}

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "database": os.getenv("POSTGRES_DB", "fraud_detection"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "password")
}


def get_db_connection():
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        return conn
    except Exception as e:
        st.error(f"Ошибка подключения к базе данных: {e}")
        return None


def get_fraud_transactions(limit=10):
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()

    try:
        query = """
            SELECT transaction_id, score, fraud_flag, created_at 
            FROM transaction_scores 
            WHERE fraud_flag = 1 
            ORDER BY created_at DESC 
            LIMIT %s
        """
        df = pd.read_sql_query(query, conn, params=(limit,))
        return df
    except Exception as e:
        st.error(f"Ошибка выполнения запроса: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


def get_recent_scores(limit=100):
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()

    try:
        query = """
            SELECT score, fraud_flag, created_at 
            FROM transaction_scores 
            ORDER BY created_at DESC 
            LIMIT %s
        """
        df = pd.read_sql_query(query, conn, params=(limit,))
        return df
    except Exception as e:
        st.error(f"Ошибка выполнения запроса: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


def load_file(uploaded_file):
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {str(e)}")
        return None


def send_to_kafka(df, topic, bootstrap_servers):
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT"
        )

        progress_bar = st.progress(0)
        total_rows = len(df)

        for idx, row in df.iterrows():
            transaction_id = str(uuid.uuid4())

            producer.send(
                topic,
                value={
                    "transaction_id": transaction_id,
                    "data": row.to_dict()
                }
            )
            progress_bar.progress((idx + 1) / total_rows)
            time.sleep(0.01)

        producer.flush()
        return True
    except Exception as e:
        st.error(f"Ошибка отправки данных: {str(e)}")
        return False


if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}

st.set_page_config(page_title="Fraud Detection System", layout="wide")

st.sidebar.title("🎯 Fraud Detection System")
page = st.sidebar.radio("Навигация", ["📤 Отправка данных", "📊 Результаты"])

if page == "📤 Отправка данных":
    st.title("📤 Отправка транзакций в Kafka")

    uploaded_file = st.file_uploader(
        "Загрузите CSV файл с транзакциями",
        type=["csv"]
    )

    if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
        st.session_state.uploaded_files[uploaded_file.name] = {
            "status": "Загружен",
            "df": load_file(uploaded_file)
        }
        st.success(f"Файл {uploaded_file.name} успешно загружен!")

    if st.session_state.uploaded_files:
        st.subheader("🗂 Загруженные файлы")

        for file_name, file_data in st.session_state.uploaded_files.items():
            cols = st.columns([3, 2, 2, 1])

            with cols[0]:
                st.markdown(f"**Файл:** `{file_name}`")
                st.markdown(f"**Статус:** `{file_data['status']}`")
                if file_data["df"] is not None:
                    st.markdown(f"**Строк:** `{len(file_data['df'])}`")

            with cols[2]:
                if st.button(f"Отправить {file_name}", key=f"send_{file_name}"):
                    if file_data["df"] is not None:
                        with st.spinner("Отправка транзакций..."):
                            success = send_to_kafka(
                                file_data["df"],
                                KAFKA_CONFIG["topic"],
                                KAFKA_CONFIG["bootstrap_servers"]
                            )
                            if success:
                                st.session_state.uploaded_files[file_name]["status"] = "Отправлен"
                                st.success(f"Файл {file_name} успешно отправлен!")
                                st.rerun()
                    else:
                        st.error("Файл не содержит данных")

else:
    st.title("📊 Результаты обнаружения мошенничества")

    col1, col2 = st.columns([1, 1])

    with col1:
        st.subheader("🔍 Последние фродовые транзакции")
        if st.button("Обновить данные", key="refresh_fraud"):
            fraud_data = get_fraud_transactions(10)
            if not fraud_data.empty:
                fraud_data['created_at'] = pd.to_datetime(fraud_data['created_at'])
                fraud_data = fraud_data.round({'score': 4})
                st.dataframe(fraud_data, use_container_width=True)

                st.metric("Обнаружено фродовых транзакций", len(fraud_data))
            else:
                st.info("Фродовые транзакции не обнаружены")

    with col2:
        st.subheader("📈 Распределение скоров")
        if st.button("Обновить график", key="refresh_chart"):
            scores_data = get_recent_scores(100)
            if not scores_data.empty:
                fig = px.histogram(
                    scores_data,
                    x='score',
                    nbins=20,
                    title='Распределение скоров последних транзакций',
                    color='fraud_flag',
                    color_discrete_map={0: 'green', 1: 'red'}
                )
                fig.update_layout(
                    xaxis_title='Score',
                    yaxis_title='Количество транзакций',
                    showlegend=True
                )
                st.plotly_chart(fig, use_container_width=True)

                st.metric("Всего транзакций в выборке", len(scores_data))
                st.metric("Фродовых в выборке", len(scores_data[scores_data['fraud_flag'] == 1]))
            else:
                st.info("Нет данных для построения графика")

    st.subheader("📊 Статистика базы данных")
    conn = get_db_connection()
    if conn:
        try:
            stats_query = """
                SELECT 
                    COUNT(*) as total_transactions,
                    SUM(fraud_flag) as fraud_transactions,
                    AVG(score) as avg_score,
                    MAX(created_at) as last_update
                FROM transaction_scores
            """
            stats = pd.read_sql_query(stats_query, conn)

            if not stats.empty:
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Всего транзакций", int(stats.iloc[0]['total_transactions']))
                with col2:
                    st.metric("Фродовых транзакций", int(stats.iloc[0]['fraud_transactions']))
                with col3:
                    st.metric("Средний скор", f"{stats.iloc[0]['avg_score']:.4f}")
                with col4:
                    st.metric("Последнее обновление", stats.iloc[0]['last_update'].strftime('%H:%M:%S'))
        except Exception as e:
            st.error(f"Ошибка получения статистики: {e}")
        finally:
            conn.close()
