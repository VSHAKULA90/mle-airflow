#Ссылка на урок: [Спринт 1/11: 1 спринт. Разработка пайплайнов подготовки данных и обучения модели → Тема 2/5: Подготовка данных с помощью Airflow → Урок 7/8: Продвинутый Airflow]( https://practicum.yandex.ru/learn/machine-learning/courses/41232eb3-8b9d-45e9-98e6-10447557b392/sprints/564458/topics/22c9e1c9-ccc2-455c-85ff-202367bf1339/lessons/98f3f2db-e10b-4c43-a748-cbc685c1515d/ )

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def create_table(**kwargs):
    
    from sqlalchemy import Table, MetaData, Column, Integer, String, Float, DateTime, UniqueConstraint, inspect
    
    hook = PostgresHook('destination_db')
    engine = hook.get_conn()
    metadata = MetaData()
    
    alt_users_churn = Table(
        "alt_users_churn",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True), 
        Column("customer_id", String, nullable=False),
        Column("begin_date", DateTime),
        Column("end_date", DateTime),
        Column("type", String),
        Column("paperless_billing", String),
        Column("payment_method", String),
        Column("monthly_charges", Float),
        Column("total_charges", Float),
        Column("internet_service", String),
        Column("online_security", String),
        Column("online_backup", String),
        Column("device_protection", String),
        Column("tech_support", String),
        Column("streaming_tv", String),
        Column("streaming_movies", String),
        Column("gender", String),
        Column("senior_citizen", Integer),
        Column("partner", String),
        Column("dependents", String),
        Column("multiple_lines", String),
        Column("target", Integer),
        UniqueConstraint("customer_id", name="unique_alt_users_churn_customer_id")
    )
    
    metadata.create_all(engine)
    
def extract(**kwargs):
    ti = kwargs["ti"]

    hook = PostgresHook('source_db')
    conn = hook.get_conn()
    sql = f"""
        select
            c.customer_id, c.begin_date, c.end_date, c.type, c.paperless_billing, c.payment_method, c.monthly_charges, c.total_charges,
            i.internet_service, i.online_security, i.online_backup, i.device_protection, i.tech_support, i.streaming_tv, i.streaming_movies,
            p.gender, p.senior_citizen, p.partner, p.dependents,
            ph.multiple_lines
        from contracts as c
        left join internet as i on i.customer_id = c.customer_id
        left join personal as p on p.customer_id = c.customer_id
        left join phone as ph on ph.customer_id = c.customer_id
    """
    data = pd.read_sql(sql, conn)
    conn.close()
    
    ti.xcom_push(key="extracted_data", value=data)


def transform(**kwargs):
    
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="extract", key="extracted_data")
    
    data['target'] = (data['end_date'] != 'No').astype(int)
    data['end_date'].replace({'No': None}, inplace=True)
    
    ti.xcom_push(key="transformed_data", value=data)

def load(**kwargs):
    ti = kwargs["ti"]
    df = ti.xcom_pull(task_ids="transform", key="transformed_data")
    
    hook = PostgresHook("destination_db")
    hook.insert_rows(
        table="alt_users_churn",
        rows=df.values.tolist(),
        target_fields=df.columns.tolist(),
        replace=True,
        replace_index=["customer_id"],
    )