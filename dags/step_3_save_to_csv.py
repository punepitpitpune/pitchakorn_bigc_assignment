import psycopg2
import pandas as pd
import numpy as np

conn = psycopg2.connect("host=localhost port=5432 dbname=airflow user=airflow password=airflow")
cur = conn.cursor()

sql = """
    SELECT *
    FROM compared_price
"""

f = pd.read_sql(sql, conn)
# print(f.head(10))

f.to_csv('result.csv')