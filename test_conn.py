import psycopg2
import pandas as pd
import numpy as np

conn = psycopg2.connect("host=localhost port=5432 dbname=airflow user=airflow password=airflow")
cur = conn.cursor()

print("sucess")