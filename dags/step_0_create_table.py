import psycopg2
import pandas as pd
import numpy as np

conn = psycopg2.connect("host=postgres port=5432 dbname=airflow user=airflow password=airflow")
cur = conn.cursor()


product = """
    CREATE TABLE product(
		"Sku" VARCHAR PRIMARY KEY,
		"Style Id" VARCHAR,
		"Catalog" VARCHAR,
		"Category" VARCHAR,
		"MRP Old" INT,
		"Ajio MRP" INT,
		"Amazon MRP" INT,
		"Amazon FBA MRP" INT,
		"Flipkart MRP" INT,
		"Limeroad MRP" INT,
		"Myntra MRP" INT,
		"Paytm MRP" INT,
		"Snapdeal MRP" INT );
"""

compared_price = """
    CREATE TABLE compared_price(
            style_id VARCHAR PRIMARY KEY,
            most_expensive VARCHAR,
            least_expensive VARCHAR,
            ajio INT,
            amazon INT,
            flipkart INT,
            limeroad INT,
            myntra INT,
            paytm INT,
            snapdeal INT );
"""

print('Table(s) created')
cur.execute(product)
cur.execute(compared_price)
conn.commit()

conn.close()

