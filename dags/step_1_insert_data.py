import psycopg2
import pandas as pd
import numpy as np


def insert_data():
    conn = psycopg2.connect("host=postgres port=5432 dbname=airflow user=airflow password=airflow")
    cur = conn.cursor()

    f = pd.read_csv('dags/raw_data.csv')
    # print(f.shape)
    # print(f.head(5))
    # print(f.isnull().sum())

    # drop unused column
    f = f.drop(['Weight', 'TP', 'Final MRP Old'], axis=1)

    # drop record that have nill
    f = f.replace("Nill", np.nan)
    f = f.dropna()
    f = f.reset_index(drop=True)
    # print(f.isnull().sum())

    f = f.astype({"MRP Old":"int", "Ajio MRP":"int", "Amazon MRP":"int", "Amazon FBA MRP":"int", "Flipkart MRP":"int", "Limeroad MRP":"int", "Myntra MRP":"int", "Paytm MRP":"int", "Snapdeal MRP":"int"})
    # print(f.info())
    print(f.shape)

    sku = f['Sku']
    style = f['Style Id']
    catalog = f['Catalog']
    category = f['Category']
    mrp = f['MRP Old']
    aijo = f['Ajio MRP']
    amazon = f['Amazon MRP']
    fba = f['Amazon FBA MRP']
    flipkart = f['Flipkart MRP']
    limeroad = f['Limeroad MRP']
    myntra = f['Myntra MRP']
    paytm = f['Paytm MRP']
    snapdeal = f['Snapdeal MRP']

    # insert data to table
    num = 0
    for i in range(len(f)):
        insert_query = f"""
            INSERT INTO product
            VALUES ('{sku[i]}', '{style[i]}', '{catalog[i]}', '{category[i]}', '{mrp[i]}', '{aijo[i]}', '{amazon[i]}', '{fba[i]}', '{flipkart[i]}', '{limeroad[i]}', '{myntra[i]}', 
                    '{paytm[i]}', '{snapdeal[i]}')
        """
        # print(insert_query)
        cur.execute(insert_query)
        conn.commit()
        num += 1
    print('finished inserted ' + str(num) + ' records')


    # 1 row per 1 product (remove duplicate sizes)
    delete = """
                DELETE FROM product 
                WHERE sku IN (
                                SELECT sku FROM product 
                                EXCEPT SELECT MAX(sku) FROM product 
                                GROUP BY    style_id ,
                                            "catalog" ,
                                            category ,
                                            mrp ,
                                            ajio ,
                                            amazon ,
                                            amazon_fba ,
                                            flipkart ,
                                            limeroad ,
                                            myntra ,
                                            paytm ,
                                            snapdeal 
                                );
    """
    cur.execute(delete)
    conn.commit()
    print('finish remove duplicate')

    conn.close()

