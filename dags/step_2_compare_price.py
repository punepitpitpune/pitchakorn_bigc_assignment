import psycopg2
import pandas as pd
import numpy as np

def compare_price():
    conn = psycopg2.connect("host=postgres port=5432 dbname=airflow user=airflow password=airflow")
    cur = conn.cursor()

    sql = """
        SELECT 
            style_id,
            CASE    
                WHEN  ajio > amazon AND ajio > flipkart AND ajio > limeroad AND ajio > myntra AND ajio > paytm AND ajio > snapdeal then 'Ajio'
                WHEN  amazon > ajio AND amazon > flipkart AND amazon > limeroad AND amazon > myntra AND amazon > paytm AND amazon > snapdeal then 'Amazon'
                WHEN  flipkart > ajio AND flipkart > amazon AND flipkart > limeroad AND flipkart > myntra AND flipkart > paytm AND flipkart > snapdeal then 'Flipkart'
                WHEN  limeroad > ajio AND limeroad > amazon AND limeroad > flipkart AND limeroad > myntra AND limeroad > paytm AND limeroad > snapdeal then 'Limeroad'  
                WHEN  myntra > ajio AND myntra > amazon AND myntra > flipkart AND myntra > limeroad AND myntra > paytm AND myntra > snapdeal then 'Myntra'
                WHEN  paytm > ajio AND paytm > amazon AND paytm > flipkart AND paytm > limeroad AND paytm > myntra AND paytm > snapdeal then 'Paytm'
                WHEN  snapdeal > ajio AND snapdeal > amazon AND snapdeal > flipkart AND snapdeal > limeroad AND snapdeal > myntra AND snapdeal > paytm then 'Snapdeal'
                ELSE 'Equal' END AS most_expensive,
            CASE 
                WHEN  ajio < amazon AND ajio < flipkart AND ajio < limeroad AND ajio < myntra AND ajio < paytm AND ajio < snapdeal then 'Ajio'
                WHEN  amazon < ajio AND amazon < flipkart AND amazon < limeroad AND amazon < myntra AND amazon < paytm AND amazon < snapdeal then 'Amazon'
                WHEN  flipkart < ajio AND flipkart < amazon AND flipkart < limeroad AND flipkart < myntra AND flipkart < paytm AND flipkart < snapdeal then 'Flipkart'
                WHEN  limeroad < ajio AND limeroad < amazon AND limeroad < flipkart AND limeroad < myntra AND limeroad < paytm AND limeroad < snapdeal then 'Limeroad'  
                WHEN  myntra < ajio AND myntra < amazon AND myntra < flipkart AND myntra < limeroad AND myntra < paytm AND myntra < snapdeal then 'Myntra'
                WHEN  paytm < ajio AND paytm < amazon AND paytm < flipkart AND paytm < limeroad AND paytm < myntra AND paytm < snapdeal then 'Paytm'
                WHEN  snapdeal < ajio AND snapdeal < amazon AND snapdeal < flipkart AND snapdeal < limeroad AND snapdeal < myntra AND snapdeal < paytm then 'Snapdeal'
                ELSE 'Equal' END AS least_expensive,
            ajio ,
            amazon ,
            flipkart ,
            limeroad ,
            myntra ,
            paytm ,
            snapdeal 
        FROM product;
    """

    # execute query
    f = pd.read_sql(sql, conn)
    f = f.drop_duplicates(subset="style_id")
    f = f.reset_index(drop=True)
    # print(f.info())

    # insert queried data to table compared_price
    style = f['style_id']
    most = f['most_expensive']
    least =f['least_expensive']
    aijo = f['ajio']
    amazon = f['amazon']
    flipkart = f['flipkart']
    limeroad = f['limeroad']
    myntra = f['myntra']
    paytm = f['paytm']
    snapdeal = f['snapdeal']

    # insert
    num = 0
    for i in range(len(f)):
        insert_query = f"""
            INSERT INTO compared_price
            VALUES ('{style[i]}', '{most[i]}', '{least[i]}', '{aijo[i]}', '{amazon[i]}', '{flipkart[i]}', '{limeroad[i]}', '{myntra[i]}', '{paytm[i]}', '{snapdeal[i]}')
        """
        # print(insert_query)
        cur.execute(insert_query)
        conn.commit()
        num += 1
    print('finished inserted ' + str(num) + ' records')

    conn.close()