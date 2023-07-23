All the codes code are in the folder "dags"
- `step_0_create_table.py`
- `step_1_insert_data.py`
- `step_2_compare_price.py`
-  pipeline code
    - ```bigc_assignment_pipeline.py```

1. RUN this cmd to compose docker <br />
    ``` docker-compose up -d ```

2. RUN ``` step_0_create_table.py ``` to create nescessary tables

3. Connect Airflow Web UI via browser<br />
    - ``` localhost:8080 ```
    - username : ```airflow```
    - password : ```airflow```

4. Trigger the pipeline

- Postgres connection detail<br />
    - hostname : ```localhost```
    - port : ```5432```
    - username : ```airflow```
    - password : ```airflow```
    - database : ```airflow```