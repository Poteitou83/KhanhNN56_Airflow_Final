** Cài đặt docker, astro cli

** Mở terminal trong folder, chạy: astro dev start

** Mở connections và config như sau:
    - conn_id: postgres
    - conn_type: postgres
    - conn_host: postgres
    - conn_schema: postgres
    - conn_login: postgres 
    - conn_password: postgres
    - conn_port: 5432

![Giao diện Airflow](./screenshots/all_dags.png)

** Chạy dag create_tables_dag_

![Giao diện Create Table thành công](./screenshots/create_table.png)

** Chạy final_etl_pipeline

![Chạy thành công](./screenshots/success.png)