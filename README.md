cài đặt docker, astro cli

mở terminal trong folder, chạy: astro dev start

Mở connections và config như sau:
    conn_id: postgres
    conn_type: postgres
    conn_host: postgres
    conn_schema: postgres
    conn_login: postgres 
    conn_password: postgres
    conn_port: 5432

![Giao diện Airflow](./screenshots/all_dags.png)

chạy dag create table

![Giao diện Create Table thành công](./screenshots/create_table.png)

chạy dag etl

![Chạy thành công](./screenshots/success.png)